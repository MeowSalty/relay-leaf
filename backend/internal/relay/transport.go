// Package relay 管理单次上游请求尝试及其 I/O 生命周期。
package relay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrCapacity = errors.New("relay: 连接容量已耗尽")
	ErrClosed   = errors.New("relay: Transport 已关闭")
)

// Options 配置容量受限的上游直连连接池。BaseTransport 会被克隆，
// 并保留协议、TLS、拨号和各阶段超时设置。
// 不支持旧式 Dial、自定义 TLS 拨号器、协议处理器和代理：
// 所有底层连接都必须经过适配器的取消控制和 I/O 统计。
type Options struct {
	MaxConnections int
	BaseTransport  *http.Transport
}

type Transport struct {
	base     *http.Transport
	max      int
	mu       sync.Mutex
	closed   bool
	entries  map[*poolEntry]struct{}
	activeIO atomic.Int64
}

type poolEntry struct {
	key        string
	cc         *http.ClientConn
	raw        *trackedConn
	busy       bool
	lastUsed   time.Time
	changed    chan struct{}
	attempt    *Attempt
	startReady chan struct{}
}

// Stats 的 Connections 包含拨号预留名额。ActiveIO 统计底层
// socket 操作，包括空闲连接的常规读取操作。
type Stats struct{ Connections, Busy, Dialing, ActiveIO int }

func NewTransport(opts Options) (*Transport, error) {
	if opts.MaxConnections <= 0 {
		return nil, errors.New("relay: MaxConnections 必须为正数")
	}
	base := opts.BaseTransport
	if base == nil {
		base = http.DefaultTransport.(*http.Transport)
	}
	base = base.Clone()
	// 环境代理是 DefaultTransport 提供的便利功能，不作为 relay 的隐式路由。
	if opts.BaseTransport == nil {
		base.Proxy = nil
		// DefaultTransport 已被使用后，Clone 可能复制标准 HTTP/2 处理器。
		// 让这个独立的 Transport 安装自己的处理器。
		base.TLSNextProto = nil
	}
	if base.Proxy != nil || base.Dial != nil || base.DialTLS != nil || base.DialTLSContext != nil || len(base.TLSNextProto) != 0 {
		return nil, errors.New("relay: 不支持自定义代理、旧式拨号器、TLS 拨号器或协议处理器")
	}
	base.DisableCompression = true
	t := &Transport{base: base, max: opts.MaxConnections, entries: make(map[*poolEntry]struct{})}
	dial := base.DialContext
	if dial == nil {
		dial = (&net.Dialer{}).DialContext
	}
	base.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		a, ok := ctx.Value(attemptContextKey{}).(*Attempt)
		if !ok {
			return nil, errors.New("relay: 拨号不属于任何请求尝试")
		}
		if timing, ok := ctx.Value(attemptTimersKey{}).(*attemptTimers); ok {
			timing.arm("connect_timeout", timing.exec.ConnectTimeout)
			defer timing.stop("connect_timeout")
		}
		raw, err := dial(ctx, network, address)
		if err != nil {
			return nil, err
		}
		conn := newTrackedConn(raw, &t.activeIO)
		a.mu.Lock()
		a.raw = conn
		cancelled := a.ctx.Err() != nil
		a.mu.Unlock()
		if cancelled {
			_ = conn.Close()
			conn.wait()
			return nil, a.ctx.Err()
		}
		return conn, nil
	}
	return t, nil
}

type attemptContextKey struct{}

// Attempt 管理一次网络请求尝试。将 Response.Body 读取到 EOF 会自动完成该尝试。
// Close 中止未完成的工作；只有所属 I/O 结束且连接池名额释放后，Done 才会关闭。
// 按照 net/http 的出站请求体契约，Request.Body.Close 必须解除并发 Read 的阻塞。
type Attempt struct {
	Response     *http.Response
	t            *Transport
	e            *poolEntry
	ctx          context.Context
	cancel       context.CancelFunc
	mu           sync.Mutex
	raw          *trackedConn
	cc           *http.ClientConn
	body         *trackedBody
	responseBody io.ReadCloser
	roundDone    chan struct{}
	requestDone  chan struct{}
	done         chan struct{}
	once         sync.Once
	stopCancel   func() bool
	terminal     bool
}

// Start 要求提供 onStarted，不排队地预留容量，并在任何拨号或请求写入之前
// 恰好提交一次。错误不会触发内部重发。
// 提交之前，Request.Body 的所有权仍由调用方持有。
func (t *Transport) Start(ctx context.Context, req *http.Request, onStarted func() error) (*Attempt, error) {
	if onStarted == nil {
		return nil, errors.New("relay: 必须提供 onStarted 提交回调")
	}
	if ctx == nil || req == nil || req.URL == nil {
		return nil, errors.New("relay: 必须提供 context 和请求 URL")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if req.URL.Scheme != "http" && req.URL.Scheme != "https" {
		return nil, errors.New("relay: 不支持此 URL scheme")
	}
	host := req.URL.Hostname()
	if host == "" {
		return nil, errors.New("relay: 必须提供 URL 主机")
	}
	port := req.URL.Port()
	if port == "" {
		if req.URL.Scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	address := net.JoinHostPort(host, port)
	key := req.URL.Scheme + "://" + address
	e, err := t.acquire(key)
	if err != nil {
		return nil, err
	}
	if err := t.commitStart(ctx, onStarted); err != nil {
		t.rollbackStart(e)
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	a := &Attempt{t: t, e: e, ctx: ctx, cancel: cancel, raw: e.raw, cc: e.cc, roundDone: make(chan struct{}), requestDone: make(chan struct{}), done: make(chan struct{})}
	if req.Body != nil && req.Body != http.NoBody {
		a.body = newTrackedBody(req.Body)
	}
	t.mu.Lock()
	e.attempt = a
	closed := t.closed
	close(e.startReady)
	t.mu.Unlock()
	if closed {
		cancel()
	}
	a.stopCancel = context.AfterFunc(ctx, func() { a.finish(true) })
	var wroteOnce sync.Once
	outCtx := httptrace.WithClientTrace(context.WithValue(ctx, attemptContextKey{}, a), &httptrace.ClientTrace{WroteRequest: func(httptrace.WroteRequestInfo) { wroteOnce.Do(func() { close(a.requestDone) }) }})
	out := req.Clone(outCtx)
	if a.body != nil {
		out.Body = a.body
	}
	// ClientConn 本身没有重定向循环；GetBody 不用于重发。
	var cc *http.ClientConn
	if a.cc == nil {
		cc, err = t.base.NewClientConn(out.Context(), req.URL.Scheme, address)
		if err == nil {
			a.mu.Lock()
			a.cc = cc
			a.mu.Unlock()
			t.mu.Lock()
			e.cc = cc
			e.raw = a.raw
			t.mu.Unlock()
			cc.SetStateHook(func(*http.ClientConn) {
				select {
				case e.changed <- struct{}{}:
				default:
				}
			})
		}
	} else {
		cc = a.cc
	}
	var resp *http.Response
	if err == nil {
		err = ctx.Err()
	}
	if err == nil {
		resp, err = cc.RoundTrip(out)
	}
	a.mu.Lock()
	if resp != nil {
		a.responseBody = resp.Body
	}
	a.mu.Unlock()
	close(a.roundDone)
	if err != nil {
		a.finish(true)
		return nil, fmt.Errorf("relay: 上游请求尝试失败: %w", err)
	}
	if ctx.Err() != nil {
		a.finish(true)
		return nil, ctx.Err()
	}
	a.Response = resp
	resp.Body = &attemptBody{ReadCloser: resp.Body, a: a}
	return a, nil
}

func (t *Transport) rollbackStart(e *poolEntry) {
	t.mu.Lock()
	defer t.mu.Unlock()
	e.busy = false
	if e.cc == nil || t.closed {
		t.dropLocked(e)
	}
	close(e.startReady)
}

func (t *Transport) commitStart(ctx context.Context, onStarted func() error) error {
	t.mu.Lock()
	// 调用 onStarted 的授权在此处与 Close 按线性顺序确定。此前的 Close
	// 会阻止计数；此后的 Close 会等待本次 Start 完成提交回调，
	// 并在任何后续 I/O 开始前中止它。
	err := ctx.Err()
	if err == nil && t.closed {
		err = ErrClosed
	}
	t.mu.Unlock()
	if err != nil {
		return err
	}
	return onStarted()
}

func (t *Transport) acquire(key string) (*poolEntry, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, ErrClosed
	}
	var evict *poolEntry
	for e := range t.entries {
		if e.busy {
			continue
		}
		expired := t.base.IdleConnTimeout > 0 && time.Since(e.lastUsed) >= t.base.IdleConnTimeout
		if e.cc.Err() != nil || expired {
			t.dropLocked(e)
			continue
		}
		if e.key == key && e.cc.Available() > 0 {
			e.busy = true
			e.startReady = make(chan struct{})
			return e, nil
		}
		if evict == nil || e.lastUsed.Before(evict.lastUsed) {
			evict = e
		}
	}
	if len(t.entries) >= t.max {
		if evict == nil {
			return nil, ErrCapacity
		}
		t.dropLocked(evict)
	}
	e := &poolEntry{key: key, busy: true, changed: make(chan struct{}, 1), startReady: make(chan struct{})}
	t.entries[e] = struct{}{}
	return e, nil
}

func (t *Transport) dropLocked(e *poolEntry) {
	if e.raw != nil {
		_ = e.raw.Close()
		e.raw.wait()
	}
	if e.cc != nil {
		_ = e.cc.Close()
	}
	delete(t.entries, e)
}

func (a *Attempt) Done() <-chan struct{} { return a.done }
func (a *Attempt) Close() error          { a.finish(true); return nil }

func (a *Attempt) abortIO() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.terminal {
		return
	}
	a.cancel()
	raw, cc := a.raw, a.cc
	if raw != nil {
		_ = raw.Close()
	}
	if cc != nil {
		_ = cc.Close()
	}
	if a.body != nil {
		_ = a.body.Close()
	}
}

func (a *Attempt) finish(abort bool) {
	// 即使正常 EOF 清理已占用 once，取消操作也必须中断 I/O。
	if abort {
		a.abortIO()
	}
	a.once.Do(func() {
		<-a.roundDone
		if a.stopCancel != nil {
			a.stopCancel()
		}
		a.mu.Lock()
		raw, cc, body := a.raw, a.cc, a.responseBody
		a.mu.Unlock()
		// 上游可能在读取完上传内容之前就完成响应。
		// 即使响应已到 EOF，也必须停止仍被阻塞的写入操作。
		if body != nil {
			select {
			case <-a.requestDone:
			default:
				a.abortIO()
			}
		}
		if a.body != nil {
			_ = a.body.Close()
			a.body.wait()
		}
		if body != nil {
			_ = body.Close()
		}
		if cc != nil {
			for cc.Err() == nil && cc.InFlight() != 0 {
				select {
				case <-a.e.changed:
				case <-a.ctx.Done():
					a.abortIO()
				}
			}
		}
		if a.ctx.Err() != nil || (cc != nil && cc.Err() != nil) {
			if raw != nil {
				_ = raw.Close()
				raw.wait()
			}
		}
		a.mu.Lock()
		a.terminal = true
		a.mu.Unlock()
		a.t.mu.Lock()
		a.e.attempt = nil
		if a.t.closed || cc == nil || cc.Err() != nil || a.ctx.Err() != nil {
			a.t.dropLocked(a.e)
		} else {
			a.e.busy = false
			a.e.lastUsed = time.Now()
		}
		a.t.mu.Unlock()
		a.cancel()
		close(a.done)
	})
	<-a.done
}

type attemptBody struct {
	io.ReadCloser
	a *Attempt
}

func (b *attemptBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil {
		b.a.finish(err != io.EOF)
	}
	return n, err
}
func (b *attemptBody) Close() error { return b.a.Close() }

func (t *Transport) Stats() Stats {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := Stats{Connections: len(t.entries), ActiveIO: int(t.activeIO.Load())}
	for e := range t.entries {
		if e.busy {
			s.Busy++
		}
		if e.cc == nil {
			s.Dialing++
		}
	}
	return s
}

func (t *Transport) Close() error {
	for {
		t.mu.Lock()
		t.closed = true
		var attempts []*Attempt
		var starting []<-chan struct{}
		for e := range t.entries {
			if e.attempt != nil {
				attempts = append(attempts, e.attempt)
			} else if e.busy {
				starting = append(starting, e.startReady)
			} else {
				t.dropLocked(e)
			}
		}
		finished := len(t.entries) == 0
		t.mu.Unlock()
		if finished {
			return nil
		}
		for _, a := range attempts {
			_ = a.Close()
		}
		for _, ready := range starting {
			<-ready
		}
	}
}

// 该门控在 Close 后拒绝新操作，并等待已进入底层 socket 的操作结束。
// 仅凭已关闭 Transport 的计数器无法保证这一点。
type trackedConn struct {
	net.Conn
	mu     sync.Mutex
	cond   *sync.Cond
	closed bool
	active int
	total  *atomic.Int64
}

func newTrackedConn(c net.Conn, total *atomic.Int64) *trackedConn {
	x := &trackedConn{Conn: c, total: total}
	x.cond = sync.NewCond(&x.mu)
	return x
}
func (c *trackedConn) begin() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false
	}
	c.active++
	c.total.Add(1)
	return true
}
func (c *trackedConn) end() {
	c.mu.Lock()
	c.active--
	c.total.Add(-1)
	c.cond.Broadcast()
	c.mu.Unlock()
}
func (c *trackedConn) Read(p []byte) (int, error) {
	if !c.begin() {
		return 0, net.ErrClosed
	}
	defer c.end()
	return c.Conn.Read(p)
}
func (c *trackedConn) Write(p []byte) (int, error) {
	if !c.begin() {
		return 0, net.ErrClosed
	}
	defer c.end()
	return c.Conn.Write(p)
}
func (c *trackedConn) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	return c.Conn.Close()
}
func (c *trackedConn) wait() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for c.active != 0 {
		c.cond.Wait()
	}
}

type trackedBody struct {
	io.ReadCloser
	mu     sync.Mutex
	cond   *sync.Cond
	active int
	closed bool
	once   sync.Once
	err    error
}

func newTrackedBody(b io.ReadCloser) *trackedBody {
	x := &trackedBody{ReadCloser: b}
	x.cond = sync.NewCond(&x.mu)
	return x
}
func (b *trackedBody) Read(p []byte) (int, error) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	b.active++
	b.mu.Unlock()
	defer func() { b.mu.Lock(); b.active--; b.cond.Broadcast(); b.mu.Unlock() }()
	return b.ReadCloser.Read(p)
}
func (b *trackedBody) Close() error {
	b.once.Do(func() { b.mu.Lock(); b.closed = true; b.mu.Unlock(); b.err = b.ReadCloser.Close() })
	return b.err
}
func (b *trackedBody) wait() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for b.active != 0 {
		b.cond.Wait()
	}
}
