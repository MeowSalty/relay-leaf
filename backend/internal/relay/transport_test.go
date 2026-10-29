package relay

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestTransport(t *testing.T, base *http.Transport, max int) *Transport {
	t.Helper()
	tr, err := NewTransport(Options{MaxConnections: max, BaseTransport: base})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		tr.Close()
		if s := tr.Stats(); s != (Stats{}) {
			t.Errorf("尚未排空: %+v", s)
		}
	})
	return tr
}

func commitNoop() error { return nil }
func request(t *testing.T, method, url string, body io.Reader) *http.Request {
	t.Helper()
	r, e := http.NewRequest(method, url, body)
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func waitDone(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("I/O 未终止")
	}
}

func TestReusePreservesGzipAndRedirect(t *testing.T) {
	for _, h2 := range []bool{false, true} {
		t.Run(map[bool]string{false: "h1", true: "h2"}[h2], func(t *testing.T) {
			var compressed bytes.Buffer
			gz := gzip.NewWriter(&compressed)
			gz.Write([]byte("data: hello\n\n"))
			gz.Close()
			var accepts, requests atomic.Int64
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				if r.URL.Path == "/redirect" {
					w.Header().Set("Location", "/ok")
					w.WriteHeader(307)
					return
				}
				if r.Header.Get("Accept-Encoding") != "" {
					t.Error("出现自动压缩协商")
				}
				w.Header().Set("Content-Encoding", "gzip")
				w.Header().Set("Trailer", "X-Final")
				w.Write(compressed.Bytes())
				w.Header().Set("X-Final", "done")
			}))
			server.EnableHTTP2 = h2
			server.Config.ConnState = func(_ net.Conn, s http.ConnState) {
				if s == http.StateNew {
					accepts.Add(1)
				}
			}
			if h2 {
				server.StartTLS()
			} else {
				server.Start()
			}
			defer server.Close()
			base := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, ForceAttemptHTTP2: h2}
			tr := newTestTransport(t, base, 1)
			for i := 0; i < 3; i++ {
				a, err := tr.Start(context.Background(), request(t, "GET", server.URL+"/ok", nil), commitNoop)
				if err != nil {
					t.Fatal(err)
				}
				b, err := io.ReadAll(a.Response.Body)
				if err != nil {
					t.Fatal(err)
				}
				waitDone(t, a.Done())
				if !bytes.Equal(b, compressed.Bytes()) || a.Response.Uncompressed || a.Response.Header.Get("Content-Encoding") != "gzip" || a.Response.Trailer.Get("X-Final") != "done" {
					t.Fatal("响应发生变化")
				}
				if h2 && a.Response.ProtoMajor != 2 {
					t.Fatal("协议不是 HTTP/2")
				}
				a.Close()
			}
			if accepts.Load() != 1 {
				t.Fatalf("未复用连接: 接受了 %d 次连接", accepts.Load())
			}
			a, err := tr.Start(context.Background(), request(t, "GET", server.URL+"/redirect", nil), commitNoop)
			if err != nil {
				t.Fatal(err)
			}
			io.Copy(io.Discard, a.Response.Body)
			a.Close()
			if a.Response.StatusCode != 307 || requests.Load() != 4 {
				t.Fatal("发生了重定向")
			}
		})
	}
}

func TestCapacityCommitAndDialCancel(t *testing.T) {
	entered := make(chan struct{})
	exited := make(chan struct{})
	var dials, starts atomic.Int64
	tr := newTestTransport(t, &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		dials.Add(1)
		close(entered)
		<-ctx.Done()
		close(exited)
		return nil, ctx.Err()
	}}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, e := tr.Start(ctx, request(t, "GET", "http://example.invalid/", nil), func() error { starts.Add(1); return nil })
		errCh <- e
	}()
	waitDone(t, entered)
	_, err := tr.Start(context.Background(), request(t, "GET", "http://example.invalid/", nil), func() error { starts.Add(1); return nil })
	if !errors.Is(err, ErrCapacity) {
		t.Fatalf("容量检查失败: %v", err)
	}
	cancel()
	waitDone(t, exited)
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Start 被阻塞")
	}
	if dials.Load() != 1 || starts.Load() != 1 || tr.Stats().Connections != 0 {
		t.Fatal("请求尝试计数不正确")
	}
	cancelled, c := context.WithCancel(context.Background())
	c()
	_, err = tr.Start(cancelled, request(t, "GET", "http://example.invalid/", nil), func() error { starts.Add(1); return nil })
	if !errors.Is(err, context.Canceled) || starts.Load() != 1 {
		t.Fatal("提交前取消未生效")
	}
	_, err = tr.Start(context.Background(), request(t, "GET", "http://example.invalid/", nil), func() error { return errors.New("拒绝") })
	if err == nil || dials.Load() != 1 || tr.Stats().Connections != 0 {
		t.Fatal("提交回滚未生效")
	}
}

type observedConn struct {
	net.Conn
	active atomic.Int64
	calls  atomic.Int64
	closed chan struct{}
	once   sync.Once
}

func (c *observedConn) Read(p []byte) (int, error) {
	c.active.Add(1)
	c.calls.Add(1)
	defer c.active.Add(-1)
	return c.Conn.Read(p)
}
func (c *observedConn) Write(p []byte) (int, error) {
	c.active.Add(1)
	c.calls.Add(1)
	defer c.active.Add(-1)
	return c.Conn.Write(p)
}
func (c *observedConn) Close() error { c.once.Do(func() { close(c.closed) }); return c.Conn.Close() }

func TestCancellationEndsRawIO(t *testing.T) {
	for _, stage := range []string{"tls", "headers", "request-write", "response-body"} {
		t.Run(stage, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			ready := make(chan struct{})
			release := make(chan struct{})
			defer close(release)
			go func() {
				conn, e := listener.Accept()
				if e != nil {
					return
				}
				defer conn.Close()
				if stage == "tls" || stage == "request-write" {
					close(ready)
					<-release
					return
				}
				r, e := http.ReadRequest(bufioReader(conn))
				if e != nil {
					return
				}
				r.Body.Close()
				if stage == "response-body" {
					io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\nx")
				}
				close(ready)
				<-release
			}()
			var raw *observedConn
			base := &http.Transport{DialContext: func(ctx context.Context, n, a string) (net.Conn, error) {
				c, e := (&net.Dialer{}).DialContext(ctx, n, a)
				if e != nil {
					return nil, e
				}
				raw = &observedConn{Conn: c, closed: make(chan struct{})}
				return raw, nil
			}, TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
			tr := newTestTransport(t, base, 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			scheme := "http"
			if stage == "tls" {
				scheme = "https"
			}
			var body io.Reader
			if stage == "request-write" {
				body = io.LimitReader(zeroReader{}, 64<<20)
			}
			r := request(t, "POST", scheme+"://"+listener.Addr().String()+"/", body)
			done := make(chan struct{})
			go func() {
				defer close(done)
				a, e := tr.Start(ctx, r, commitNoop)
				if e == nil {
					io.Copy(io.Discard, a.Response.Body)
					a.Close()
				}
			}()
			waitDone(t, ready)
			cancel()
			waitDone(t, done)
			waitDone(t, raw.closed)
			if raw.active.Load() != 0 {
				t.Fatal("请求尝试完成后底层 I/O 仍在运行")
			}
			calls := raw.calls.Load()
			time.Sleep(20 * time.Millisecond)
			if raw.calls.Load() != calls {
				t.Fatal("清理后启动了底层 I/O")
			}
			if tr.Stats() != (Stats{}) {
				t.Fatalf("尚未排空: %+v", tr.Stats())
			}
		})
	}
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) { clear(p); return len(p), nil }

func TestCancelUnblocksRequestBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.Copy(io.Discard, r.Body) }))
	defer server.Close()
	rd, wr := io.Pipe()
	defer wr.Close()
	tr := newTestTransport(t, &http.Transport{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); tr.Start(ctx, request(t, "POST", server.URL, rd), commitNoop) }()
	// socket 读取操作证明拨号已成功，此时请求写入操作仍在等待管道。
	deadline := time.Now().Add(time.Second)
	for tr.Stats().ActiveIO == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	waitDone(t, done)
	if _, err := wr.Write([]byte("x")); err == nil {
		t.Fatal("请求体未关闭")
	}
}

func TestCloseInterruptsAttempt(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "100")
		w.Write([]byte("x"))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()
	tr := newTestTransport(t, &http.Transport{}, 1)
	a, e := tr.Start(context.Background(), request(t, "GET", server.URL, nil), commitNoop)
	if e != nil {
		t.Fatal(e)
	}
	tr.Close()
	waitDone(t, a.Done())
	if tr.Stats() != (Stats{}) {
		t.Fatal("Close 未排空操作")
	}
	_, e = tr.Start(context.Background(), request(t, "GET", server.URL, nil), commitNoop)
	if !errors.Is(e, ErrClosed) {
		t.Fatal(e)
	}
}

func TestCompletedResponseStopsIncompleteUpload(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	peerDone := make(chan struct{})
	go func() {
		defer close(peerDone)
		c, e := ln.Accept()
		if e != nil {
			return
		}
		defer c.Close()
		_, e = http.ReadRequest(bufio.NewReader(c))
		if e != nil {
			return
		}
		io.WriteString(c, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
		io.Copy(io.Discard, c)
	}()
	rd, wr := io.Pipe()
	defer wr.Close()
	tr := newTestTransport(t, &http.Transport{}, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		a, e := tr.Start(context.Background(), request(t, "POST", "http://"+ln.Addr().String(), rd), commitNoop)
		if e != nil {
			t.Error(e)
			return
		}
		b, e := io.ReadAll(a.Response.Body)
		if e != nil || string(b) != "ok" {
			t.Errorf("响应: %q %v", b, e)
		}
		a.Close()
	}()
	waitDone(t, done)
	waitDone(t, peerDone)
	if tr.Stats() != (Stats{}) {
		t.Fatal("未完成的上传仍在运行")
	}
}

func TestCancellationIsolatesConcurrentH2Attempts(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "2")
		w.Write([]byte("a"))
		w.(http.Flusher).Flush()
		select {
		case <-release:
			w.Write([]byte("b"))
		case <-r.Context().Done():
		}
	}))
	s.EnableHTTP2 = true
	s.StartTLS()
	defer s.Close()
	tr := newTestTransport(t, &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, ForceAttemptHTTP2: true}, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a, err := tr.Start(ctx, request(t, "GET", s.URL, nil), commitNoop)
	if err != nil {
		t.Fatal(err)
	}
	b, err := tr.Start(context.Background(), request(t, "GET", s.URL, nil), commitNoop)
	if err != nil {
		t.Fatal(err)
	}
	if a.Response.ProtoMajor != 2 || b.Response.ProtoMajor != 2 {
		t.Fatal("协议不是 HTTP/2")
	}
	cancel()
	waitDone(t, a.Done())
	if tr.Stats().Busy != 1 || tr.Stats().Connections != 1 {
		t.Fatal("取消了另一请求尝试")
	}
	// 完成第二个响应，无需等待整个测试夹具释放。
	buf := make([]byte, 1)
	if _, err := io.ReadFull(b.Response.Body, buf); err != nil || string(buf) != "a" {
		t.Fatalf("另一响应被中断: %v", err)
	}
	b.Close()
	waitDone(t, b.Done())
}

func TestTransportCloseWaitsForPrecommitReservation(t *testing.T) {
	tr := newTestTransport(t, &http.Transport{DialContext: func(context.Context, string, string) (net.Conn, error) {
		t.Error("关闭后仍发生拨号")
		return nil, injectedIO
	}}, 1)
	entered := make(chan struct{})
	release := make(chan struct{})
	startDone := make(chan struct{})
	closeDone := make(chan struct{})
	go func() {
		defer close(startDone)
		tr.Start(context.Background(), request(t, "GET", "http://example.invalid", nil), func() error { close(entered); <-release; return injectedIO })
	}()
	waitDone(t, entered)
	go func() { tr.Close(); close(closeDone) }()
	// 关闭状态与预留名额的发布由同一把锁保护。
	for {
		tr.mu.Lock()
		closed := tr.closed
		tr.mu.Unlock()
		if closed {
			break
		}
		time.Sleep(time.Millisecond)
	}
	close(release)
	waitDone(t, startDone)
	waitDone(t, closeDone)
	if tr.Stats() != (Stats{}) {
		t.Fatal("关闭后仍有预留容量")
	}
}

func TestH2HeaderAndBodyCancellationEndsRawIO(t *testing.T) {
	for _, body := range []bool{false, true} {
		t.Run(map[bool]string{true: "body", false: "headers"}[body], func(t *testing.T) {
			reached := make(chan struct{})
			s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if body {
					w.Header().Set("Content-Length", "100")
					w.Write([]byte("x"))
					w.(http.Flusher).Flush()
				}
				close(reached)
				<-r.Context().Done()
			}))
			s.EnableHTTP2 = true
			s.StartTLS()
			defer s.Close()
			var raw *observedConn
			base := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, ForceAttemptHTTP2: true, DialContext: func(ctx context.Context, n, a string) (net.Conn, error) {
				c, e := (&net.Dialer{}).DialContext(ctx, n, a)
				if e != nil {
					return nil, e
				}
				raw = &observedConn{Conn: c, closed: make(chan struct{})}
				return raw, nil
			}}
			tr := newTestTransport(t, base, 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan struct{})
			go func() {
				defer close(done)
				a, e := tr.Start(ctx, request(t, "GET", s.URL, nil), commitNoop)
				if e == nil {
					if a.Response.ProtoMajor != 2 {
						t.Error("协议不是 HTTP/2")
					}
					io.Copy(io.Discard, a.Response.Body)
					a.Close()
				}
			}()
			waitDone(t, reached)
			cancel()
			waitDone(t, done)
			waitDone(t, raw.closed)
			if raw.active.Load() != 0 || tr.Stats() != (Stats{}) {
				t.Fatal("HTTP/2 底层 I/O 尚未排空")
			}
		})
	}
}

func TestMissingCommitAndLegacyDialFailBeforeOutbound(t *testing.T) {
	var dials atomic.Int64
	tr := newTestTransport(t, &http.Transport{DialContext: func(context.Context, string, string) (net.Conn, error) { dials.Add(1); return nil, injectedIO }}, 1)
	_, err := tr.Start(context.Background(), request(t, "GET", "http://example.invalid", nil), nil)
	if err == nil || dials.Load() != 0 || tr.Stats() != (Stats{}) {
		t.Fatal("缺少提交回调时仍占用了容量或发起了拨号")
	}
	_, err = NewTransport(Options{MaxConnections: 1, BaseTransport: &http.Transport{Dial: func(string, string) (net.Conn, error) { dials.Add(1); return nil, injectedIO }}})
	if err == nil || dials.Load() != 0 {
		t.Fatal("接受了不支持 context 的旧式拨号器")
	}
}

func TestCloseBetweenPrepareAndCommitPreventsCounting(t *testing.T) {
	var counts, dials atomic.Int64
	tr := newTestTransport(t, &http.Transport{DialContext: func(context.Context, string, string) (net.Conn, error) { dials.Add(1); return nil, injectedIO }}, 1)
	// 停在 Start 使用的同一预留边界，随后让 Close 在
	// 对外可见的提交回调获准执行之前先完成。
	e, err := tr.acquire("http://example.invalid:80")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { tr.Close(); close(done) }()
	for {
		tr.mu.Lock()
		closed := tr.closed
		tr.mu.Unlock()
		if closed {
			break
		}
		time.Sleep(time.Millisecond)
	}
	err = tr.commitStart(context.Background(), func() error { counts.Add(1); return nil })
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("Close 后发生提交: %v", err)
	}
	tr.rollbackStart(e)
	waitDone(t, done)
	if counts.Load() != 0 || dials.Load() != 0 || tr.Stats() != (Stats{}) {
		t.Fatal("关闭后的准备操作占用了请求尝试名额")
	}
}

// 在实际对端解析请求，不使用 RoundTrip 次数代替。
func bufioReader(r io.Reader) *bufio.Reader { return bufio.NewReader(r) }
