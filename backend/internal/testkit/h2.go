package testkit

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"time"
)

const (
	ErrorCodeNo            uint32 = 0
	ErrorCodeProtocol      uint32 = 1
	ErrorCodeRefusedStream uint32 = 7
	FrameData              byte   = 0
	FrameHeaders           byte   = 1
	FrameRSTStream         byte   = 3
	FrameSettings          byte   = 4
	FramePing              byte   = 6
	FrameGoAway            byte   = 7
)

// H2Frame 表示协议帧。Payload 是独立副本。HEADERS 不进行 HPACK 解码。
type H2Frame struct {
	Type, Flags byte
	StreamID    uint32
	Payload     []byte
}

// H2Options 仅控制协议故障注入；正常响应使用 httptest。
type H2Options struct {
	// nil 保留协议默认值。指向零值的指针可在对端处理 SETTINGS 后
	// 阻止请求 DATA。此端点始终不发送 WINDOW_UPDATE。
	InitialWindowSize *uint32
	// OnHeaders 针对每个 HEADERS 帧在独立的 goroutine 中调用。它可以
	// 等待 Gate，同时连接继续读取帧并观察取消信号。
	OnHeaders func(context.Context, *H2Conn, H2Frame) error
}

// H2Server 接受使用 ALPN h2 的真实回环 TCP/TLS 连接。
// 计数包含实际接受的 TCP 连接和协议 HEADERS，不是 RoundTrip 调用次数。
type H2Server struct {
	URL            string
	listener       net.Listener
	tlsConfig      *tls.Config
	clientTLS      *tls.Config
	opts           H2Options
	ctx            context.Context
	cancel         context.CancelFunc
	mu             sync.Mutex
	conns          map[*H2Conn]struct{}
	errs           []error
	wg             sync.WaitGroup
	closeOnce      sync.Once
	connections    atomic.Int64
	headers        atomic.Int64
	dataBytes      atomic.Int64
	headersReached chan struct{}
	headersOnce    sync.Once
}

func NewH2Server(opts H2Options) (*H2Server, error) {
	if opts.InitialWindowSize != nil && *opts.InitialWindowSize > 0x7fffffff {
		return nil, fmt.Errorf("无效的 HTTP/2 初始窗口")
	}
	// 获取 httptest 的本地证书，并在原始 TLS 端点上复用。
	certServer := httptest.NewTLSServer(nil)
	cert := certServer.TLS.Certificates[0]
	pool := x509.NewCertPool()
	pool.AddCert(certServer.Certificate())
	certServer.Close()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &H2Server{URL: "https://" + ln.Addr().String(), listener: ln,
		tlsConfig: &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{"h2"}, MinVersion: tls.VersionTLS12},
		clientTLS: &tls.Config{RootCAs: pool, NextProtos: []string{"h2"}, MinVersion: tls.VersionTLS12},
		opts:      opts, ctx: ctx, cancel: cancel, conns: make(map[*H2Conn]struct{}), headersReached: make(chan struct{})}
	if opts.InitialWindowSize != nil {
		value := *opts.InitialWindowSize
		s.opts.InitialWindowSize = &value
	}
	s.wg.Add(1)
	go s.accept()
	return s, nil
}

func (s *H2Server) TLSConfig() *tls.Config          { return s.clientTLS.Clone() }
func (s *H2Server) HeadersReached() <-chan struct{} { return s.headersReached }
func (s *H2Server) Counts() (connections, headers int64) {
	return s.connections.Load(), s.headers.Load()
}
func (s *H2Server) DataBytes() int64 { return s.dataBytes.Load() }
func (s *H2Server) Errors() []error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]error(nil), s.errs...)
}
func (s *H2Server) record(err error) {
	if err != nil {
		s.mu.Lock()
		s.errs = append(s.errs, err)
		s.mu.Unlock()
	}
}
func (s *H2Server) Close() {
	s.closeOnce.Do(func() {
		s.cancel()
		_ = s.listener.Close()
		s.mu.Lock()
		for c := range s.conns {
			_ = c.conn.Close()
		}
		s.mu.Unlock()
		s.wg.Wait()
	})
}

func (s *H2Server) accept() {
	defer s.wg.Done()
	for {
		raw, err := s.listener.Accept()
		if err != nil {
			if s.ctx.Err() == nil {
				s.record(err)
			}
			return
		}
		s.connections.Add(1)
		ctx, cancel := context.WithCancel(s.ctx)
		c := &H2Conn{conn: tls.Server(raw, s.tlsConfig), ctx: ctx, cancel: cancel, done: make(chan struct{}), settingsACK: make(chan struct{})}
		s.mu.Lock()
		if s.ctx.Err() != nil {
			s.mu.Unlock()
			_ = raw.Close()
			cancel()
			return
		}
		s.conns[c] = struct{}{}
		s.mu.Unlock()
		s.wg.Add(1)
		go s.serve(c)
	}
}

// H2Conn 提供受控的协议写入及对端关闭/SETTINGS 到达信号。
type H2Conn struct {
	conn        *tls.Conn
	ctx         context.Context
	cancel      context.CancelFunc
	done        chan struct{}
	settingsACK chan struct{}
	ackOnce     sync.Once
	writeMu     sync.Mutex
}

func (c *H2Conn) Done() <-chan struct{}                 { return c.done }
func (c *H2Conn) SettingsAcknowledged() <-chan struct{} { return c.settingsACK }
func (c *H2Conn) Close() error                          { return c.conn.Close() }
func (c *H2Conn) SendGoAway(lastStreamID, errorCode uint32) error {
	p := make([]byte, 8)
	binary.BigEndian.PutUint32(p, lastStreamID&0x7fffffff)
	binary.BigEndian.PutUint32(p[4:], errorCode)
	return c.WriteFrame(H2Frame{Type: FrameGoAway, Payload: p})
}
func (c *H2Conn) ResetStream(streamID, errorCode uint32) error {
	p := make([]byte, 4)
	binary.BigEndian.PutUint32(p, errorCode)
	return c.WriteFrame(H2Frame{Type: FrameRSTStream, StreamID: streamID, Payload: p})
}
func (c *H2Conn) WriteFrame(f H2Frame) error {
	if len(f.Payload) > 0xffffff || f.StreamID > 0x7fffffff {
		return fmt.Errorf("无效的 HTTP/2 帧大小或流 ID")
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	h := make([]byte, 9)
	h[0] = byte(len(f.Payload) >> 16)
	h[1] = byte(len(f.Payload) >> 8)
	h[2] = byte(len(f.Payload))
	h[3] = f.Type
	h[4] = f.Flags
	binary.BigEndian.PutUint32(h[5:], f.StreamID)
	// 安全截止时间限制异常 socket 样本的等待时间；测试通过 Gate 同步。
	if err := c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	if _, err := c.conn.Write(h); err != nil {
		return err
	}
	_, err := c.conn.Write(f.Payload)
	return err
}

func (s *H2Server) serve(c *H2Conn) {
	defer s.wg.Done()
	defer func() {
		c.cancel()
		_ = c.conn.Close()
		close(c.done)
		s.mu.Lock()
		delete(s.conns, c)
		s.mu.Unlock()
	}()
	if err := c.conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		s.record(err)
		return
	}
	if err := c.conn.HandshakeContext(c.ctx); err != nil {
		if s.ctx.Err() == nil {
			s.record(err)
		}
		return
	}
	if c.conn.ConnectionState().NegotiatedProtocol != "h2" {
		s.record(fmt.Errorf("对端未协商 h2"))
		return
	}
	preface := make([]byte, 24)
	if _, err := io.ReadFull(c.conn, preface); err != nil {
		s.record(err)
		return
	}
	if string(preface) != "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n" {
		s.record(fmt.Errorf("无效的 HTTP/2 连接前言"))
		return
	}
	var settings []byte
	if s.opts.InitialWindowSize != nil {
		settings = make([]byte, 6)
		binary.BigEndian.PutUint16(settings, 4)
		binary.BigEndian.PutUint32(settings[2:], *s.opts.InitialWindowSize)
	}
	if err := c.WriteFrame(H2Frame{Type: FrameSettings, Payload: settings}); err != nil {
		s.record(err)
		return
	}
	if err := c.conn.SetReadDeadline(time.Time{}); err != nil {
		s.record(err)
		return
	}
	for {
		f, err := readH2Frame(c.conn)
		if err != nil {
			return
		} // 对端取消或关闭属于预期的样本结束方式。
		switch f.Type {
		case FrameSettings:
			if f.Flags&1 != 0 {
				c.ackOnce.Do(func() { close(c.settingsACK) })
			} else if err := c.WriteFrame(H2Frame{Type: FrameSettings, Flags: 1}); err != nil {
				s.record(err)
				return
			}
		case FramePing:
			if f.Flags&1 == 0 {
				if err := c.WriteFrame(H2Frame{Type: FramePing, Flags: 1, Payload: f.Payload}); err != nil {
					s.record(err)
					return
				}
			}
		case FrameData:
			s.dataBytes.Add(int64(len(f.Payload)))
		case FrameHeaders:
			s.headers.Add(1)
			s.headersOnce.Do(func() { close(s.headersReached) })
			if s.opts.OnHeaders != nil {
				s.wg.Add(1)
				go func() {
					defer s.wg.Done()
					err := s.opts.OnHeaders(c.ctx, c, f)
					if c.ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, net.ErrClosed)) {
						return // 样本脚本取消属于正常清理。
					}
					s.record(err)
				}()
			}
		}
	}
}

func readH2Frame(r io.Reader) (H2Frame, error) {
	h := make([]byte, 9)
	if _, err := io.ReadFull(r, h); err != nil {
		return H2Frame{}, err
	}
	n := int(h[0])<<16 | int(h[1])<<8 | int(h[2])
	if n > 1<<20 {
		return H2Frame{}, fmt.Errorf("样本帧超过 1 MiB")
	}
	f := H2Frame{Type: h[3], Flags: h[4], StreamID: binary.BigEndian.Uint32(h[5:]) & 0x7fffffff, Payload: make([]byte, n)}
	_, err := io.ReadFull(r, f.Payload)
	return f, err
}
