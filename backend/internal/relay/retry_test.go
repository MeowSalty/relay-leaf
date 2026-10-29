package relay

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"relayleaf/internal/testkit"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestH2SingleAttemptAndStandardRetryControl(t *testing.T) {
	for _, fault := range []string{"goaway", "refused", "protocol"} {
		for _, adapter := range []bool{true, false} {
			t.Run(fault+map[bool]string{true: "/adapter", false: "/standard"}[adapter], func(t *testing.T) {
				var headers atomic.Int64
				s, e := testkit.NewH2Server(testkit.H2Options{OnHeaders: func(ctx context.Context, c *testkit.H2Conn, f testkit.H2Frame) error {
					if headers.Add(1) == 1 {
						switch fault {
						case "goaway":
							return c.SendGoAway(0, testkit.ErrorCodeNo)
						case "refused":
							return c.ResetStream(f.StreamID, testkit.ErrorCodeRefusedStream)
						default:
							return c.ResetStream(f.StreamID, testkit.ErrorCodeProtocol)
						}
					}
					return c.WriteFrame(testkit.H2Frame{Type: testkit.FrameHeaders, Flags: 5, StreamID: f.StreamID, Payload: []byte{0x88}})
				}})
				if e != nil {
					t.Fatal(e)
				}
				defer s.Close()
				base := &http.Transport{TLSClientConfig: s.TLSConfig(), ForceAttemptHTTP2: true}
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				r := request(t, "POST", s.URL, strings.NewReader(`{"model":"x"}`))
				r.Header.Set("Idempotency-Key", "attempt-test")
				if adapter {
					tr := newTestTransport(t, base, 1)
					var starts int
					a, err := tr.Start(ctx, r, func() error { starts++; return nil })
					if a != nil {
						a.Close()
					}
					if err == nil || starts != 1 {
						t.Fatalf("预期只有一次失败的请求尝试: %v %d", err, starts)
					}
					_, n := s.Counts()
					if n != 1 {
						t.Fatalf("发生了隐式重发: %d", n)
					}
				} else {
					defer base.CloseIdleConnections()
					resp, err := base.RoundTrip(r.WithContext(ctx))
					if err != nil {
						t.Fatal(err)
					}
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
					_, n := s.Counts()
					if n != 2 {
						t.Fatalf("阳性对照未重发: %d", n)
					}
				}
			})
		}
	}
}

func TestH2NoWindowUpdateCancellation(t *testing.T) {
	zero := uint32(0)
	peer := make(chan *testkit.H2Conn, 1)
	s, e := testkit.NewH2Server(testkit.H2Options{InitialWindowSize: &zero, OnHeaders: func(ctx context.Context, c *testkit.H2Conn, f testkit.H2Frame) error {
		select {
		case peer <- c:
		default:
		}
		return nil
	}})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	var raw *observedConn
	base := &http.Transport{TLSClientConfig: s.TLSConfig(), ForceAttemptHTTP2: true, DialContext: func(ctx context.Context, n, a string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, n, a)
		if err != nil {
			return nil, err
		}
		raw = &observedConn{Conn: conn, closed: make(chan struct{})}
		return raw, nil
	}}
	tr := newTestTransport(t, base, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		tr.Start(ctx, request(t, "POST", s.URL, io.LimitReader(zeroReader{}, 1<<20)), commitNoop)
	}()
	waitDone(t, s.HeadersReached())
	c := <-peer
	waitDone(t, c.SettingsAcknowledged())
	cancel()
	waitDone(t, done)
	waitDone(t, c.Done())
	waitDone(t, raw.closed)
	if raw.active.Load() != 0 || tr.Stats() != (Stats{}) {
		t.Fatal("HTTP/2 I/O 尚未排空")
	}
	_, n := s.Counts()
	if n != 1 {
		t.Fatal("HTTP/2 发生重发")
	}
	if s.DataBytes() >= 1<<20 {
		t.Fatal("测试夹具未阻塞流量控制")
	}
}

var injectedIO = errors.New("注入的 Transport 故障")

type faultConn struct {
	net.Conn
	arm       *atomic.Bool
	mode      string
	readFault atomic.Bool
}

func (c *faultConn) Write(p []byte) (int, error) {
	if c.arm.CompareAndSwap(true, false) {
		switch c.mode {
		case "nothing-written":
			return 0, injectedIO
		case "partial-write":
			n, e := c.Conn.Write(p[:1])
			if e != nil {
				return n, e
			}
			return n, injectedIO
		case "read-failure":
			c.readFault.Store(true)
			n, e := c.Conn.Write(p)
			c.Conn.SetReadDeadline(time.Now())
			return n, e
		}
	}
	return c.Conn.Write(p)
}
func (c *faultConn) Read(p []byte) (int, error) {
	n, e := c.Conn.Read(p)
	if e != nil && c.readFault.Load() {
		return n, injectedIO
	}
	return n, e
}

func TestH1FaultsNeverResend(t *testing.T) {
	for _, fault := range []string{"nothing-written", "partial-write", "read-failure", "idle-close"} {
		for _, variant := range []string{"get", "post-replay", "post-empty", "post-no-getbody"} {
			for _, adapter := range []bool{true, false} {
				t.Run(fault+"/"+variant+map[bool]string{true: "/adapter", false: "/standard"}[adapter], func(t *testing.T) {
					var requests, accepts atomic.Int64
					var arm atomic.Bool
					s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						n := requests.Add(1)
						io.Copy(io.Discard, r.Body)
						if fault == "read-failure" && n == 2 {
							// 避免该处理器成功返回的回环响应
							// 与注入的首字节读取错误发生竞态。
							<-r.Context().Done()
							return
						}
						if fault == "idle-close" && n == 2 {
							c, _, e := w.(http.Hijacker).Hijack()
							if e == nil {
								c.Close()
							}
							return
						}
						w.Header().Set("Content-Length", "2")
						io.WriteString(w, "ok")
					}))
					s.Config.ConnState = func(_ net.Conn, state http.ConnState) {
						if state == http.StateNew {
							accepts.Add(1)
						}
					}
					s.Start()
					defer s.Close()
					base := &http.Transport{DialContext: func(ctx context.Context, n, a string) (net.Conn, error) {
						c, e := (&net.Dialer{}).DialContext(ctx, n, a)
						if e != nil {
							return nil, e
						}
						return &faultConn{Conn: c, arm: &arm, mode: fault}, nil
					}}
					var tr *Transport
					if adapter {
						tr = newTestTransport(t, base, 1)
					} else {
						defer base.CloseIdleConnections()
					}
					ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
					defer cancel()
					do := func(r *http.Request) error {
						if adapter {
							a, e := tr.Start(ctx, r, commitNoop)
							if e != nil {
								return e
							}
							_, e = io.Copy(io.Discard, a.Response.Body)
							a.Close()
							return e
						}
						resp, e := base.RoundTrip(r.WithContext(ctx))
						if e != nil {
							return e
						}
						_, e = io.Copy(io.Discard, resp.Body)
						resp.Body.Close()
						return e
					}
					if e := do(request(t, "GET", s.URL, nil)); e != nil {
						t.Fatal(e)
					}
					arm.Store(true)
					r := request(t, "POST", s.URL, nil)
					switch variant {
					case "get":
						r = request(t, "GET", s.URL, nil)
					case "post-replay":
						r = request(t, "POST", s.URL, strings.NewReader("original"))
						r.Header.Set("Idempotency-Key", "replay-test")
					case "post-no-getbody":
						r = request(t, "POST", s.URL, strings.NewReader("original"))
						r.GetBody = nil
					}
					err := do(r)
					wantRetry := !adapter && fault != "partial-write" && ((fault == "nothing-written" && variant != "post-no-getbody") || ((fault == "read-failure" || fault == "idle-close") && (variant == "get" || variant == "post-replay")))
					if wantRetry {
						if err != nil || accepts.Load() != 2 {
							t.Fatalf("标准阳性对照: err=%v accepts=%d", err, accepts.Load())
						}
					} else {
						if err == nil || accepts.Load() != 1 {
							t.Fatalf("发生重发: err=%v accepts=%d", err, accepts.Load())
						}
					}
				})
			}
		}
	}
}
