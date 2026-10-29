package testkit

import (
	"context"
	"encoding/binary"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func h2Client(s *H2Server) (*http.Client, *http.Transport) {
	tr := &http.Transport{TLSClientConfig: s.TLSConfig(), ForceAttemptHTTP2: true, DisableCompression: true}
	return &http.Client{Transport: tr, Timeout: 3 * time.Second}, tr
}

func TestH2WireFaults(t *testing.T) {
	for _, kind := range []string{"protocol-reset", "goaway", "refused"} {
		t.Run(kind, func(t *testing.T) {
			s, err := NewH2Server(H2Options{OnHeaders: func(ctx context.Context, c *H2Conn, f H2Frame) error {
				if kind == "goaway" {
					return c.SendGoAway(0, ErrorCodeProtocol)
				}
				if kind == "refused" {
					return c.ResetStream(f.StreamID, ErrorCodeRefusedStream)
				}
				return c.ResetStream(f.StreamID, ErrorCodeProtocol)
			}})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			_, tr := h2Client(s)
			defer tr.CloseIdleConnections()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			cc, err := tr.NewClientConn(ctx, "https", strings.TrimPrefix(s.URL, "https://"))
			if err != nil {
				t.Fatal(err)
			}
			defer cc.Close()
			req, err := http.NewRequestWithContext(ctx, "GET", s.URL, nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := cc.RoundTrip(req)
			if resp != nil {
				_ = resp.Body.Close()
			}
			if err == nil {
				t.Fatal("协议故障被错误接受")
			}
			conn, heads := s.Counts()
			want := int64(1)
			if heads != want || conn != 1 {
				t.Fatalf("协议计数：连接数=%d HEADERS 数=%d，预期 1/%d", conn, heads, want)
			}
			if len(s.Errors()) != 0 {
				t.Fatalf("样本错误：%v", s.Errors())
			}
		})
	}
}

func TestH2ZeroWindowAndCancellation(t *testing.T) {
	zero := uint32(0)
	postReached := NewGate()
	var calls atomic.Int64
	s, err := NewH2Server(H2Options{InitialWindowSize: &zero, OnHeaders: func(ctx context.Context, c *H2Conn, f H2Frame) error {
		if calls.Add(1) == 1 {
			select {
			case <-c.SettingsAcknowledged():
			case <-ctx.Done():
				return ctx.Err()
			}
			// HPACK 静态表索引 8 为 :status=200。结束此次预热请求，使
			// 下一个 POST 复用已确认零窗口的对端连接。
			return c.WriteFrame(H2Frame{Type: FrameHeaders, Flags: 5, StreamID: f.StreamID, Payload: []byte{0x88}})
		}
		return postReached.Wait(ctx)
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	client, tr := h2Client(s)
	defer tr.CloseIdleConnections()
	resp, err := client.Get(s.URL)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "POST", s.URL, strings.NewReader(strings.Repeat("x", 128<<10)))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		resp, err := client.Do(req)
		if resp != nil {
			_ = resp.Body.Close()
		}
		done <- err
	}()
	waitSignal(t, postReached.Reached())
	if got := s.DataBytes(); got != 0 {
		t.Fatalf("已确认零窗口后仍发送 DATA：%d", got)
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("取消后请求仍成功")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("阻塞的写入未被取消")
	}
	postReached.Open()
	conn, heads := s.Counts()
	if conn != 1 || heads != 2 {
		t.Fatalf("连接数=%d HEADERS 数=%d", conn, heads)
	}
}

// 阳性对照证明，仅统计外层 Do/RoundTrip 调用次数并不充分。
func TestStandardTransportRetryHasMultipleWireRequests(t *testing.T) {
	for _, code := range []uint32{ErrorCodeRefusedStream, ErrorCodeProtocol} {
		var calls atomic.Int64
		s, err := NewH2Server(H2Options{OnHeaders: func(ctx context.Context, c *H2Conn, f H2Frame) error {
			if calls.Add(1) == 1 {
				return c.ResetStream(f.StreamID, code)
			}
			return c.WriteFrame(H2Frame{Type: FrameHeaders, Flags: 5, StreamID: f.StreamID, Payload: []byte{0x88}})
		}})
		if err != nil {
			t.Fatal(err)
		}
		client, tr := h2Client(s)
		resp, err := client.Get(s.URL)
		if err != nil {
			tr.CloseIdleConnections()
			s.Close()
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		conn, heads := s.Counts()
		tr.CloseIdleConnections()
		s.Close()
		if heads != 2 || conn < 1 || conn > 2 {
			t.Fatalf("错误码=%d 连接数=%d HEADERS 数=%d", code, conn, heads)
		}
	}
}

func TestH2FrameOwnedPayloadAndBounds(t *testing.T) {
	var head [9]byte
	head[2] = 4
	head[3] = FrameRSTStream
	binary.BigEndian.PutUint32(head[5:], 3)
	raw := append(head[:], 0, 0, 0, 7)
	f, err := readH2Frame(strings.NewReader(string(raw)))
	if err != nil || f.StreamID != 3 || binary.BigEndian.Uint32(f.Payload) != 7 {
		t.Fatalf("%v %v", f, err)
	}
	head[0] = 0x20
	if _, err := readH2Frame(strings.NewReader(string(head[:]))); err == nil {
		t.Fatal("未限制帧大小")
	}
	if _, err := readH2Frame(strings.NewReader("short")); err != io.ErrUnexpectedEOF {
		t.Fatal(err)
	}
}

func TestH2CloseCancelsBlockedScriptAndClosesSocket(t *testing.T) {
	g := NewGate()
	connSeen := make(chan *H2Conn, 1)
	s, err := NewH2Server(H2Options{OnHeaders: func(ctx context.Context, c *H2Conn, f H2Frame) error { connSeen <- c; return g.Wait(ctx) }})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	_, tr := h2Client(s)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cc, err := tr.NewClientConn(ctx, "https", strings.TrimPrefix(s.URL, "https://"))
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	req, err := http.NewRequestWithContext(ctx, "GET", s.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	requestDone := make(chan struct{})
	go func() {
		resp, _ := cc.RoundTrip(req)
		if resp != nil {
			_ = resp.Body.Close()
		}
		close(requestDone)
	}()
	waitSignal(t, g.Reached())
	c := <-connSeen
	s.Close()
	s.Close()
	waitSignal(t, c.Done())
	waitSignal(t, requestDone)
	if errs := s.Errors(); len(errs) != 0 {
		t.Fatalf("正常清理被报告为故障：%v", errs)
	}
}
