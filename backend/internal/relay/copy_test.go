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
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type writerWrapper struct{ http.ResponseWriter }

func (w writerWrapper) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func TestCopyFlushRawBytesTrailerAndUpstreamWait(t *testing.T) {
	var compressed bytes.Buffer
	gz := gzip.NewWriter(&compressed)
	gz.Write([]byte("data: first\n\ndata: second\n\n"))
	gz.Close()
	payload := compressed.Bytes()
	rd, wr := io.Pipe()
	defer wr.Close()
	done := make(chan error, 1)
	resp := &http.Response{StatusCode: 201, Header: http.Header{"Content-Encoding": []string{"gzip"}, "X-Upstream": []string{"one", "two"}}, Trailer: http.Header{"X-End": nil}, Body: rd}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		done <- CopyResponse(r.Context(), writerWrapper{w}, resp, 20*time.Millisecond)
	}))
	defer server.Close()
	base := &http.Transport{DisableCompression: true}
	defer base.CloseIdleConnections()
	client := &http.Client{Transport: base}
	response, err := client.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 201 || response.Header.Get("Content-Encoding") != "gzip" || len(response.Header.Values("X-Upstream")) != 2 {
		t.Fatal("响应头发生变化")
	}
	firstWritten := make(chan struct{})
	go func() { wr.Write(payload[:10]); close(firstWritten) }()
	first := make([]byte, 10)
	if _, err := io.ReadFull(response.Body, first); err != nil {
		t.Fatal(err)
	}
	waitDone(t, firstWritten)
	// 等待上游数据不占用下游写入的超时时间。
	time.Sleep(50 * time.Millisecond)
	go func() { wr.Write(payload[10:]); resp.Trailer.Set("X-End", "done"); wr.Close() }()
	rest, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(append(first, rest...), payload) || response.Trailer.Get("X-End") != "done" {
		t.Fatal("原始字节或 trailer 发生变化")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("复制未完成")
	}
}

type slowPeerConn struct {
	net.Conn
	active    atomic.Int64
	writeSize atomic.Int64
}

func (c *slowPeerConn) Write(p []byte) (int, error) {
	c.active.Add(1)
	defer c.active.Add(-1)
	c.writeSize.Store(int64(len(p)))
	return c.Conn.Write(p)
}

type singleConnListener struct {
	net.Listener
	conn net.Conn
	once sync.Once
}

func (l *singleConnListener) Accept() (net.Conn, error) {
	var conn net.Conn
	l.once.Do(func() { conn = l.conn })
	if conn != nil {
		return conn, nil
	}
	return l.Listener.Accept()
}

func TestCopySlowDownstreamEndsConnectionWrite(t *testing.T) {
	for _, idle := range []time.Duration{0, time.Second} {
		t.Run(map[bool]string{true: "idle-timeout", false: "cancel"}[idle > 0], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			conn, client := net.Pipe()
			peer := &slowPeerConn{Conn: conn}
			result := make(chan error, 1)
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				err := CopyResponse(ctx, writerWrapper{w}, &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(zeroReader{})}, idle)
				result <- err
				panic(http.ErrAbortHandler)
			}))
			server.Listener = &singleConnListener{Listener: server.Listener, conn: peer}
			server.Start()
			defer server.Close()
			defer client.Close()
			if err := client.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
				t.Fatal(err)
			}
			if _, err := io.WriteString(client, "GET / HTTP/1.1\r\nHost: local\r\n\r\n"); err != nil {
				t.Fatalf("发送请求: %v", err)
			}
			// CopyResponse 单独刷新响应头，读取这次写入不会消费正文。
			reader := bufio.NewReader(client)
			for {
				line, err := reader.ReadString('\n')
				if err != nil {
					t.Fatalf("读取响应头: %v", err)
				}
				if line == "\r\n" {
					break
				}
			}
			if reader.Buffered() != 0 {
				t.Fatal("读取响应头时预取了正文")
			}
			// net.Pipe 没有缓冲。只读一个字节，确保当前 Write 已开始，
			// 且剩余字节仍等待客户端读取，不依赖系统的 TCP 缓冲大小。
			if _, err := io.ReadFull(client, make([]byte, 1)); err != nil {
				t.Fatalf("读取响应头后的首字节: %v", err)
			}
			if peer.active.Load() != 1 || peer.writeSize.Load() <= 1 {
				t.Fatal("下游连接没有等待读取剩余字节的 Write")
			}
			if idle == 0 {
				cancel()
			}
			select {
			case err := <-result:
				if err == nil {
					t.Fatal("慢速写入端报告成功")
				}
				if idle > 0 && !os.IsTimeout(err) {
					t.Fatalf("预期超时: %v", err)
				}
				if idle == 0 && !errors.Is(err, context.Canceled) && !os.IsTimeout(err) {
					t.Fatalf("预期取消或写入截止时间错误: %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("取消或超时后被阻塞的写入操作仍在运行")
			}
			if peer.active.Load() != 0 {
				t.Fatal("底层下游 Write 仍在运行")
			}
		})
	}
}

func TestCopyPropagatesFlushError(t *testing.T) {
	err := CopyResponse(context.Background(), flushFailure{}, &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(nil))}, 0)
	if !errors.Is(err, injectedIO) {
		t.Fatalf("Flush 错误丢失: %v", err)
	}
}

type flushFailure struct{}

func TestCopyRemovesAllConnectionFields(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Connection", "X-First")
		w.Header().Add("Connection", "X-Second, X-Third")
		w.Header().Set("X-First", "secret")
		w.Header().Set("X-Second", "secret")
		w.Header().Set("X-Third", "secret")
		w.Header().Set("X-Preserve", "yes")
		io.WriteString(w, "body")
	}))
	defer up.Close()
	tr := newTestTransport(t, &http.Transport{}, 1)
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		a, e := tr.Start(r.Context(), request(t, "GET", up.URL, nil), commitNoop)
		if e != nil {
			t.Error(e)
			return
		}
		if e := CopyResponse(r.Context(), w, a.Response, 0); e != nil {
			t.Error(e)
		}
	}))
	defer down.Close()
	clientTransport := &http.Transport{}
	defer clientTransport.CloseIdleConnections()
	resp, e := (&http.Client{Transport: clientTransport}).Get(down.URL)
	if e != nil {
		t.Fatal(e)
	}
	defer resp.Body.Close()
	b, e := io.ReadAll(resp.Body)
	if e != nil || string(b) != "body" {
		t.Fatalf("响应体: %q %v", b, e)
	}
	for _, key := range []string{"Connection", "X-First", "X-Second", "X-Third"} {
		if resp.Header.Get(key) != "" {
			t.Fatalf("逐跳字段被泄漏: %s", key)
		}
	}
	if resp.Header.Get("X-Preserve") != "yes" {
		t.Fatal("端到端响应头丢失")
	}
}

func TestCopyH2TrailersCrossToH1(t *testing.T) {
	for _, announced := range []bool{true, false} {
		t.Run(map[bool]string{true: "announced-and-late", false: "only-late"}[announced], func(t *testing.T) {
			up := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Length", "4")
				if announced {
					w.Header().Set("Trailer", "X-Known")
				}
				io.WriteString(w, "body")
				if announced {
					w.Header().Set("X-Known", "known")
				}
				w.Header().Set(http.TrailerPrefix+"X-Late", "late")
			}))
			up.EnableHTTP2 = true
			up.StartTLS()
			defer up.Close()
			tr := newTestTransport(t, &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, ForceAttemptHTTP2: true}, 1)
			down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				a, e := tr.Start(r.Context(), request(t, "GET", up.URL, nil), commitNoop)
				if e != nil {
					t.Error(e)
					return
				}
				if a.Response.ProtoMajor != 2 {
					t.Error("上游协议不是 HTTP/2")
				}
				if a.Response.Header.Get("Content-Length") != "4" {
					t.Error("HTTP/2 夹具缺少 Content-Length")
				}
				if e := CopyResponse(r.Context(), w, a.Response, 0); e != nil {
					t.Error(e)
				}
			}))
			defer down.Close()
			clientTransport := &http.Transport{}
			defer clientTransport.CloseIdleConnections()
			resp, e := (&http.Client{Transport: clientTransport}).Get(down.URL)
			if e != nil {
				t.Fatal(e)
			}
			defer resp.Body.Close()
			if resp.ProtoMajor != 1 || resp.Header.Get("Content-Length") != "" {
				t.Fatal("HTTP/1 长度字段阻止了 trailer 传输")
			}
			b, e := io.ReadAll(resp.Body)
			if e != nil || string(b) != "body" {
				t.Fatalf("响应体: %q %v", b, e)
			}
			if resp.Trailer.Get("X-Late") != "late" {
				t.Fatalf("后续出现的 trailer 丢失: %+v", resp.Trailer)
			}
			if announced && resp.Trailer.Get("X-Known") != "known" {
				t.Fatal("已声明的 trailer 丢失")
			}
		})
	}
}

func (flushFailure) Header() http.Header              { return make(http.Header) }
func (flushFailure) Write(p []byte) (int, error)      { return len(p), nil }
func (flushFailure) WriteHeader(int)                  {}
func (flushFailure) SetWriteDeadline(time.Time) error { return nil }
func (flushFailure) FlushError() error                { return injectedIO }
