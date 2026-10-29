package relay

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"relayleaf/internal/catalog"
	"relayleaf/internal/config"
	"relayleaf/internal/testkit"
)

const lifecycleClientKey = "cccccccccccccccccccccccccccccccc"
const lifecycleTarget = "example-provider/example-model/responses"
const lifecycleQuota = "example-provider/shared"
const lifecycleCreatedEvent = "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp-life\",\"status\":\"in_progress\"}}\n\n"
const lifecycleCompletedJSON = `{"id":"resp-life","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"完成"}]}]}`

func lifecycleSnapshot(t *testing.T, origin string, execution map[string]string) *catalog.Snapshot {
	t.Helper()
	source := strings.Replace(string(config.Example()), "https://api.example.com", origin, 1)
	source = strings.Replace(source, "    credentials:\n", "    quota_scopes:\n      shared:\n        owner: {project: lifecycle-project}\n        limits: {max_inflight: 4}\n    credentials:\n", 1)
	source = strings.Replace(source, "        secret_ref: env:RELAYLEAF_UPSTREAM_KEY\n", "        secret_ref: env:RELAYLEAF_UPSTREAM_KEY\n        identity:\n          attributes: {project: lifecycle-project}\n        quota_scopes: [shared]\n", 1)
	source = strings.Replace(source, "empty_response: {enabled: true}", "empty_response: {enabled: false}", 1)
	var names []string
	for name := range execution {
		names = append(names, name)
	}
	sort.Strings(names)
	var fields strings.Builder
	for _, name := range names {
		fmt.Fprintf(&fields, "  %s: %s\n", name, execution[name])
	}
	source = strings.Replace(source, "execution_defaults:\n", "execution_defaults:\n"+fields.String(), 1)
	path := filepath.Join(t.TempDir(), "relayleaf.yaml")
	if err := os.WriteFile(path, []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
	snapshot, err := config.LoadFile(context.Background(), path, config.Options{ResolveSecrets: true, LookupEnv: func(name string) (string, bool) {
		switch name {
		case "RELAYLEAF_CLIENT_KEY":
			return lifecycleClientKey, true
		case "RELAYLEAF_UPSTREAM_KEY":
			return "lifecycle-upstream-secret", true
		default:
			return "", false
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

type lifecycleClock struct {
	clock *testkit.Clock
	short chan struct{}
}

func newLifecycleClock() *lifecycleClock {
	return &lifecycleClock{clock: testkit.NewClock(time.Now()), short: make(chan struct{}, 64)}
}

func (c *lifecycleClock) newTimer(d time.Duration) Timer {
	timer := c.clock.NewTimer(d)
	if d > 0 && d <= time.Second {
		select {
		case c.short <- struct{}{}:
		default:
		}
	}
	return timer
}

func lifecycleSignal(t *testing.T, signal <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("等待%s超过安全时限", name)
	}
}

func lifecycleUpstream(t *testing.T, h2 bool, handler http.Handler) *httptest.Server {
	t.Helper()
	s := httptest.NewUnstartedServer(handler)
	s.EnableHTTP2 = h2
	if h2 {
		s.StartTLS()
	} else {
		s.Start()
	}
	t.Cleanup(s.Close)
	return s
}

func lifecycleHandler(t *testing.T, upstream *httptest.Server, clock *lifecycleClock, execution map[string]string) *Handler {
	t.Helper()
	base := upstream.Client().Transport.(*http.Transport).Clone()
	base.Protocols = new(http.Protocols)
	base.Protocols.SetHTTP1(true)
	base.Protocols.SetHTTP2(upstream.EnableHTTP2)
	h, err := NewHandler(lifecycleSnapshot(t, upstream.URL, execution), HandlerOptions{BaseTransport: base, Now: clock.clock.Now, NewTimer: clock.newTimer})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h
}

func lifecycleGateway(t *testing.T, h *Handler) (*httptest.Server, <-chan struct{}) {
	return lifecycleGatewayProtocol(t, h, false)
}

func lifecycleGatewayProtocol(t *testing.T, h *Handler, h2 bool) (*httptest.Server, <-chan struct{}) {
	t.Helper()
	done := make(chan struct{}, 8)
	s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() { done <- struct{}{} }()
		if h2 && r.ProtoMajor != 2 {
			t.Error("HTTP/2 下游没有协商到 HTTP/2")
		}
		h.ServeHTTP(w, r)
	}))
	s.EnableHTTP2 = h2
	if h2 {
		s.StartTLS()
	} else {
		s.Start()
	}
	s.Client().Timeout = 5 * time.Second
	t.Cleanup(func() { _ = h.Close(); s.Close() })
	return s, done
}

func lifecycleRequest(t *testing.T, ctx context.Context, url string, stream bool) *http.Request {
	t.Helper()
	body := fmt.Sprintf(`{"model":"example-assistant","input":"生命周期验证","stream":%t}`, stream)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url+"/v1/responses", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+lifecycleClientKey)
	return req
}

func lifecycleEmpty(t *testing.T, h *Handler, cancelled bool) {
	t.Helper()
	s := h.Stats()
	if s.Inbound != 0 || s.ActiveAttempts != 0 || s.BodyBytes != 0 || s.ResponseBytes != 0 || s.Targets[lifecycleTarget] != 0 || s.Quotas[lifecycleQuota] != 0 {
		t.Fatalf("请求结束后占用没有回落：%+v", s)
	}
	transport := h.TransportStats()
	if transport.Busy != 0 || cancelled && transport != (Stats{}) {
		t.Fatalf("请求结束后上游 I/O 尚未收尾：%+v", transport)
	}
}

type lifecycleInputBody struct {
	io.ReadCloser
	readOnce sync.Once
	reading  chan struct{}
}

func (b *lifecycleInputBody) Read(p []byte) (int, error) {
	b.readOnce.Do(func() { close(b.reading) })
	return b.ReadCloser.Read(p)
}

func TestHandlerBlockedInputTimeoutReleasesAdmission(t *testing.T) {
	var outbound atomic.Int64
	upstream := lifecycleUpstream(t, false, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { outbound.Add(1) }))
	clock := newLifecycleClock()
	h := lifecycleHandler(t, upstream, clock, map[string]string{"request_body_read_timeout": "1s"})
	reading, done := make(chan struct{}), make(chan struct{})
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		r.Body = &lifecycleInputBody{ReadCloser: r.Body, reading: reading}
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(func() { _ = h.Close(); gateway.Close() })
	conn, err := net.DialTimeout("tcp", gateway.Listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	_, err = fmt.Fprintf(conn, "POST /v1/responses HTTP/1.1\r\nHost: local\r\nAuthorization: Bearer %s\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{", lifecycleClientKey)
	if err != nil {
		t.Fatal(err)
	}
	lifecycleSignal(t, reading, "入站正文阻塞读取")
	lifecycleSignal(t, clock.short, "正文读取计时器")
	clock.clock.Advance(time.Second)
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if response.StatusCode != 408 || response.Header.Get("X-RelayLeaf-Error-Code") != "request_body_timeout" {
		t.Fatalf("正文超时返回了错误结果：%d %v", response.StatusCode, response.Header)
	}
	lifecycleSignal(t, done, "正文超时请求清理")
	lifecycleEmpty(t, h, true)
	if outbound.Load() != 0 || h.Stats().Attempts != 0 {
		t.Fatal("未完整接收正文仍发起了上游尝试")
	}
}

func TestHandlerUpstreamPhaseTimeoutCancelsIO(t *testing.T) {
	for _, h2 := range []bool{false, true} {
		for _, stage := range []string{"header", "first_event", "idle"} {
			t.Run(fmt.Sprintf("h2=%t/%s", h2, stage), func(t *testing.T) {
				reached, cancelled := make(chan struct{}), make(chan struct{})
				upstream := lifecycleUpstream(t, h2, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if h2 && r.ProtoMajor != 2 {
						t.Error("HTTP/2 上游没有协商到 HTTP/2")
					}
					_, _ = io.Copy(io.Discard, r.Body)
					if stage != "header" {
						w.Header().Set("Content-Type", "text/event-stream")
						w.WriteHeader(200)
						if stage == "idle" {
							_, _ = io.WriteString(w, lifecycleCreatedEvent)
						}
						_ = http.NewResponseController(w).Flush()
					}
					close(reached)
					<-r.Context().Done()
					close(cancelled)
				}))
				clock := newLifecycleClock()
				execution := map[string]string{"response_header_timeout": "10s", "stream_first_event_timeout": "10s", "stream_idle_timeout": "10s"}
				field := map[string]string{"header": "response_header_timeout", "first_event": "stream_first_event_timeout", "idle": "stream_idle_timeout"}[stage]
				execution[field] = "1s"
				h := lifecycleHandler(t, upstream, clock, execution)
				gateway, done := lifecycleGatewayProtocol(t, h, h2)
				client := gateway.Client()
				client.Timeout = 5 * time.Second
				result := make(chan *http.Response, 1)
				failure := make(chan error, 1)
				go func() {
					response, err := client.Do(lifecycleRequest(t, context.Background(), gateway.URL, true))
					if err != nil {
						failure <- err
						return
					}
					result <- response
				}()
				lifecycleSignal(t, reached, "上游阶段")
				lifecycleSignal(t, clock.short, "上游阶段计时器")
				clock.clock.Advance(time.Second)
				var response *http.Response
				select {
				case response = <-result:
				case err := <-failure:
					if stage == "header" {
						t.Fatalf("尚未提交的头部超时没有返回网关错误：%v", err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("上游阶段超时没有结束下游请求")
				}
				if response != nil {
					_, _ = io.Copy(io.Discard, response.Body)
					_ = response.Body.Close()
					if stage == "header" && (response.StatusCode != 504 || response.Header.Get("X-RelayLeaf-Error-Code") != "upstream_timeout") {
						t.Fatalf("头部超时没有生成预期的未提交错误：%d %v", response.StatusCode, response.Header)
					}
				}
				lifecycleSignal(t, cancelled, "上游 I/O 取消")
				lifecycleSignal(t, done, "阶段超时执行清理")
				lifecycleEmpty(t, h, true)
				if h.Stats().Attempts != 1 {
					t.Fatal("上游超时没有保留恰好一次真实尝试")
				}
				recent := h.Recent()
				if len(recent) != 1 || recent[0].Attempt == nil || recent[0].Attempt.FailureKind != field || recent[0].Attempt.ErrorSource != "upstream" {
					t.Fatalf("计时器来源没有准确记录：%+v", recent)
				}
			})
		}
	}
}

func TestHandlerCallerCancellationReleasesIOAndLeases(t *testing.T) {
	for _, h2 := range []bool{false, true} {
		t.Run(fmt.Sprintf("h2=%t", h2), func(t *testing.T) {
			cancelled := make(chan struct{})
			upstream := lifecycleUpstream(t, h2, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, lifecycleCreatedEvent)
				_ = http.NewResponseController(w).Flush()
				<-r.Context().Done()
				close(cancelled)
			}))
			clock := newLifecycleClock()
			h := lifecycleHandler(t, upstream, clock, nil)
			gateway, done := lifecycleGatewayProtocol(t, h, h2)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			response, err := gateway.Client().Do(lifecycleRequest(t, ctx, gateway.URL, true))
			if err != nil {
				t.Fatal(err)
			}
			reader := bufio.NewReader(response.Body)
			for {
				line, err := reader.ReadString('\n')
				if err != nil {
					t.Fatal(err)
				}
				if line == "\n" {
					break
				}
			}
			cancel()
			_ = response.Body.Close()
			lifecycleSignal(t, cancelled, "调用方取消传播到上游")
			lifecycleSignal(t, done, "调用方取消的执行清理")
			lifecycleEmpty(t, h, true)
		})
	}
}

func TestHandlerUpstreamEOFReleasesUpstreamBeforeDownstreamDrains(t *testing.T) {
	upstream := lifecycleUpstream(t, false, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", fmt.Sprint(len(lifecycleCompletedJSON)))
		_, _ = io.WriteString(w, lifecycleCompletedJSON)
	}))
	clock := newLifecycleClock()
	h := lifecycleHandler(t, upstream, clock, map[string]string{"downstream_write_idle_timeout": "5s"})
	serverConn, client := net.Pipe()
	peer := &slowPeerConn{Conn: serverConn}
	done := make(chan struct{})
	gateway := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		h.ServeHTTP(w, r)
	}))
	gateway.Listener = &singleConnListener{Listener: gateway.Listener, conn: peer}
	gateway.Start()
	t.Cleanup(func() { _ = client.Close(); _ = h.Close(); gateway.Close() })
	_ = client.SetDeadline(time.Now().Add(5 * time.Second))
	body := `{"model":"example-assistant","input":"验证下游排空"}`
	_, err := fmt.Fprintf(client, "POST /v1/responses HTTP/1.1\r\nHost: local\r\nAuthorization: Bearer %s\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s", lifecycleClientKey, len(body), body)
	if err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(client)
	response, err := http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	// net.Pipe 不缓存数据。响应头刷新后读取一个字节，余下正文仍阻塞当前 Write。
	if reader.Buffered() != 0 {
		t.Fatal("读取响应头时预取了正文，无法验证真实下游背压")
	}
	if _, err := io.ReadFull(client, make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	if peer.active.Load() != 1 || peer.writeSize.Load() <= 1 {
		t.Fatal("下游正文没有稳定停在阻塞写入状态")
	}
	s := h.Stats()
	if s.Inbound != 1 || s.ActiveAttempts != 0 || s.Targets[lifecycleTarget] != 0 || s.Quotas[lifecycleQuota] != 0 || h.TransportStats().Busy != 0 {
		t.Fatalf("上游EOF后下游仍未排空时的租约错误：%+v，传输=%+v", s, h.TransportStats())
	}
	if _, err := io.CopyN(io.Discard, client, response.ContentLength-1); err != nil {
		t.Fatal(err)
	}
	lifecycleSignal(t, done, "下游最终排空")
	_ = client.Close()
	_ = response.Body.Close()
	lifecycleEmpty(t, h, false)
}

func TestHandlerClientCapacityRejectionDoesNotStartAnotherAttempt(t *testing.T) {
	var outbound atomic.Int64
	cancelled := make(chan struct{}, 4)
	upstream := lifecycleUpstream(t, false, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		outbound.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, lifecycleCreatedEvent)
		_ = http.NewResponseController(w).Flush()
		<-r.Context().Done()
		cancelled <- struct{}{}
	}))
	clock := newLifecycleClock()
	h := lifecycleHandler(t, upstream, clock, nil)
	gateway, done := lifecycleGateway(t, h)
	var cancels []context.CancelFunc
	var responses []*http.Response
	defer func() {
		for _, cancel := range cancels {
			cancel()
		}
		for _, response := range responses {
			_ = response.Body.Close()
		}
	}()
	for range 4 {
		ctx, cancel := context.WithCancel(context.Background())
		cancels = append(cancels, cancel)
		response, err := gateway.Client().Do(lifecycleRequest(t, ctx, gateway.URL, true))
		if err != nil {
			t.Fatal(err)
		}
		responses = append(responses, response)
		if response.StatusCode != 200 {
			t.Fatalf("没有建立预期的四个在途请求：%d", response.StatusCode)
		}
	}
	response, err := gateway.Client().Do(lifecycleRequest(t, context.Background(), gateway.URL, false))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	lifecycleSignal(t, done, "客户端容量拒绝")
	if response.StatusCode != 429 || response.Header.Get("X-RelayLeaf-Error-Code") != "client_concurrency_limited" || outbound.Load() != 4 || h.Stats().Attempts != 4 {
		t.Fatalf("容量拒绝产生了额外出站或错误结果：状态=%d，出站=%d，统计=%+v", response.StatusCode, outbound.Load(), h.Stats())
	}
	for i, cancel := range cancels {
		cancel()
		_ = responses[i].Body.Close()
	}
	for range 4 {
		lifecycleSignal(t, cancelled, "容量测试的上游取消")
		lifecycleSignal(t, done, "容量测试的在途清理")
	}
	lifecycleEmpty(t, h, true)
}

func TestHandlerAdmissionRejectionHasNoOutboundAttempt(t *testing.T) {
	var outbound atomic.Int64
	upstream := lifecycleUpstream(t, false, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { outbound.Add(1) }))
	clock := newLifecycleClock()
	h := lifecycleHandler(t, upstream, clock, nil)
	h.StopAccepting()
	gateway, done := lifecycleGateway(t, h)
	response, err := gateway.Client().Do(lifecycleRequest(t, context.Background(), gateway.URL, false))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	lifecycleSignal(t, done, "停止接纳后的拒绝")
	if response.StatusCode != 503 || outbound.Load() != 0 || h.Stats().Attempts != 0 {
		t.Fatalf("停止接纳仍发生出站：状态=%d，次数=%d", response.StatusCode, outbound.Load())
	}
	lifecycleEmpty(t, h, true)
}

func TestHandlerConnectionFailureCountsOneAttempt(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	origin := "http://" + listener.Addr().String()
	_ = listener.Close()
	clock := newLifecycleClock()
	h, err := NewHandler(lifecycleSnapshot(t, origin, nil), HandlerOptions{Now: clock.clock.Now, NewTimer: clock.newTimer})
	if err != nil {
		t.Fatal(err)
	}
	gateway, done := lifecycleGateway(t, h)
	response, err := gateway.Client().Do(lifecycleRequest(t, context.Background(), gateway.URL, false))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	lifecycleSignal(t, done, "连接失败后的执行清理")
	if response.StatusCode != 502 || h.Stats().Attempts != 1 {
		t.Fatalf("连接失败没有保留一次真实尝试：状态=%d，统计=%+v", response.StatusCode, h.Stats())
	}
	lifecycleEmpty(t, h, true)
}
