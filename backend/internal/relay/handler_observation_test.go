package relay

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"relayleaf/internal/catalog"
	"relayleaf/internal/observe"
	"relayleaf/internal/testkit"
)

const observationClientToken = "observation-client-token-0123456789"

func observationSample(t testing.TB, name string) []byte {
	t.Helper()
	raw, err := testkit.ReadSample(name)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// observationSnapshot 构造配套身份、两种模式和零重试的实际配置快照。
func observationSnapshot(origin string, change func(*catalog.Data)) *catalog.Snapshot {
	exec := catalog.Execution{MaxRetries: 0, MaxRetriesPerProvider: 0, ConnectTimeout: 3 * time.Second, TLSHandshakeTimeout: 3 * time.Second, ResponseHeaderTimeout: 3 * time.Second, StreamFirstEventTimeout: 3 * time.Second, NonStreamResponseTimeout: 3 * time.Second, StreamIdleTimeout: 3 * time.Second, DownstreamWriteIdleTimeout: 3 * time.Second, RequestBodyReadTimeout: 3 * time.Second, EmptyResponseEnabled: true, EmptyResponseMaxBufferBytes: 65536, EmptyResponseMaxWait: 2 * time.Second, MaxErrorBodyBytes: 16384, MaxErrorBodyWait: time.Second}
	endpoint := catalog.Endpoint{ID: "responses", Protocol: "openai.responses", Operation: "create", PathTemplate: "/v1/responses", AuthType: "bearer", RequestModes: []string{"non_stream", "stream"}}
	credential := catalog.Credential{ID: "key", Version: "v1", Enabled: true, Secret: catalog.NewSecret("observation-upstream-secret"), Execution: exec}
	provider := catalog.Provider{ID: "provider", Origin: origin, Enabled: true, Execution: exec, Health: catalog.Health{QuotaCooldown: time.Minute}, Endpoints: map[string]catalog.Endpoint{"responses": endpoint}, Credentials: map[string]catalog.Credential{"key": credential}}
	targetID := "provider/model/responses"
	target := catalog.Target{ID: targetID, ProviderID: "provider", ModelID: "model", EndpointID: "responses", UpstreamName: "upstream-model", Enabled: true, Credentials: []string{"key"}, RequestModes: []string{"non_stream", "stream"}, Capabilities: []string{"image_input", "tool_calling"}, Limits: catalog.LocalLimits{MaxInflight: 4}}
	route := catalog.Route{ID: "route", Enabled: true, Protocol: "openai.responses", Operation: "create", PublicModel: "public-model", ResourcePolicy: "stateless_pool", Targets: []catalog.RouteTarget{{Target: targetID, Priority: 10, Weight: 1}}}
	client := catalog.ClientKey{ID: "client", Version: "v1", Enabled: true, AllowedRoutes: []string{"route"}, Limits: catalog.LocalLimits{MaxInflight: 4}}
	d := catalog.Data{Gateway: "127.0.0.1:0", SecretsResolved: true, Providers: map[string]catalog.Provider{"provider": provider}, Targets: map[string]catalog.Target{targetID: target}, Routes: map[string]catalog.Route{"route": route}, ClientKeys: map[string]catalog.ClientKey{"client": client}, Verifications: []catalog.Verification{{ClientID: "client", Material: catalog.NewSecret(observationClientToken)}}, Limits: catalog.Limits{MaxInflight: 8, MaxRuntimeStateEntries: 64, MaxRequestBodyBytes: 65536, MaxDecompressedBodyBytes: 65536, MaxTotalBodyBufferBytes: 1 << 20, MaxTotalResponseBufferBytes: 1 << 20}, Execution: exec, Observability: catalog.Observability{RecentRequestCapacity: 32, MaxObservedJSONBytes: 65536, MaxObservedSSEEventBytes: 65536, MaxObservedStateBytes: 16384}}
	if change != nil {
		change(&d)
	}
	return catalog.NewSnapshot(d)
}
func observationExecution(d *catalog.Data, change func(*catalog.Execution)) {
	change(&d.Execution)
	p := d.Providers["provider"]
	change(&p.Execution)
	c := p.Credentials["key"]
	change(&c.Execution)
	p.Credentials["key"] = c
	d.Providers["provider"] = p
}
func observationGateway(t *testing.T, up *httptest.Server, change func(*catalog.Data), opts HandlerOptions) (*Handler, *httptest.Server) {
	t.Helper()
	h, err := NewHandler(observationSnapshot(up.URL, change), opts)
	if err != nil {
		t.Fatal(err)
	}
	down := httptest.NewServer(h)
	t.Cleanup(func() { _ = h.Close(); down.Close() })
	return h, down
}
func observationRequest(t *testing.T, url, body string) *http.Request {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, url+"/v1/responses", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer "+observationClientToken)
	return r
}
func observationCall(t *testing.T, url, body string) (*http.Response, []byte, error) {
	t.Helper()
	resp, err := http.DefaultClient.Do(observationRequest(t, url, body))
	if err != nil {
		t.Fatal(err)
	}
	raw, readErr := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	return resp, raw, readErr
}
func observationRecords(t *testing.T, h *Handler, want int) []observe.RequestRecord {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	poll := time.NewTicker(time.Millisecond)
	defer poll.Stop()
	for {
		records := h.Recent()
		if len(records) >= want {
			return records
		}
		select {
		case <-deadline.C:
			t.Fatalf("近期记录未达到%d，实际%d", want, len(records))
		case <-poll.C:
		}
	}
}
func observationNoOccupancy(t *testing.T, h *Handler) {
	t.Helper()
	s := h.Stats()
	if s.Inbound != 0 || s.ActiveAttempts != 0 || s.BodyBytes != 0 || s.ResponseBytes != 0 {
		t.Fatalf("请求结束后资源残留：%+v", s)
	}
}

func observationWait(t *testing.T, message string, ready func() bool) {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for !ready() {
		select {
		case <-deadline.C:
			t.Fatal(message)
		case <-tick.C:
		}
	}
}

func TestHandlerObservationEmptyCommitAndApplicability(t *testing.T) {
	for _, mode := range []string{"non_stream", "stream"} {
		for _, applicable := range []string{"enabled", "disabled", "unknown"} {
			t.Run(mode+"/"+applicable, func(t *testing.T) {
				name := "empty.json"
				media := "application/json"
				body := `{"model":"public-model"}`
				if mode == "stream" {
					name = "empty-lf.sse"
					media = "text/event-stream"
					body = `{"model":"public-model","stream":true}`
				}
				if applicable == "unknown" {
					body = strings.TrimSuffix(body, "}") + `,"vendor_execution_control":true}`
				}
				raw := observationSample(t, name)
				var count atomic.Int64
				up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					count.Add(1)
					w.Header().Set("Content-Type", media)
					w.Header().Set("X-Upstream-Resource-Id", "must-not-leak")
					_, _ = w.Write(raw)
				}))
				defer up.Close()
				change := func(d *catalog.Data) {
					if applicable == "disabled" {
						observationExecution(d, func(e *catalog.Execution) { e.EmptyResponseEnabled = false })
					}
				}
				h, down := observationGateway(t, up, change, HandlerOptions{})
				resp, received, err := observationCall(t, down.URL, body)
				if err != nil {
					t.Fatal(err)
				}
				if applicable == "enabled" {
					if resp.StatusCode != 502 || resp.Header.Get("X-RelayLeaf-Error-Code") != "upstream_empty_response" {
						t.Fatalf("零重试空回状态错误：%d %q %s", resp.StatusCode, resp.Header.Get("X-RelayLeaf-Error-Code"), received)
					}
					if bytes.Contains(received, []byte("resp_synthetic")) || bytes.Contains(received, []byte("response.created")) || resp.Header.Get("X-Upstream-Resource-Id") != "" {
						t.Fatalf("空回候选泄露：%v %s", resp.Header, received)
					}
				} else {
					if resp.StatusCode != 200 || !bytes.Equal(received, raw) || resp.Header.Get("X-Upstream-Resource-Id") != "must-not-leak" {
						t.Fatalf("不适用空回未原样交付：%d %v %s", resp.StatusCode, resp.Header, received)
					}
				}
				records := observationRecords(t, h, 1)
				if count.Load() != 1 || records[0].Attempts != 1 {
					t.Fatalf("单次执行被扩大：出站%d 记录%+v", count.Load(), records[0])
				}
				observationNoOccupancy(t, h)
			})
		}
	}
}

func TestHandlerObservationContentReleasesPrefixBeforeEOF(t *testing.T) {
	raw := observationSample(t, "completed-lf.sse")
	split := bytes.Index(raw, []byte("event: response.completed"))
	prefix, tail := raw[:split], raw[split:]
	gate := testkit.NewGate()
	defer gate.Open()
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write(prefix)
		w.(http.Flusher).Flush()
		if gate.Wait(r.Context()) == nil {
			_, _ = w.Write(tail)
		}
	}))
	defer up.Close()
	h, down := observationGateway(t, up, nil, HandlerOptions{})
	type result struct {
		resp *http.Response
		err  error
	}
	done := make(chan result, 1)
	r := observationRequest(t, down.URL, `{"model":"public-model","stream":true}`)
	go func() { resp, err := http.DefaultClient.Do(r); done <- result{resp, err} }()
	select {
	case <-gate.Reached():
	case <-time.After(3 * time.Second):
		t.Fatal("上游未写出首内容")
	}
	var resp *http.Response
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatal(got.err)
		}
		resp = got.resp
	case <-time.After(time.Second):
		t.Fatal("出现内容后仍等待EOF才提交前缀")
	}
	defer resp.Body.Close()
	received := make([]byte, len(prefix))
	if _, err := io.ReadFull(resp.Body, received); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(received, prefix) {
		t.Fatal("暂存前缀字节变化")
	}
	if len(h.Recent()) != 0 {
		t.Fatal("未结束流提前结算记录")
	}
	gate.Open()
	rest, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(rest, tail) {
		t.Fatal("终态尾部未完整复制")
	}
	record := observationRecords(t, h, 1)[0]
	if !record.Attempt.Response.ProtocolComplete || record.Attempt.Response.Output != "nonempty" {
		t.Fatalf("完整流结算错误：%+v", record)
	}
	observationNoOccupancy(t, h)
}

func TestHandlerObservationUnknownTruncatedAndLimitsPassThrough(t *testing.T) {
	cases := []struct {
		name, mode string
		change     func(*catalog.Data)
	}{{"unknown-output.json", "non_stream", nil}, {"unknown-event.sse", "stream", nil}, {"truncated.sse", "stream", nil}, {"status-conflict.sse", "stream", nil}, {"terminal-tail.sse", "stream", nil}, {"empty.json", "non_stream", func(d *catalog.Data) { d.Observability.MaxObservedJSONBytes = 32 }}, {"empty-lf.sse", "stream", func(d *catalog.Data) { d.Observability.MaxObservedSSEEventBytes = 32 }}, {"empty-lf.sse", "stream", func(d *catalog.Data) {
		observationExecution(d, func(e *catalog.Execution) { e.EmptyResponseMaxBufferBytes = 16 })
	}}}
	for i, c := range cases {
		t.Run(fmt.Sprintf("%d/%s", i, c.name), func(t *testing.T) {
			raw := observationSample(t, c.name)
			media := "application/json"
			body := `{"model":"public-model"}`
			if c.mode == "stream" {
				media = "text/event-stream"
				body = `{"model":"public-model","stream":true}`
			}
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", media)
				_, _ = w.Write(raw)
			}))
			defer up.Close()
			h, down := observationGateway(t, up, c.change, HandlerOptions{})
			resp, received, err := observationCall(t, down.URL, body)
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != 200 || !bytes.Equal(received, raw) {
				t.Fatalf("不可判空响应被改写：%d %s", resp.StatusCode, received)
			}
			record := observationRecords(t, h, 1)[0]
			if record.ErrorCode == "upstream_empty_response" || record.Attempt.Response.Output == "empty" && c.change == nil {
				t.Fatalf("未知/截断被判空：%+v", record)
			}
			observationNoOccupancy(t, h)
		})
	}
}

func TestHandlerObservationTerminalTailReadErrorPreservesBytes(t *testing.T) {
	raw := append(observationSample(t, "completed-lf.sse"), []byte(": 尾部仍需复制\n\n")...)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, buffer, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = fmt.Fprintf(buffer, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: %d\r\nConnection: close\r\n\r\n", len(raw)+10)
		_, _ = buffer.Write(raw)
		_ = buffer.Flush()
	}))
	defer up.Close()
	h, down := observationGateway(t, up, nil, HandlerOptions{})
	resp, received, err := observationCall(t, down.URL, `{"model":"public-model","stream":true}`)
	if err == nil || resp.StatusCode != 200 || !bytes.Equal(received, raw) {
		t.Fatalf("提交后尾部错误未保留原协议：状态%d 错误%v 长度%d/%d", resp.StatusCode, err, len(received), len(raw))
	}
	if bytes.Contains(received, []byte("upstream_transport_error")) {
		t.Fatal("已提交流尾追加了网关JSON")
	}
	record := observationRecords(t, h, 1)[0]
	if record.Attempt.Response.ProtocolComplete || record.Attempt.Response.EOF || record.Attempt.FailureKind == "" {
		t.Fatalf("完成标记掩盖尾部I/O错误：%+v", record)
	}
	observationNoOccupancy(t, h)
}

func TestHandlerObservationRestrictionsAffectNextRequest(t *testing.T) {
	cases := []struct {
		name        string
		status      int
		body, retry string
		permanent   bool
	}{
		{"普通429", 429, `{"error":{"type":"rate_limit_error","code":"rate_limit_exceeded"}}`, "", false},
		{"RetryAfter429", 429, `{"error":{"type":"rate_limit_error","code":"rate_limit_exceeded"}}`, "7", false},
		{"RetryAfter503", 503, `{"error":{"type":"server_error","code":"server_is_overloaded"}}`, "7", false},
		{"长期额度", 429, `{"error":{"type":"insufficient_quota","code":"insufficient_quota"}}`, "", true},
		{"额度耗尽的等待提示不证明恢复", 429, `{"error":{"type":"insufficient_quota","code":"insufficient_quota"}}`, "7", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			clock := testkit.NewClock(time.Now())
			var count atomic.Int64
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				count.Add(1)
				w.Header().Set("Content-Type", "application/json")
				if c.retry != "" {
					w.Header().Set("Retry-After", c.retry)
				}
				w.WriteHeader(c.status)
				_, _ = io.WriteString(w, c.body)
			}))
			defer up.Close()
			h, down := observationGateway(t, up, nil, HandlerOptions{Now: clock.Now})
			first, raw, err := observationCall(t, down.URL, `{"model":"public-model"}`)
			if err != nil || first.StatusCode != c.status || string(raw) != c.body {
				t.Fatalf("原上游错误未透传：%d %s %v", first.StatusCode, raw, err)
			}
			observationRecords(t, h, 1)
			second, _, err := observationCall(t, down.URL, `{"model":"public-model"}`)
			if err != nil || second.StatusCode != 503 || count.Load() != 1 {
				t.Fatalf("冷却中发生新的出站：状态%d 出站%d 错误%v", second.StatusCode, count.Load(), err)
			}
			observationRecords(t, h, 2)
			foundPermanent := false
			for _, restriction := range h.Stats().Restrictions {
				if restriction.Permanent {
					foundPermanent = true
				}
			}
			if foundPermanent != c.permanent {
				t.Fatalf("永久限制分类错误：%+v", h.Stats().Restrictions)
			}
			if c.permanent {
				clock.Advance(24 * time.Hour)
				third, _, err := observationCall(t, down.URL, `{"model":"public-model"}`)
				if err != nil || third.StatusCode != 503 || count.Load() != 1 {
					t.Fatalf("永久额度被时间自动解除：%d %d %v", third.StatusCode, count.Load(), err)
				}
				observationRecords(t, h, 3)
			}
			observationNoOccupancy(t, h)
		})
	}
}

func TestHandlerObservationOrdinaryAuthErrorsDoNotPermanentlyDisable(t *testing.T) {
	for _, status := range []int{401, 403, 404} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var count atomic.Int64
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				count.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(status)
				_, _ = io.WriteString(w, `{"error":{"type":"unknown_provider_error","code":"unverified_auth_code"}}`)
			}))
			defer up.Close()
			h, down := observationGateway(t, up, nil, HandlerOptions{})
			for i := 0; i < 2; i++ {
				resp, _, err := observationCall(t, down.URL, `{"model":"public-model"}`)
				if err != nil || resp.StatusCode != status {
					t.Fatalf("普通鉴权错误导致后续拒绝：%d %v", resp.StatusCode, err)
				}
				observationRecords(t, h, i+1)
			}
			if count.Load() != 2 {
				t.Fatalf("普通状态码永久停止凭据：%d", count.Load())
			}
			for _, restriction := range h.Stats().Restrictions {
				if restriction.Permanent {
					t.Fatalf("普通状态码产生永久限制：%+v", restriction)
				}
			}
			observationNoOccupancy(t, h)
		})
	}
}

func TestHandlerObservationErrorWaitLimitKeepsRealEOF(t *testing.T) {
	const body = `{"error":{"type":"insufficient_quota","code":"insufficient_quota"}}`
	gate := testkit.NewGate()
	defer gate.Open()
	var count atomic.Int64
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := count.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(400)
		if n == 1 {
			_, _ = io.WriteString(w, body[:9])
			w.(http.Flusher).Flush()
			if gate.Wait(r.Context()) != nil {
				return
			}
			_, _ = io.WriteString(w, body[9:])
		} else {
			_, _ = io.WriteString(w, body)
		}
	}))
	defer up.Close()
	h, down := observationGateway(t, up, func(d *catalog.Data) {
		observationExecution(d, func(e *catalog.Execution) { e.MaxErrorBodyWait = 300 * time.Millisecond })
	}, HandlerOptions{})
	resp, err := http.DefaultClient.Do(observationRequest(t, down.URL, `{"model":"public-model"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	// 先确认正文观察缓冲实际建立，再等待检查期限归还它；上游仍在 Gate 中。
	observationWait(t, "错误正文未进入观察缓冲", func() bool { return h.Stats().ResponseBytes > 32<<10 })
	observationWait(t, "错误检查到限未释放观察缓冲", func() bool { return h.Stats().ResponseBytes == 32<<10 })
	gate.Open()
	raw, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != 400 || string(raw) != body {
		t.Fatalf("观察到限破坏原响应：%d %s %v", resp.StatusCode, raw, err)
	}
	record := observationRecords(t, h, 1)[0]
	if !record.Attempt.Response.EOF || record.Attempt.Response.Error.Present || record.Attempt.Response.Reason != "error_observation_timeout" {
		t.Fatalf("观察停止伪造传输错误或继续分类：%+v", record)
	}
	for _, restriction := range h.Stats().Restrictions {
		if restriction.Permanent {
			t.Fatalf("未完成错误观察产生永久限制：%+v", restriction)
		}
	}
	second, _, err := observationCall(t, down.URL, `{"model":"public-model"}`)
	if err != nil || second.StatusCode != 400 || count.Load() != 2 {
		t.Fatalf("不完整错误观察停止后续出站：%d %d %v", second.StatusCode, count.Load(), err)
	}
	observationRecords(t, h, 2)
	observationNoOccupancy(t, h)
}
