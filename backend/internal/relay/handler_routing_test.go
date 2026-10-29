package relay

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"relayleaf/internal/testkit"
)

func TestConfiguredGroupsKeepClientPoolsAndTargetPriority(t *testing.T) {
	type sentRequest struct{ provider, token, project, body string }
	sent := make(chan sentRequest, 32)
	completed := observationSample(t, "completed.json")
	upstream := func(id string) *httptest.Server {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			raw, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("读取测试上游请求失败：%v", err)
			}
			sent <- sentRequest{id, r.Header.Get("Authorization"), r.Header.Get("OpenAI-Project"), string(raw)}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(completed)
		}))
		t.Cleanup(server.Close)
		return server
	}
	primary, backup := upstream("primary"), upstream("backup")
	document := routingConfig(t, primary.URL, backup.URL)
	credentials := routingConfigMap(t, document, "providers", "primary", "credentials")
	extra := routingConfigClone(t, routingConfigMap(t, credentials, "general-key"))
	extra["secret"] = routingUpstreamToken("extra-general")
	credentials["extra-general"] = extra
	routingConfigMap(t, document, "providers", "primary", "models", "model", "bindings", "responses")["credentials"] = []string{"general-key", "coding-key", "internal-key", "extra-general"}
	handler, gateway, client := routingGateway(t, document, HandlerOptions{})
	for i, clientID := range []string{"general-client", "general-client", "general-client", "general-client", "coding-client", "coding-client"} {
		req := routingRequest(t, gateway.URL, clientID, []byte(`{"model":"example-assistant","input":"private-routing-input"}`))
		req.Header.Set("OpenAI-Project", "untrusted-project")
		resp, raw := contractSend(t, client, req)
		if resp.StatusCode != 200 {
			t.Fatalf("配置授权调用失败：%d %s", resp.StatusCode, raw)
		}
		got := <-sent
		wantKey := "coding-key"
		if clientID == "general-client" {
			wantKey = "general-key"
			if i%2 == 1 {
				wantKey = "extra-general"
			}
		}
		if got.provider != "primary" || got.token != "Bearer "+routingUpstreamToken(wantKey) || got.project != "project-primary" || !strings.Contains(got.body, `"model":"primary-model"`) {
			t.Fatalf("组权限、凭据轮转或出站身份错误：%+v", got)
		}
	}
	contractRecords(t, handler, 6, "private-routing-input", routingToken("general-client"), routingUpstreamToken("general-key"), routingUpstreamToken("internal-key"))
	if handler.Stats().Attempts != 6 {
		t.Fatalf("单次调用的计数不符：%+v", handler.Stats())
	}
	for _, record := range handler.Recent() {
		if record.Attempts != 1 || record.Attempt == nil || record.Attempt.ProviderID != "primary" {
			t.Fatalf("实际尝试记录未对应最终候选：%+v", record)
		}
	}
}

func TestConfiguredRejectionsKeepAuthorizationAndAvailabilitySeparate(t *testing.T) {
	var outgoing atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		outgoing.Add(1)
		w.WriteHeader(500)
	}))
	t.Cleanup(up.Close)
	for _, tc := range []struct {
		name, clientID, body string
		change               func(*testing.T, map[string]any)
		status               int
		code                 string
	}{
		{"组权限先于硬引用", "blocked-client", `{"model":"example-assistant","previous_response_id":"resp_private"}`, nil, 403, "upstream_forbidden"},
		{"路由权限先于组", "route-blocked-client", `{"model":"example-assistant","previous_response_id":"resp_private"}`, nil, 403, "route_forbidden"},
		{"路由停用", "general-client", `{"model":"example-assistant"}`, func(t *testing.T, d map[string]any) {
			routingConfigMap(t, d, "routes", "example-responses")["enabled"] = false
		}, 503, "no_available_target"},
		{"路由停用仍校验组", "blocked-client", `{"model":"example-assistant"}`, func(t *testing.T, d map[string]any) {
			routingConfigMap(t, d, "routes", "example-responses")["enabled"] = false
		}, 403, "upstream_forbidden"},
		{"提供方全部停用", "general-client", `{"model":"example-assistant"}`, func(t *testing.T, d map[string]any) {
			for _, id := range []string{"primary", "backup"} {
				routingConfigMap(t, d, "providers", id)["enabled"] = false
			}
		}, 503, "no_available_target"},
		{"绑定全部停用", "general-client", `{"model":"example-assistant"}`, func(t *testing.T, d map[string]any) {
			for _, id := range []string{"primary", "backup"} {
				routingConfigMap(t, d, "providers", id, "models", "model", "bindings", "responses")["enabled"] = false
			}
		}, 503, "no_available_target"},
		{"凭据全部停用", "general-client", `{"model":"example-assistant"}`, func(t *testing.T, d map[string]any) {
			for _, id := range []string{"primary", "backup"} {
				for _, credential := range routingConfigMap(t, d, "providers", id, "credentials") {
					credential.(map[string]any)["enabled"] = false
				}
			}
		}, 503, "no_available_target"},
		{"禁用凭据不成为失权", "coding-client", `{"model":"example-assistant"}`, func(t *testing.T, d map[string]any) {
			routingConfigMap(t, d, "providers", "primary", "credentials", "coding-key")["enabled"] = false
		}, 503, "no_available_target"},
		{"未获准目标不提供模式", "coding-client", `{"model":"example-assistant","stream":true}`, func(t *testing.T, d map[string]any) {
			routingConfigMap(t, d, "providers", "primary", "models", "model", "bindings", "responses")["request_modes"] = []string{"non_stream"}
		}, 400, "unsupported_request_mode"},
		{"停用不隐藏模式错误", "coding-client", `{"model":"example-assistant","stream":true}`, func(t *testing.T, d map[string]any) {
			routingConfigMap(t, d, "providers", "primary", "credentials", "coding-key")["enabled"] = false
			routingConfigMap(t, d, "providers", "primary", "models", "model", "bindings", "responses")["request_modes"] = []string{"non_stream"}
		}, 400, "unsupported_request_mode"},
		{"未获准目标不提供能力", "coding-client", `{"model":"example-assistant","input":[{"type":"message","content":[{"type":"input_file","file_data":"data:text/plain;base64,eA=="}]}]}`, func(t *testing.T, d map[string]any) {
			routingConfigMap(t, d, "providers", "primary", "models", "model", "bindings", "responses")["capabilities"] = []string{"image_input", "tool_calling"}
		}, 400, "unsupported_capability"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			document := routingConfig(t, up.URL, up.URL)
			if tc.change != nil {
				tc.change(t, document)
			}
			handler, gateway, client := routingGateway(t, document, HandlerOptions{})
			resp, raw := contractSend(t, client, routingRequest(t, gateway.URL, tc.clientID, []byte(tc.body)))
			if resp.StatusCode != tc.status || resp.Header.Get("X-RelayLeaf-Error-Code") != tc.code {
				t.Fatalf("拒绝分类错误：%d/%s %s", resp.StatusCode, resp.Header.Get("X-RelayLeaf-Error-Code"), raw)
			}
			for _, forbidden := range []string{"general-key", "internal-key", "project-primary", "resp_private"} {
				if strings.Contains(string(raw), forbidden) {
					t.Fatalf("普通错误暴露了上游信息：%s", raw)
				}
			}
			contractRecords(t, handler, 1, routingToken(tc.clientID), "resp_private")
			if outgoing.Load() != 0 || handler.Stats().Attempts != 0 || handler.Recent()[0].Attempt != nil {
				t.Fatal("配置拒绝产生了真实上游尝试")
			}
		})
	}
}

func TestConfiguredCooldownSelectsOnlyAvailableAuthorizedTargets(t *testing.T) {
	for _, failureStatus := range []int{429, 503} {
		t.Run(fmt.Sprint(failureStatus), func(t *testing.T) {
			var primaryCalls, backupCalls, generalCalls atomic.Int32
			completed := observationSample(t, "completed.json")
			primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				primaryCalls.Add(1)
				if r.Header.Get("Authorization") == "Bearer "+routingUpstreamToken("general-key") && generalCalls.Add(1) == 1 {
					w.Header().Set("Retry-After", "60")
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(failureStatus)
					_, _ = io.WriteString(w, `{"error":{"code":"rate_limit_exceeded","type":"rate_limit_error"}}`)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write(completed)
			}))
			backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				backupCalls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write(completed)
			}))
			t.Cleanup(primary.Close)
			t.Cleanup(backup.Close)
			clock := testkit.NewClock(time.Now())
			handler, gateway, client := routingGateway(t, routingConfig(t, primary.URL, backup.URL), HandlerOptions{Now: clock.Now, NewTimer: func(d time.Duration) Timer { return clock.NewTimer(d) }})
			body := []byte(`{"model":"example-assistant","input":"cooldown"}`)
			resp, raw := contractSend(t, client, routingRequest(t, gateway.URL, "general-client", body))
			if resp.StatusCode != failureStatus || backupCalls.Load() != 0 {
				t.Fatalf("真实失败被隐藏重试：%d %s", resp.StatusCode, raw)
			}
			resp, _ = contractSend(t, client, routingRequest(t, gateway.URL, "general-client", body))
			if resp.StatusCode != 200 || primaryCalls.Load() != 1 || backupCalls.Load() != 1 {
				t.Fatal("主级冷却时没有选择获准备用")
			}
			resp, raw = contractSend(t, client, routingRequest(t, gateway.URL, "coding-client", body))
			wantCodingStatus, wantAttempts := 200, uint64(4)
			if failureStatus == 503 {
				wantCodingStatus, wantAttempts = 503, 3
			}
			if resp.StatusCode != wantCodingStatus || backupCalls.Load() != 1 {
				t.Fatalf("限制范围或备用授权错误：%d %s", resp.StatusCode, raw)
			}
			clock.Advance(time.Minute)
			resp, raw = contractSend(t, client, routingRequest(t, gateway.URL, "general-client", body))
			if resp.StatusCode != 200 || backupCalls.Load() != 1 {
				t.Fatalf("到期恢复没有回到合法主级：%d %s", resp.StatusCode, raw)
			}
			contractRecords(t, handler, 4, routingUpstreamToken("general-key"), routingToken("coding-client"))
			if stats := handler.Stats(); stats.Attempts != wantAttempts || len(stats.Restrictions) != 0 {
				t.Fatalf("真实计数或恢复结算不符：%+v", stats)
			}
		})
	}
}

func TestConfiguredSharedQuotaCannotBeBypassedByCredentialOrAlias(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	var calls atomic.Int32
	completed := observationSample(t, "completed.json")
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			close(entered)
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(completed)
	}))
	t.Cleanup(up.Close)
	document := routingConfig(t, up.URL, up.URL)
	routingConfigMap(t, document, "providers", "backup")["enabled"] = false
	primary := routingConfigMap(t, document, "providers", "primary")
	primary["quota_scopes"] = map[string]any{"project": map[string]any{"owner": map[string]any{"project": "project-primary"}, "limits": map[string]any{"max_inflight": 1}}}
	for _, credential := range routingConfigMap(t, primary, "credentials") {
		credential.(map[string]any)["quota_scopes"] = []string{"project"}
	}
	routingConfigMap(t, primary, "models", "model", "bindings", "responses")["quota_scopes"] = []string{"project"}
	handler, gateway, client := routingGateway(t, document, HandlerOptions{})
	firstRequest := routingRequest(t, gateway.URL, "general-client", []byte(`{"model":"example-assistant","input":"held"}`))
	type result struct {
		status int
		err    error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := client.Do(firstRequest)
		if err != nil {
			done <- result{err: err}
			return
		}
		_, readErr := io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		done <- result{status: resp.StatusCode, err: readErr}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("首个上游请求未进入受控等待")
	}
	resp, raw := contractSend(t, client, routingRequest(t, gateway.URL, "coding-client", []byte(`{"model":"example-alias","input":"competing"}`)))
	if resp.StatusCode != 503 || resp.Header.Get("X-RelayLeaf-Error-Code") != "no_available_target" {
		t.Fatalf("共享额度未限制另一 Key/Route：%d %s", resp.StatusCode, raw)
	}
	observationRecords(t, handler, 1)
	if stats := handler.Stats(); calls.Load() != 1 || stats.Attempts != 1 || stats.Inbound != 1 || stats.Targets["primary/model/responses"] != 1 || stats.Quotas["primary/project"] != 1 {
		t.Fatalf("配额交集计数或部分占用不符：%+v", stats)
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case result := <-done:
		if result.err != nil || result.status != 200 {
			t.Fatalf("首个请求未正常收尾：%+v", result)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("首个请求未释放连接")
	}
	contractRecords(t, handler, 2, routingUpstreamToken("coding-key"))
}

func TestConfiguredPrecommitExhaustionReturnsWithoutOutbound(t *testing.T) {
	var calls atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { calls.Add(1); w.WriteHeader(500) }))
	t.Cleanup(up.Close)
	handler, gateway, client := routingGateway(t, routingConfig(t, up.URL, up.URL), HandlerOptions{})
	if err := handler.transport.Close(); err != nil {
		t.Fatal(err)
	}
	resp, raw := contractSend(t, client, routingRequest(t, gateway.URL, "general-client", []byte(`{"model":"example-assistant"}`)))
	if resp.StatusCode != 503 || resp.Header.Get("X-RelayLeaf-Error-Code") != "no_available_target" {
		t.Fatalf("提交前候选耗尽未返回明确错误：%d %s", resp.StatusCode, raw)
	}
	contractRecords(t, handler, 1)
	if calls.Load() != 0 || handler.Stats().Attempts != 0 || handler.Recent()[0].Attempt != nil {
		t.Fatal("提交失败产生了上游请求或尝试记录")
	}
}
