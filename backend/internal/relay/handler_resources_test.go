package relay

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"relayleaf/internal/protocol"
)

var configuredResourceTypes = []string{"openai.file", "openai.vector_store", "openai.container", "openai.response"}

func configuredResourceBody(stream bool) []byte {
	return []byte(fmt.Sprintf(` {
 "model" : "example-assistant", "stream":%t, "previous_response_id":"resp_configured_private",
 "input":[
   {"role":"user","content":[{"type":"input_file","file_id":"file_configured_private"},{"type":"input_image","file_id":"file_configured_image"}]},
   {"type":"code_interpreter_call","container_id":"cntr_configured_history","id":"ci_link","code":"print(1)","outputs":[]},
   {"type":"computer_call_output","call_id":"computer_link","output":{"type":"computer_screenshot","file_id":"file_configured_screenshot"}}
 ],
 "tools":[{"type":"file_search","vector_store_ids":["vs_configured_private"]},{"type":"code_interpreter","container":{"type":"auto","file_ids":["file_configured_container"]}}],
 "metadata":{"business":{"model":1e1000,"model":-0,"files":{"type":"code_interpreter"},"text":"原字节🌿"}}
} `, stream))
}

type configuredResourceRequest struct {
	body                    []byte
	authorization, encoding string
	project                 string
	protoMajor              int
}

func configuredResourceUpstream(t *testing.T, http2 bool, response []byte, media string) (*httptest.Server, *http.Transport, <-chan configuredResourceRequest) {
	t.Helper()
	requests := make(chan configuredResourceRequest, 4)
	up := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("读取资源请求失败：%v", err)
		}
		requests <- configuredResourceRequest{body: body, authorization: r.Header.Get("Authorization"), encoding: r.Header.Get("Content-Encoding"), project: r.Header.Get("OpenAI-Project"), protoMajor: r.ProtoMajor}
		w.Header().Set("Content-Type", media)
		w.Header().Set("X-Upstream-Resource-Id", "resource-response-header")
		if _, err := w.Write(response); err != nil {
			t.Errorf("写入资源响应失败：%v", err)
		}
	}))
	up.EnableHTTP2 = http2
	up.StartTLS()
	t.Cleanup(up.Close)
	return up, up.Client().Transport.(*http.Transport), requests
}

func configuredResourceReceived(t *testing.T, requests <-chan configuredResourceRequest) configuredResourceRequest {
	t.Helper()
	select {
	case request := <-requests:
		return request
	case <-time.After(3 * time.Second):
		t.Fatal("未收到预期的真实资源请求")
		return configuredResourceRequest{}
	}
}

func configuredResourceRecords(t *testing.T, handler *Handler, attempts int, credential string) {
	t.Helper()
	contractRecords(t, handler, 1,
		"resp_configured_private", "file_configured_private", "file_configured_image", "cntr_configured_history", "file_configured_screenshot", "file_configured_container", "vs_configured_private", "opaque_configured_private",
		"resp_synthetic_completed", "resp_synthetic_empty", "msg_synthetic",
		routingToken("general-client"), routingToken("coding-client"), routingUpstreamToken("general-key"), routingUpstreamToken("coding-key"), routingUpstreamToken("internal-key"), routingUpstreamToken("backup-key"))
	record := handler.Recent()[0]
	if record.Attempts != attempts || handler.Stats().Attempts != uint64(attempts) {
		t.Fatalf("资源请求真实尝试数不符：记录=%+v，统计=%+v", record, handler.Stats())
	}
	if attempts == 0 {
		if record.Attempt != nil {
			t.Fatal("资源准入拒绝产生了尝试记录")
		}
		return
	}
	if record.Attempt == nil || record.Attempt.ProviderID != "primary" || record.Attempt.TargetID != "primary/model/responses" || record.Attempt.CredentialID != credential || record.Attempt.UpstreamModel != "primary-model" {
		t.Fatalf("资源引用离开获准作用域或凭据：%+v", record.Attempt)
	}
}

func TestConfiguredResourceScopeIntersectsAccessGroups(t *testing.T) {
	response := observationSample(t, "completed.json")
	for _, tc := range []struct {
		name, clientID, credential, code string
		shared, disableOwner             bool
		status                           int
	}{
		{name: "私有作用域所有者", clientID: "general-client", credential: "general-key", status: 200},
		{name: "私有作用域与编程组无交集", clientID: "coding-client", status: 503, code: "no_available_target"},
		{name: "共享作用域普通组", shared: true, clientID: "general-client", credential: "general-key", status: 200},
		{name: "共享作用域编程组", shared: true, clientID: "coding-client", credential: "coding-key", status: 200},
		{name: "空组授权优先拒绝", shared: true, clientID: "blocked-client", status: 403, code: "upstream_forbidden"},
		{name: "路由授权优先拒绝", shared: true, clientID: "route-blocked-client", status: 403, code: "route_forbidden"},
		{name: "私有所有者停用不回落其它凭据", disableOwner: true, clientID: "general-client", status: 503, code: "no_available_target"},
		{name: "共享组求交不回落未获准组", shared: true, disableOwner: true, clientID: "general-client", status: 503, code: "no_available_target"},
		{name: "共享作用域另一获准组可用", shared: true, disableOwner: true, clientID: "coding-client", credential: "coding-key", status: 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			up, transport, requests := configuredResourceUpstream(t, false, response, "application/json")
			var backupCalls atomic.Int32
			backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { backupCalls.Add(1); w.WriteHeader(500) }))
			t.Cleanup(backup.Close)
			doc := routingConfig(t, up.URL, backup.URL)
			routingFixedScope(t, doc, tc.shared, configuredResourceTypes...)
			if tc.disableOwner {
				routingConfigMap(t, doc, "providers", "primary", "credentials", "general-key")["enabled"] = false
			}
			handler, down, client := routingGateway(t, doc, HandlerOptions{BaseTransport: transport})
			raw := configuredResourceBody(false)
			resp, received := contractSend(t, client, routingRequest(t, down.URL, tc.clientID, raw))
			if resp.StatusCode != tc.status || resp.Header.Get("X-RelayLeaf-Error-Code") != tc.code {
				t.Fatalf("固定作用域与分组求交状态错误：%d/%s，预期%d/%s，正文=%s", resp.StatusCode, resp.Header.Get("X-RelayLeaf-Error-Code"), tc.status, tc.code, received)
			}
			attempts := 0
			if tc.status == 200 {
				attempts = 1
				request := configuredResourceReceived(t, requests)
				want := []byte(strings.Replace(string(raw), `"example-assistant"`, `"primary-model"`, 1))
				if !bytes.Equal(request.body, want) || request.authorization != "Bearer "+routingUpstreamToken(tc.credential) || request.project != "project-primary" || !bytes.Equal(received, response) {
					t.Fatal("资源正文或获准上游认证未保持预期")
				}
			}
			if len(requests) != 0 || backupCalls.Load() != 0 {
				t.Fatal("资源请求产生额外尝试或回落到其他作用域")
			}
			configuredResourceRecords(t, handler, attempts, tc.credential)
		})
	}
}

func TestConfiguredResourceScopeRejectsMissingTypesAndStatelessRoutes(t *testing.T) {
	for _, tc := range []struct {
		name, code, input string
		fixed             bool
		resourceTypes     []string
	}{
		{name: "无状态路由不能推断归属", code: "resource_owner_unknown"},
		{name: "只覆盖文件仍缺容器和向量库", fixed: true, resourceTypes: []string{"openai.file"}, code: "resource_scope_conflict"},
		{name: "联合引用缺历史容器覆盖", fixed: true, resourceTypes: []string{"openai.file", "openai.vector_store", "openai.response"}, code: "resource_scope_conflict"},
		{name: "无状态路由单独历史容器", input: `[{"type":"code_interpreter_call","container_id":"cntr_configured_history"}]`, code: "resource_owner_unknown"},
		{name: "无状态路由单独截图文件", input: `[{"type":"computer_call_output","output":{"type":"computer_screenshot","file_id":"file_configured_screenshot"}}]`, code: "resource_owner_unknown"},
		{name: "历史容器要求独立容器覆盖", fixed: true, resourceTypes: []string{"openai.file"}, input: `[{"type":"code_interpreter_call","container_id":"cntr_configured_history"}]`, code: "resource_scope_conflict"},
		{name: "截图文件要求独立文件覆盖", fixed: true, resourceTypes: []string{"openai.container"}, input: `[{"type":"computer_call_output","output":{"type":"computer_screenshot","file_id":"file_configured_screenshot"}}]`, code: "resource_scope_conflict"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { calls.Add(1); w.WriteHeader(500) }))
			t.Cleanup(up.Close)
			doc := routingConfig(t, up.URL, up.URL)
			if tc.fixed {
				routingFixedScope(t, doc, false, tc.resourceTypes...)
			}
			handler, down, client := routingGateway(t, doc, HandlerOptions{})
			raw := configuredResourceBody(false)
			if tc.input != "" {
				raw = []byte(`{"model":"example-assistant","input":` + tc.input + `}`)
			}
			resp, body := contractSend(t, client, routingRequest(t, down.URL, "general-client", raw))
			if resp.StatusCode != 409 || resp.Header.Get("X-RelayLeaf-Error-Code") != tc.code || calls.Load() != 0 {
				t.Fatalf("硬资源出站前拒绝错误：%d/%s，调用数%d，正文=%s", resp.StatusCode, resp.Header.Get("X-RelayLeaf-Error-Code"), calls.Load(), body)
			}
			configuredResourceRecords(t, handler, 0, "")
		})
	}
}

func TestConfiguredOpaqueRequiresOneStaticUpstreamModel(t *testing.T) {
	for _, tc := range []struct {
		name                string
		sameModel, disabled bool
		status              int
	}{
		{name: "多模型冲突", status: 409},
		{name: "冲突目标停用仍拒绝", disabled: true, status: 409},
		{name: "同模型多目标", sameModel: true, status: 200},
		{name: "同模型停用目标", sameModel: true, disabled: true, status: 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response := observationSample(t, "completed.json")
			up, transport, requests := configuredResourceUpstream(t, false, response, "application/json")
			doc := routingConfig(t, up.URL, up.URL)
			routingFixedScope(t, doc, false, "openai.opaque")
			model := routingConfigClone(t, routingConfigMap(t, doc, "providers", "primary", "models", "model"))
			model["upstream_name"] = "different-model"
			if tc.sameModel {
				model["upstream_name"] = "primary-model"
			}
			routingConfigMap(t, model, "bindings", "responses")["enabled"] = !tc.disabled
			routingConfigMap(t, doc, "providers", "primary", "models")["second"] = model
			route := routingConfigMap(t, doc, "routes", "example-responses")
			route["targets"] = []any{map[string]any{"target": "primary/model/responses", "priority": 10, "weight": 1}, map[string]any{"target": "primary/second/responses", "priority": 20, "weight": 1}}
			handler, down, client := routingGateway(t, doc, HandlerOptions{BaseTransport: transport})
			raw := []byte(`{"model":"example-assistant","input":[{"type":"reasoning","encrypted_content":"opaque_configured_private"}]}`)
			resp, body := contractSend(t, client, routingRequest(t, down.URL, "general-client", raw))
			attempts := 0
			if resp.StatusCode != tc.status {
				t.Fatalf("opaque 静态模型约束状态错误：%d，预期%d，正文=%s", resp.StatusCode, tc.status, body)
			}
			if tc.status == 409 {
				if resp.Header.Get("X-RelayLeaf-Error-Code") != "resource_scope_conflict" || len(requests) != 0 {
					t.Fatal("opaque 多模型冲突未在真实尝试前拒绝")
				}
			} else {
				attempts = 1
				request := configuredResourceReceived(t, requests)
				if !bytes.Equal(request.body, []byte(strings.Replace(string(raw), `"example-assistant"`, `"primary-model"`, 1))) || !bytes.Equal(body, response) {
					t.Fatal("opaque 同模型请求或响应未保真")
				}
			}
			configuredResourceRecords(t, handler, attempts, "general-key")
		})
	}
}

func TestConfiguredResourcePreservesJSONGzipAndSSEOnRealConnections(t *testing.T) {
	for _, http2 := range []bool{false, true} {
		for _, compressed := range []bool{false, true} {
			for _, stream := range []bool{false, true} {
				t.Run(fmt.Sprintf("HTTP%d/gzip=%t/stream=%t", map[bool]int{false: 1, true: 2}[http2], compressed, stream), func(t *testing.T) {
					media, sample := "application/json", "completed.json"
					if stream {
						media, sample = "text/event-stream", "completed-lf.sse"
					}
					response := observationSample(t, sample)
					up, transport, requests := configuredResourceUpstream(t, http2, response, media)
					doc := routingConfig(t, up.URL, up.URL)
					routingFixedScope(t, doc, true, configuredResourceTypes...)
					handler, down, client := routingGateway(t, doc, HandlerOptions{BaseTransport: transport})
					raw := configuredResourceBody(stream)
					incoming := raw
					if compressed {
						incoming = contractGzip(t, raw)
					}
					req := routingRequest(t, down.URL, "coding-client", incoming)
					if compressed {
						req.Header.Set("Content-Encoding", "gzip")
					}
					resp, received := contractSend(t, client, req)
					request := configuredResourceReceived(t, requests)
					wantProtocol := 1
					if http2 {
						wantProtocol = 2
					}
					want := []byte(strings.Replace(string(raw), `"example-assistant"`, `"primary-model"`, 1))
					if resp.StatusCode != 200 || !bytes.Equal(received, response) || !bytes.Equal(request.body, want) || request.encoding != "" || request.protoMajor != wantProtocol || request.project != "project-primary" || request.authorization != "Bearer "+routingUpstreamToken("coding-key") {
						t.Fatalf("真实连接上的资源字节、模型映射或认证错误：状态%d，上游协议%d，编码%q", resp.StatusCode, request.protoMajor, request.encoding)
					}
					configuredResourceRecords(t, handler, 1, "coding-key")
					if handler.Recent()[0].Attempt.Response.Replay != protocol.ReplayForbidden {
						t.Fatal("服务器资源请求的观察记录授予了重放资格")
					}
				})
			}
		}
	}
}

func TestConfiguredServerToolEmptyResponsePassesThrough(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprintf("stream=%t", stream), func(t *testing.T) {
			media, sample := "application/json", "empty.json"
			if stream {
				media, sample = "text/event-stream", "empty-lf.sse"
			}
			response := observationSample(t, sample)
			up, transport, requests := configuredResourceUpstream(t, true, response, media)
			doc := routingConfig(t, up.URL, up.URL)
			routingFixedScope(t, doc, false, configuredResourceTypes...)
			handler, down, client := routingGateway(t, doc, HandlerOptions{BaseTransport: transport})
			resp, body := contractSend(t, client, routingRequest(t, down.URL, "general-client", configuredResourceBody(stream)))
			if resp.StatusCode != 200 || !bytes.Equal(body, response) || resp.Header.Get("X-RelayLeaf-Error-Code") != "" || resp.Header.Get("X-Upstream-Resource-Id") != "resource-response-header" {
				t.Fatalf("服务器工具空回未原样透传：状态%d，错误码%s，正文%s", resp.StatusCode, resp.Header.Get("X-RelayLeaf-Error-Code"), body)
			}
			configuredResourceReceived(t, requests)
			configuredResourceRecords(t, handler, 1, "general-key")
			facts := handler.Recent()[0].Attempt.Response
			if facts.Replay != protocol.ReplayForbidden || facts.Output != "empty" || len(requests) != 0 {
				t.Fatalf("服务器工具空回观察或单次执行约束错误：%+v", facts)
			}
		})
	}
}
