package relay

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"relayleaf/internal/catalog"
	"relayleaf/internal/config"
)

const contractClientSecret = "contract-client-secret-0123456789abcdef"
const contractUpstreamSecret = "contract-upstream-secret-a"

func contractSnapshot(t *testing.T, origin string, modify func(*catalog.Data)) *catalog.Snapshot {
	t.Helper()
	path := filepath.Join(t.TempDir(), "relayleaf.yaml")
	if err := os.WriteFile(path, []byte(strings.ReplaceAll(string(config.Example()), "https://api.example.com", origin)), 0o600); err != nil {
		t.Fatal(err)
	}
	snapshot, err := config.LoadFile(context.Background(), path, config.Options{ResolveSecrets: true, LookupEnv: func(name string) (string, bool) {
		switch name {
		case "RELAYLEAF_CLIENT_KEY":
			return contractClientSecret, true
		case "RELAYLEAF_UPSTREAM_KEY":
			return contractUpstreamSecret, true
		}
		return "", false
	}})
	if err != nil {
		t.Fatal(err)
	}
	d := catalog.Data{Gateway: snapshot.GatewayAddress(), Providers: snapshot.Providers(), Targets: snapshot.Targets(), Routes: snapshot.Routes(), ClientKeys: snapshot.ClientKeys(), Verifications: []catalog.Verification{{ClientID: "local-client", Material: catalog.NewSecret(contractClientSecret)}}, Limits: snapshot.Limits(), Execution: snapshot.Execution(), Health: snapshot.Health(), Observability: snapshot.Observability(), SecretsResolved: true}
	if modify != nil {
		modify(&d)
	}
	return catalog.NewSnapshot(d)
}

func contractGateway(t *testing.T, snapshot *catalog.Snapshot) (*Handler, string, *http.Client) {
	t.Helper()
	handler, err := NewHandler(snapshot, HandlerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	t.Cleanup(func() {
		if err := handler.Close(); err != nil {
			t.Errorf("关闭契约验收处理器失败：%v", err)
		}
	})
	transport := &http.Transport{DisableCompression: true, Proxy: nil}
	t.Cleanup(transport.CloseIdleConnections)
	return handler, server.URL, &http.Client{Transport: transport, Timeout: 5 * time.Second}
}

func contractSend(t *testing.T, client *http.Client, req *http.Request) (*http.Response, []byte) {
	t.Helper()
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(resp.Body)
	closeErr := resp.Body.Close()
	if readErr != nil || closeErr != nil {
		t.Fatalf("读取契约验收响应失败：%v %v", readErr, closeErr)
	}
	return resp, body
}

func contractGzip(t *testing.T, body []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)
	if _, err := writer.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func contractRecords(t *testing.T, handler *Handler, count int, forbidden ...string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for len(handler.Recent()) < count && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	records := handler.Recent()
	if len(records) != count {
		t.Fatalf("近期记录数=%d，预期 %d", len(records), count)
	}
	encoded, err := json.Marshal(records)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range forbidden {
		if bytes.Contains(encoded, []byte(secret)) {
			t.Fatal("内存记录包含秘密或正文")
		}
	}
	stats := handler.Stats()
	if stats.Inbound != 0 || stats.ActiveAttempts != 0 || stats.BodyBytes != 0 || stats.ResponseBytes != 0 {
		t.Fatalf("请求结束后仍有运行占用：%+v", stats)
	}
}

func TestHandlerContractRejectsBeforeAnyUpstreamAttempt(t *testing.T) {
	var sent atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { sent.Add(1); w.WriteHeader(500) }))
	t.Cleanup(upstream.Close)
	denied := strings.Repeat("d", 32)
	snapshot := contractSnapshot(t, upstream.URL, func(d *catalog.Data) {
		d.ClientKeys["denied"] = catalog.ClientKey{ID: "denied", Enabled: true, Limits: catalog.LocalLimits{MaxInflight: 4}}
		d.Verifications = append(d.Verifications, catalog.Verification{ClientID: "denied", Material: catalog.NewSecret(denied)})
	})
	handler, gateway, client := contractGateway(t, snapshot)
	for _, tc := range []struct {
		name, method, path, body, token, contentType, encoding string
		extraAuth                                              bool
		status                                                 int
		code                                                   string
	}{
		{name: "缺少认证", method: "POST", path: "/v1/responses", body: "[", status: 401, code: "invalid_client_key"},
		{name: "重复载体优先", method: "POST", path: "/v1/responses", body: "[", token: "Basic invalid", extraAuth: true, status: 400, code: "ambiguous_client_auth"},
		{name: "不支持的载体", method: "POST", path: "/v1/responses?%6bey=client", body: "[", status: 400, code: "invalid_client_auth"},
		{name: "方法", method: "GET", path: "/v1/responses", status: 405, code: "method_not_allowed"},
		{name: "转义路径", method: "POST", path: "/v1/%72esponses", status: 404, code: "route_not_found"},
		{name: "尾斜线", method: "POST", path: "/v1/responses/", status: 404, code: "route_not_found"},
		{name: "错误格式", method: "POST", path: "/v1/responses", body: `{"model":"example-assistant"}`, token: "Bearer " + contractClientSecret, contentType: "text/plain", status: 415, code: "unsupported_request_format"},
		{name: "无效gzip", method: "POST", path: "/v1/responses", body: "not-gzip", token: "Bearer " + contractClientSecret, contentType: "application/json", encoding: "gzip", status: 415, code: "unsupported_request_format"},
		{name: "重复模型", method: "POST", path: "/v1/responses", body: `{"model":"example-assistant","model":"other","input":"private-contract-payload"}`, token: "Bearer " + contractClientSecret, contentType: "application/json", status: 400, code: "ambiguous_request_metadata"},
		{name: "后台模式", method: "POST", path: "/v1/responses", body: `{"model":"example-assistant","input":"private-contract-payload","background":true}`, token: "Bearer " + contractClientSecret, contentType: "application/json", status: 400, code: "unsupported_request_mode"},
		{name: "硬资源引用", method: "POST", path: "/v1/responses", body: `{"model":"example-assistant","input":"private-contract-payload","previous_response_id":"resp_private"}`, token: "Bearer " + contractClientSecret, contentType: "application/json", status: 409, code: "resource_owner_unknown"},
		{name: "精确公开模型", method: "POST", path: "/v1/responses", body: `{"model":"Example-assistant","input":"private-contract-payload"}`, token: "Bearer " + contractClientSecret, contentType: "application/json", status: 404, code: "route_not_found"},
		{name: "授权", method: "POST", path: "/v1/responses", body: `{"model":"example-assistant","input":"private-contract-payload"}`, token: "Bearer " + denied, contentType: "application/json", status: 403, code: "route_forbidden"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(tc.method, gateway+tc.path, strings.NewReader(tc.body))
			if err != nil {
				t.Fatal(err)
			}
			if tc.token != "" {
				req.Header.Set("Authorization", tc.token)
			}
			if tc.extraAuth {
				req.Header.Set("X-Api-Key", "invalid")
			}
			if tc.contentType != "" {
				req.Header.Set("Content-Type", tc.contentType)
			}
			if tc.encoding != "" {
				req.Header.Set("Content-Encoding", tc.encoding)
			}
			resp, body := contractSend(t, client, req)
			if resp.StatusCode != tc.status || resp.Header.Get("X-RelayLeaf-Error-Code") != tc.code {
				t.Fatalf("入口拒绝状态=%d 错误码=%q，预期 %d/%s：%s", resp.StatusCode, resp.Header.Get("X-RelayLeaf-Error-Code"), tc.status, tc.code, body)
			}
			if tc.status == 401 && !strings.HasPrefix(resp.Header.Get("WWW-Authenticate"), "Bearer ") {
				t.Fatal("401 未提供 Bearer challenge")
			}
			if tc.status == 405 && resp.Header.Get("Allow") != "POST" {
				t.Fatal("405 未说明允许的方法")
			}
			if sent.Load() != 0 || handler.Stats().Attempts != 0 {
				t.Fatal("入口拒绝产生了上游真实尝试")
			}
		})
	}
	contractRecords(t, handler, 13, contractClientSecret, contractUpstreamSecret, "private-contract-payload", "resp_private", denied)
}

func TestHandlerContractPreservesRequestAndCompressedResponseBytes(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		compressed, sameModel bool
	}{{"JSON局部模型替换", false, false}, {"gzip局部模型替换", true, false}, {"gzip模型不变", true, true}} {
		t.Run(tc.name, func(t *testing.T) {
			decoded := []byte("{\n \"model\":\"example-assistant\", \"input\":\"private-contract-input\", \"extension\":{\"model\":\"preserve-model\",\"n\":9007199254740993}, \"stream\":false\n}")
			raw := decoded
			if tc.compressed {
				raw = contractGzip(t, decoded)
			}
			upstreamName := "example-upstream-model"
			if tc.sameModel {
				upstreamName = "example-assistant"
			}
			wantRequest := []byte(strings.Replace(string(decoded), `"example-assistant"`, `"`+upstreamName+`"`, 1))
			wantEncoding := ""
			if tc.compressed && tc.sameModel {
				wantRequest, wantEncoding = raw, "gzip"
			}
			response := contractGzip(t, []byte(`{"id":"resp_contract","object":"response","status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"private-contract-response"}]}],"usage":{"input_tokens":2,"output_tokens":3,"total_tokens":5}}`))
			var sent atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				sent.Add(1)
				body, err := io.ReadAll(req.Body)
				if err != nil || !bytes.Equal(body, wantRequest) || req.Header.Get("Content-Encoding") != wantEncoding || req.ContentLength != int64(len(wantRequest)) {
					t.Error("真实出站正文、编码或长度不符合局部改写规则")
				}
				if req.Header.Get("Authorization") != "Bearer "+contractUpstreamSecret || req.URL.RawQuery != "ordinary=%2f&ordinary=+&flag" {
					t.Error("上游认证或查询片段未保持预期")
				}
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Content-Encoding", "gzip")
				if _, err := w.Write(response); err != nil {
					t.Errorf("写入压缩响应失败：%v", err)
				}
			}))
			t.Cleanup(upstream.Close)
			snapshot := contractSnapshot(t, upstream.URL, func(d *catalog.Data) {
				if tc.sameModel {
					target := d.Targets["example-provider/example-model/responses"]
					target.UpstreamName = upstreamName
					d.Targets[target.ID] = target
				}
			})
			handler, gateway, client := contractGateway(t, snapshot)
			req, err := http.NewRequest("POST", gateway+"/v1/responses?ordinary=%2f&ordinary=+&flag", bytes.NewReader(raw))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", "Bearer "+contractClientSecret)
			req.Header.Set("Content-Type", "application/json; charset=utf-8")
			if tc.compressed {
				req.Header.Set("Content-Encoding", "gzip")
			}
			resp, body := contractSend(t, client, req)
			if resp.StatusCode != 200 || resp.Header.Get("Content-Encoding") != "gzip" || !bytes.Equal(body, response) {
				t.Fatal("最终交付未保留上游压缩响应原字节")
			}
			if sent.Load() != 1 || handler.Stats().Attempts != 1 {
				t.Fatal("单个请求产生了重复上游尝试")
			}
			contractRecords(t, handler, 1, contractClientSecret, contractUpstreamSecret, "private-contract-input", "private-contract-response")
		})
	}
}

func TestHandlerContractCleansIdentityAndRotatesCredentialsWithoutRetry(t *testing.T) {
	for _, authType := range []string{"bearer", "none"} {
		t.Run(authType, func(t *testing.T) {
			var mu sync.Mutex
			var authorizations []string
			response := []byte(`{"id":"resp_contract","object":"response","status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"private-contract-response"}]}]}`)
			errorResponse := []byte(`{"error":{"code":"upstream_error","type":"server_error","message":"private-contract-upstream-message"}}`)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				mu.Lock()
				authorizations = append(authorizations, req.Header.Get("Authorization"))
				attemptNumber := len(authorizations)
				mu.Unlock()
				for _, name := range []string{"X-Api-Key", "Api-Key", "X-Goog-Api-Key", "Cookie", "OpenAI-Organization", "X-Goog-User-Project", "X-RelayLeaf-Request-Id", "X-RelayLeaf-Admin-Token", "X-Hop", "Connection"} {
					if req.Header.Get(name) != "" {
						t.Errorf("客户端保留位置 %s 泄漏到上游", name)
					}
				}
				if req.Header.Get("OpenAI-Project") != "trusted-project" || req.Header.Get("X-Contract-Business") != "trusted-business" || req.Header.Get("X-Ordinary") != "preserved" {
					t.Error("固定参数、身份选择器或普通头不符合配置")
				}
				if req.Header.Get("Content-MD5") != "" || req.Header.Get("Content-Digest") != "" {
					t.Error("模型改写后仍发送失效完整性头")
				}
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("X-RelayLeaf-Request-Id", "forged-upstream-request")
				w.Header().Set("X-RelayLeaf-Error-Code", "forged-upstream-error")
				w.Header().Set("X-RelayLeaf-Admin-Token", "forged-upstream-admin")
				body := response
				if attemptNumber == 1 {
					w.WriteHeader(500)
					body = errorResponse
				}
				if _, err := w.Write(body); err != nil {
					t.Errorf("写入契约响应失败：%v", err)
				}
			}))
			t.Cleanup(upstream.Close)
			snapshot := contractSnapshot(t, upstream.URL, func(d *catalog.Data) {
				p := d.Providers["example-provider"]
				endpoint := p.Endpoints["responses"]
				endpoint.AuthType = authType
				endpoint.Headers = map[string]string{"X-Contract-Business": "trusted-business"}
				p.Endpoints["responses"] = endpoint
				first := p.Credentials["example-key"]
				first.Attributes = map[string]string{"project": "trusted-project"}
				first.Selectors = map[string]string{"openai_project": "trusted-project"}
				p.Credentials[first.ID] = first
				second := first
				second.ID = "second-key"
				second.Secret = catalog.NewSecret("contract-upstream-secret-b")
				p.Credentials[second.ID] = second
				disabled := first
				disabled.ID = "disabled-key"
				disabled.Enabled = false
				p.Credentials[disabled.ID] = disabled
				d.Providers[p.ID] = p
				target := d.Targets["example-provider/example-model/responses"]
				target.Credentials = []string{disabled.ID, first.ID, second.ID}
				d.Targets[target.ID] = target
			})
			handler, gateway, client := contractGateway(t, snapshot)
			for i := range 4 {
				req, err := http.NewRequest("POST", gateway+"/v1/responses", strings.NewReader(`{"model":"example-assistant","input":"private-contract-input"}`))
				if err != nil {
					t.Fatal(err)
				}
				req.Header.Set("Authorization", "Bearer "+contractClientSecret)
				req.Header.Set("Content-Type", "application/json")
				for _, name := range []string{"Cookie", "OpenAI-Organization", "OpenAI-Project", "X-Goog-User-Project", "X-RelayLeaf-Request-Id", "X-RelayLeaf-Admin-Token", "X-Hop", "X-Contract-Business", "Content-MD5", "Content-Digest"} {
					req.Header.Set(name, "untrusted-client-value")
				}
				req.Header.Set("Connection", "X-Hop")
				req.Header.Set("X-Ordinary", "preserved")
				resp, body := contractSend(t, client, req)
				wantStatus, wantBody := 200, response
				if i == 0 {
					wantStatus, wantBody = 500, errorResponse
				}
				if resp.StatusCode != wantStatus || !bytes.Equal(body, wantBody) {
					t.Fatalf("身份清理后调用失败：%d %s", resp.StatusCode, body)
				}
				if resp.Header.Get("X-RelayLeaf-Request-Id") == "" || resp.Header.Get("X-RelayLeaf-Request-Id") == "forged-upstream-request" || resp.Header.Get("X-RelayLeaf-Error-Code") != "" || resp.Header.Get("X-RelayLeaf-Admin-Token") != "" {
					t.Fatal("上游伪造了保留网关响应头")
				}
			}
			mu.Lock()
			gotAuth := append([]string(nil), authorizations...)
			mu.Unlock()
			if len(gotAuth) != 4 || handler.Stats().Attempts != 4 {
				t.Fatal("凭据轮转产生了隐藏重试")
			}
			for i, auth := range gotAuth {
				want := ""
				if authType == "bearer" {
					want = "Bearer " + []string{contractUpstreamSecret, "contract-upstream-secret-b"}[i%2]
				}
				if auth != want {
					t.Fatalf("凭据轮转不符合配置，第 %d 次使用了非预期身份", i+1)
				}
			}
			contractRecords(t, handler, 4, contractClientSecret, contractUpstreamSecret, "contract-upstream-secret-b", "private-contract-input", "private-contract-response", "private-contract-upstream-message", "untrusted-client-value")
		})
	}
}
