package routing

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"relayleaf/internal/catalog"
	"relayleaf/internal/protocol"
)

func TestBuildRequestPreservesRawEncodingOrReplacesOnlyModel(t *testing.T) {
	decoded := []byte("{\n  \"model\": \"public-model\", \"input\": \"hello\", \"extension\": {\"model\":\"untouched\", \"number\":9007199254740993, \"array\":[1, 2]}\n}")
	facts, err := protocol.ScanRequest(decoded, 65536)
	if err != nil {
		t.Fatal(err)
	}
	var compressed bytes.Buffer
	gz := gzip.NewWriter(&compressed)
	_, _ = gz.Write(decoded)
	_ = gz.Close()
	for _, changed := range []bool{false, true} {
		t.Run(map[bool]string{false: "原压缩正文", true: "局部模型替换"}[changed], func(t *testing.T) {
			candidate := Candidate{Provider: catalog.Provider{Origin: "https://trusted.example"}, Endpoint: catalog.Endpoint{PathTemplate: "/vendor/%72esponses", AuthType: "none"}, Target: catalog.Target{UpstreamName: facts.Model}}
			if changed {
				candidate.Target.UpstreamName = "new\"model"
			}
			inbound := httptest.NewRequest("POST", "http://host/v1/responses?one=%2f&one=+&blank=&bare&KEY=ordinary", nil)
			inbound.Header.Set("Content-Type", "application/json")
			inbound.Header.Set("Content-Encoding", "gzip")
			inbound.Header.Set("Connection", "Content-Type, Content-Encoding")
			inbound.Header.Set("Content-MD5", "stale")
			inbound.Header.Set("Digest", "stale")
			inbound.Header.Set("Content-Length", "999")
			req, buildErr := BuildRequest(context.Background(), inbound, candidate, facts, compressed.Bytes(), decoded)
			if buildErr != nil {
				t.Fatal(buildErr)
			}
			body, readErr := io.ReadAll(req.Body)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if req.URL.EscapedPath() != "/vendor/%72esponses" || req.URL.RawQuery != inbound.URL.RawQuery || req.Host != "trusted.example" {
				t.Fatal("上游 URL 未由配置产生或普通查询参数被重编码")
			}
			if req.ContentLength != int64(len(body)) || req.Header.Get("Content-Length") != "" {
				t.Fatal("出站长度未按实际正文重建")
			}
			if changed {
				want := strings.Replace(string(decoded), `"public-model"`, `"new\"model"`, 1)
				if string(body) != want || req.Header.Get("Content-Encoding") != "" || req.Header.Get("Content-MD5") != "" || req.Header.Get("Digest") != "" {
					t.Fatalf("局部替换或失效头清理错误：%s", body)
				}
			} else if !bytes.Equal(body, compressed.Bytes()) || req.Header.Get("Content-Encoding") != "gzip" || req.Header.Get("Digest") != "stale" {
				t.Fatal("模型未改变时未保留原始压缩字节及有效头")
			}
			if inbound.Header.Get("Content-MD5") != "stale" {
				t.Fatal("入站请求被改写")
			}
		})
	}
}

func TestBuildRequestStripsUntrustedIdentityAndInjectsTrustedValues(t *testing.T) {
	body := []byte(`{"model":"same","input":"hello"}`)
	facts, err := protocol.ScanRequest(body, 65536)
	if err != nil {
		t.Fatal(err)
	}
	for _, authType := range []string{"none", "bearer", "header", "query"} {
		t.Run(authType, func(t *testing.T) {
			inbound := httptest.NewRequest("POST", "/v1/responses?ordinary=%2f&%6bey=client&business=old&custom_auth=untrusted&ordinary=+", nil)
			for _, name := range []string{"Authorization", "X-Api-Key", "Api-Key", "X-Goog-Api-Key", "Cookie", "OpenAI-Organization", "OpenAI-Project", "X-Goog-User-Project", "X-RelayLeaf-Admin-Token", "Proxy-Authorization", "Keep-Alive", "X-Custom-Auth"} {
				inbound.Header.Set(name, "untrusted")
			}
			inbound.Header.Set("Connection", "X-Hop")
			inbound.Header.Set("X-Hop", "untrusted")
			inbound.Header.Set("X-Ordinary", "preserved")
			inbound.Header.Set("Content-Type", "application/json")
			inbound.Header.Set("X-Business", "old")
			endpoint := catalog.Endpoint{PathTemplate: "/v1/responses", AuthType: authType, Headers: map[string]string{"X-Business": "trusted"}, Query: map[string]string{"business": "trusted value"}}
			if authType == "header" {
				endpoint.AuthName = "X-Custom-Auth"
			}
			if authType == "query" {
				endpoint.AuthName = "custom_auth"
			}
			candidate := Candidate{Provider: catalog.Provider{Origin: "https://trusted.example"}, Endpoint: endpoint, Target: catalog.Target{UpstreamName: "same"}, Credential: catalog.Credential{Secret: catalog.NewSecret("upstream-secret"), Selectors: map[string]string{"openai_project": "trusted-project"}}}
			req, buildErr := BuildRequest(context.Background(), inbound, candidate, facts, body, body)
			if buildErr != nil {
				t.Fatal(buildErr)
			}
			for _, name := range []string{"X-Api-Key", "Api-Key", "X-Goog-Api-Key", "Cookie", "OpenAI-Organization", "X-Goog-User-Project", "X-RelayLeaf-Admin-Token", "Proxy-Authorization", "Keep-Alive", "Connection", "X-Hop"} {
				if req.Header.Get(name) != "" {
					t.Errorf("不可信头 %s 泄漏", name)
				}
			}
			if req.Header.Get("OpenAI-Project") != "trusted-project" || req.Header.Get("X-Business") != "trusted" || req.Header.Get("X-Ordinary") != "preserved" {
				t.Fatal("可信固定头或身份选择器错误")
			}
			wantAuth := ""
			if authType == "bearer" {
				wantAuth = "Bearer upstream-secret"
			}
			if req.Header.Get("Authorization") != wantAuth {
				t.Fatal("出站 Bearer 认证错误")
			}
			if authType == "header" && req.Header.Get("X-Custom-Auth") != "upstream-secret" {
				t.Fatal("出站自定义头认证错误")
			}
			if req.URL.Query().Has("key") || req.URL.Query().Get("business") != "trusted value" || !strings.HasPrefix(req.URL.RawQuery, "ordinary=%2f&") || !strings.Contains(req.URL.RawQuery, "ordinary=+") {
				t.Fatal("查询清理破坏普通参数或泄漏客户端密钥")
			}
			if authType == "query" && req.URL.Query().Get("custom_auth") != "upstream-secret" {
				t.Fatal("出站查询认证错误")
			}
		})
	}
}

func TestBuildRequestRejectsMalformedQueryBeforeTransport(t *testing.T) {
	body := []byte(`{"model":"same","input":"hello"}`)
	facts, err := protocol.ScanRequest(body, 65536)
	if err != nil {
		t.Fatal(err)
	}
	candidate := Candidate{Provider: catalog.Provider{Origin: "https://trusted.example"}, Endpoint: catalog.Endpoint{PathTemplate: "/v1/responses", AuthType: "none"}, Target: catalog.Target{UpstreamName: "same"}}
	for _, query := range []string{"%zz=value", "ordinary=%zz", "ordinary=a;b"} {
		inbound := httptest.NewRequest("POST", "/v1/responses?"+query, nil)
		inbound.Header.Set("Content-Type", "application/json")
		req, buildErr := BuildRequest(context.Background(), inbound, candidate, facts, body, body)
		if req != nil || buildErr == nil || buildErr.Code != "invalid_request_metadata" {
			t.Fatalf("非法查询参数未在构造时拒绝：%v", buildErr)
		}
	}
}
