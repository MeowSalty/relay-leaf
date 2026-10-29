package relay

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"

	"relayleaf/internal/catalog"
	"relayleaf/internal/config"
)

func routingToken(clientID string) string {
	return "routing-client-" + clientID + "-0123456789abcdefghijklmnopqrstuv"
}

func routingUpstreamToken(credentialID string) string {
	return "routing-upstream-" + credentialID
}

func routingConfigMap(t *testing.T, document map[string]any, path ...string) map[string]any {
	t.Helper()
	current := document
	for _, key := range path {
		next, ok := current[key].(map[string]any)
		if !ok {
			t.Fatalf("测试配置中的对象不存在：%v", path)
		}
		current = next
	}
	return current
}

func routingConfigClone(t *testing.T, value map[string]any) map[string]any {
	t.Helper()
	raw, err := yaml.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var clone map[string]any
	if err := yaml.Unmarshal(raw, &clone); err != nil {
		t.Fatal(err)
	}
	return clone
}

// routingConfig 从实际模板构造两级上游与独立客户端，所有配置仍经 YAML 编译。
func routingConfig(t *testing.T, primaryOrigin, backupOrigin string) map[string]any {
	t.Helper()
	var document map[string]any
	if err := yaml.Unmarshal(config.Example(), &document); err != nil {
		t.Fatal(err)
	}
	document["access_groups"] = map[string]any{"general": map[string]any{}, "coding": map[string]any{}, "internal": map[string]any{}}
	keys := map[string]any{}
	for _, id := range []string{"general-client", "coding-client", "blocked-client", "route-blocked-client"} {
		groups := []string{"general"}
		if id == "coding-client" {
			groups = []string{"coding"}
		} else if id == "blocked-client" {
			groups = []string{}
		}
		routes := []string{"example-responses", "alias-responses"}
		if id == "route-blocked-client" {
			routes = []string{}
		}
		keys[id] = map[string]any{"enabled": true, "secret": routingToken(id), "secret_version": "v1", "allowed_routes": routes, "allowed_access_groups": groups, "denied_access_groups": []string{"internal"}, "limits": map[string]any{"max_inflight": 4}}
	}
	routingConfigMap(t, document, "client_auth")["keys"] = keys
	template := routingConfigMap(t, document, "providers", "example-provider")
	providers := map[string]any{}
	for _, setup := range []struct{ id, origin string }{{"primary", primaryOrigin}, {"backup", backupOrigin}} {
		provider := routingConfigClone(t, template)
		provider["origin"] = setup.origin
		credentialIDs := []string{"general-key", "coding-key", "internal-key"}
		if setup.id == "backup" {
			credentialIDs = []string{"backup-key"}
		}
		credentials := map[string]any{}
		for _, id := range credentialIDs {
			groups := []string{"general"}
			if id == "coding-key" {
				groups = []string{"coding"}
			} else if id == "internal-key" {
				groups = []string{"general", "internal"}
			}
			project := "project-" + setup.id
			credentials[id] = map[string]any{"enabled": true, "secret": routingUpstreamToken(id), "secret_version": "v1", "access_groups": groups, "identity": map[string]any{"attributes": map[string]any{"project": project}, "selectors": map[string]any{"openai_project": project}}}
		}
		provider["credentials"] = credentials
		model := routingConfigMap(t, provider, "models", "example-model")
		model["upstream_name"] = setup.id + "-model"
		binding := routingConfigMap(t, model, "bindings", "responses")
		binding["credentials"] = credentialIDs
		binding["capabilities"] = []string{"image_input", "tool_calling", "file_input", "server_tools"}
		provider["models"] = map[string]any{"model": model}
		providers[setup.id] = provider
	}
	document["providers"] = providers
	route := routingConfigMap(t, document, "routes", "example-responses")
	route["targets"] = []any{map[string]any{"target": "primary/model/responses", "priority": 10, "weight": 1}, map[string]any{"target": "backup/model/responses", "priority": 20, "weight": 1}}
	alias := routingConfigClone(t, route)
	alias["public_model"] = "example-alias"
	routingConfigMap(t, document, "routes")["alias-responses"] = alias
	return document
}

func routingFixedScope(t *testing.T, document map[string]any, shared bool, resourceTypes ...string) {
	t.Helper()
	provider := routingConfigMap(t, document, "providers", "primary")
	scope := map[string]any{"owner": map[string]any{"project": "project-primary"}, "resource_types": resourceTypes, "sharing": "private"}
	routingConfigMap(t, provider, "credentials", "general-key")["resource_scope"] = "fixed"
	if shared {
		routingConfigMap(t, provider, "credentials", "coding-key")["resource_scope"] = "fixed"
		scope["sharing"] = "verified_shared"
		scope["verification"] = map[string]any{"checked_at": "2026-10-29T00:00:00Z", "evidence": "受控测试上游验证全部声明的资源类型", "credential_versions": map[string]any{"general-key": "v1", "coding-key": "v1"}}
	}
	provider["resource_scopes"] = map[string]any{"fixed": scope}
	route := routingConfigMap(t, document, "routes", "example-responses")
	route["resource_policy"] = map[string]any{"mode": "fixed_scope", "scope": "primary/fixed"}
	route["targets"] = []any{map[string]any{"target": "primary/model/responses", "priority": 10, "weight": 1}}
}

func routingSnapshot(t *testing.T, document map[string]any) *catalog.Snapshot {
	t.Helper()
	raw, err := yaml.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "relayleaf.yaml")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	snapshot, err := config.LoadFile(context.Background(), path, config.Options{ResolveSecrets: true, LookupEnv: func(string) (string, bool) {
		t.Error("使用内联测试材料的配置不应读取环境秘密")
		return "", false
	}})
	if err != nil {
		t.Fatalf("加载真实 YAML 测试配置失败：%v", err)
	}
	return snapshot
}

func routingGateway(t *testing.T, document map[string]any, options HandlerOptions) (*Handler, *httptest.Server, *http.Client) {
	t.Helper()
	handler, err := NewHandler(routingSnapshot(t, document), options)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	client := &http.Client{Transport: &http.Transport{DisableCompression: true, Proxy: nil}, Timeout: 5 * time.Second}
	t.Cleanup(func() {
		client.CloseIdleConnections()
		_ = handler.Close()
		server.Close()
	})
	return handler, server, client
}

func routingRequest(t *testing.T, gateway, clientID string, body []byte) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, gateway+"/v1/responses", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+routingToken(clientID))
	return req
}
