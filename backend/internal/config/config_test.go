package config

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"go.yaml.in/yaml/v3"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"relayleaf/internal/catalog"
)

const validClient = "rlk_0123456789abcdefghijklmnopqrstuv"

func testOptions() Options {
	return Options{ResolveSecrets: true, LookupEnv: func(k string) (string, bool) {
		switch k {
		case "RELAYLEAF_CLIENT_KEY":
			return validClient, true
		case "RELAYLEAF_UPSTREAM_KEY":
			return "upstream-private-key", true
		}
		return "", false
	}, Now: func() time.Time { return time.Date(2026, 10, 28, 0, 0, 0, 0, time.UTC) }}
}
func parse(t *testing.T, s string, o Options) *catalog.Snapshot {
	t.Helper()
	snapshot, err := loadBytes(context.Background(), "fixture.yaml", []byte(s), o)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}
func TestExampleAndImmutableCandidate(t *testing.T) {
	s := parse(t, string(Example()), testOptions())
	summary := s.Summary()
	if summary.Providers != 1 || summary.Targets != 1 || summary.Routes != 1 || summary.ClientKeys != 1 || !summary.SecretsResolved {
		t.Fatalf("摘要：%+v", summary)
	}
	now := testOptions().Now()
	k, ok := s.Authenticate(validClient, now)
	if !ok || k.ID != "local-client" {
		t.Fatal("有效密钥被拒绝")
	}
	k.AllowedRoutes[0] = "changed"
	if k, ok = s.Authenticate(validClient, now); !ok || k.AllowedRoutes[0] != "example-responses" {
		t.Fatal("密钥修改影响了快照")
	}
	if _, ok = s.Authenticate(validClient+"x", now); ok {
		t.Fatal("无效密钥被接受")
	}
	p := s.Providers()
	provider := p["example-provider"]
	e := provider.Endpoints["responses"]
	e.RequestModes[0] = "changed"
	provider.Endpoints["responses"] = e
	delete(p, "example-provider")
	if s.Providers()["example-provider"].Endpoints["responses"].RequestModes[0] != "non_stream" {
		t.Fatal("映射修改影响了快照")
	}
	r := s.Routes()
	route := r["example-responses"]
	route.Targets[0].Weight = 100
	if s.Routes()["example-responses"].Targets[0].Weight != 1 {
		t.Fatal("路由修改影响了快照")
	}
	a := Example()
	a[0] = '!'
	if Example()[0] != '#' {
		t.Fatal("示例共用了缓冲区")
	}
}
func TestCheckDoesNotReadSecrets(t *testing.T) {
	calls := 0
	parse(t, string(Example()), Options{LookupEnv: func(string) (string, bool) { calls++; return "", false }})
	if calls != 0 {
		t.Fatal("普通检查解析了密钥内容")
	}
	bad := strings.Replace(string(Example()), "allowed_routes: [example-responses]", "allowed_routes: [missing]", 1)
	o := testOptions()
	o.LookupEnv = func(string) (string, bool) { calls++; return "", false }
	if _, err := loadBytes(context.Background(), "fixture.yaml", []byte(bad), o); err == nil {
		t.Fatal("无效引用被接受")
	}
	if calls != 0 {
		t.Fatal("完整配置图校验前读取了密钥内容")
	}
}
func TestStrictYAMLAndUnsupported(t *testing.T) {
	base := string(Example())
	tests := map[string]string{
		"duplicate":                base + "version: 1\n",
		"multi_document":           base + "---\nversion: 1\n",
		"alias":                    strings.Replace(base, "enabled: false", "enabled: &flag false", 1) + "extra: *flag\n",
		"merge":                    base + "merge: {<<: {x: y}}\n",
		"unknown":                  base + "unknown_secret_value: super-sensitive\n",
		"unknown_version":          strings.Replace(base, "version: 1", "version: 2", 1),
		"null_limits":              strings.Replace(base, "limits: {max_inflight: 4}", "limits: null", 1),
		"null_rate":                strings.Replace(base, "limits: {max_inflight: 4}", "limits: {request_rate: null}", 1),
		"wrong_bool":               strings.Replace(base, "enabled: false", "enabled: 'false'", 1),
		"wrong_integer":            strings.Replace(base, "max_inflight: 32", "max_inflight: 32.5", 1),
		"oversized_integer":        strings.Replace(base, "max_inflight: 32", "max_inflight: 2147483648", 1),
		"unsupported_rate":         strings.Replace(base, "limits: {max_inflight: 4}", "limits: {request_rate: {requests: 60, interval: 1m, burst: 5}}", 1),
		"bad_rate_interval":        strings.Replace(base, "limits: {max_inflight: 4}", "limits: {request_rate: {requests: 60, interval: 100us, burst: 5}}", 1),
		"queue":                    strings.Replace(base, "queue_capacity: 0", "queue_capacity: 1", 1),
		"breaker_default":          strings.Replace(base, "  circuit_breaker: {enabled: false}\n", "", 1),
		"retries_default":          strings.Replace(base, "  max_retries: 0\n", "", 1),
		"conditional_retry":        strings.Replace(base, "  max_retries: 0", "  max_retries: 0\n  conditional_retry: {enabled: true}", 1),
		"affinity":                 strings.Replace(base, "session_affinity: {enabled: false}", "session_affinity: {enabled: true, ttl: 1m}", 1),
		"closed_affinity_bad":      strings.Replace(base, "session_affinity: {enabled: false}", "session_affinity: {enabled: false, ttl: 0s}", 1),
		"closed_first_content_bad": strings.Replace(base, "  max_retries: 0", "  max_retries: 0\n  stream_first_content_timeout: {enabled: false, timeout: 0s}", 1),
		"fixed_scope":              strings.Replace(base, "resource_policy: {mode: stateless_pool}", "resource_policy: {mode: fixed_scope, scope: example-provider/scope}", 1),
		"unknown_credential":       strings.Replace(base, "credentials: [example-key]", "credentials: [other/key]", 1),
		"binding_empty":            strings.Replace(base, "credentials: [example-key]", "credentials: []", 1),
		"unsupported_mode":         strings.Replace(base, "[non_stream, stream]", "[non_stream, websocket]", 1),
		"auth_reserved_identity":   strings.Replace(base, "auth: {type: bearer}", "auth: {type: header, name: OpenAI-Project}", 1),
		"identity_mismatch":        strings.Replace(base, "secret_ref: env:RELAYLEAF_UPSTREAM_KEY", "identity: {attributes: {project: a}, selectors: {openai_project: b}}\n        secret_ref: env:RELAYLEAF_UPSTREAM_KEY", 1),
		"invalid_origin":           strings.Replace(base, "https://api.example.com", "https://secret:password@api.example.com", 1),
		"unknown_proxy":            strings.Replace(base, "origin: https://api.example.com", "origin: https://api.example.com\n    proxy: http://proxy.example.com", 1),
	}
	tests["anchored_key"] = strings.Replace(base, "version: 1", "&version_anchor version: 1", 1)
	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := loadBytes(context.Background(), "fixture.yaml", []byte(input), Options{})
			if err == nil {
				t.Fatal("无效配置被接受")
			}
			var e *Error
			if !errors.As(err, &e) || e.Line <= 0 || e.Column <= 0 || e.Path == "" {
				t.Fatalf("缺少安全诊断信息：%v", err)
			}
			if strings.Contains(err.Error(), "super-sensitive") || strings.Contains(err.Error(), "password") {
				t.Fatalf("错误暴露了字段值：%v", err)
			}
		})
	}
}
func TestResourceLimits(t *testing.T) {
	for name, input := range map[string]string{"bytes": strings.Repeat(" ", MaxFileBytes+1), "depth": "x: " + strings.Repeat("[", MaxDepth+1) + "a" + strings.Repeat("]", MaxDepth+1), "nodes": "x: [" + strings.Repeat("a,", MaxNodes) + "a]"} {
		t.Run(name, func(t *testing.T) {
			if _, err := loadBytes(context.Background(), "fixture.yaml", []byte(input), Options{}); err == nil {
				t.Fatal("未受限的 YAML 被接受")
			}
		})
	}
}
func TestExecutionInheritance(t *testing.T) {
	s := string(Example())
	s = strings.Replace(s, "origin: https://api.example.com", "origin: https://api.example.com\n    execution:\n      stream_first_event_timeout: 90s\n      empty_response: {max_wait: 80s}", 1)
	s = strings.Replace(s, "secret_ref: env:RELAYLEAF_UPSTREAM_KEY", "execution:\n          stream_first_event_timeout: null\n          empty_response: {enabled: false}\n        secret_ref: env:RELAYLEAF_UPSTREAM_KEY", 1)
	snap := parse(t, s, Options{})
	e := snap.Providers()["example-provider"].Credentials["example-key"].Execution
	if e.StreamFirstEventTimeout != 90*time.Second || e.EmptyResponseMaxWait != 80*time.Second || e.EmptyResponseEnabled {
		t.Fatalf("继承的执行配置错误：%+v", e)
	}
	if e.Sources["stream_first_event_timeout"] != "$.providers.example-provider.execution" {
		t.Fatal("来源信息丢失")
	}
	e.Sources["stream_first_event_timeout"] = "changed"
	if snap.Providers()["example-provider"].Credentials["example-key"].Execution.Sources["stream_first_event_timeout"] == "changed" {
		t.Fatal("来源信息被修改")
	}
	bad := strings.Replace(s, "execution:\n          stream_first_event_timeout: null", "execution:\n          max_retries: null\n          stream_first_event_timeout: null", 1)
	if _, err := loadBytes(context.Background(), "fixture.yaml", []byte(bad), Options{}); err == nil {
		t.Fatal("接受了不允许的 null 覆盖")
	}
	d := defaultExecution()
	if d.MaxRetries != 1 || !d.EmptyResponseEnabled {
		t.Fatal("内置默认值发生变化")
	}
}
func TestSecretLifetimeAndNoneAuthentication(t *testing.T) {
	base := string(Example())
	for _, field := range []string{"enabled: false", "expires_at: '2026-10-28T00:00:00Z'"} {
		s := base
		if strings.HasPrefix(field, "enabled") {
			s = strings.Replace(s, "    local-client:\n      enabled: true", "    local-client:\n      "+field, 1)
		} else {
			s = strings.Replace(s, "    local-client:", "    local-client:\n      "+field, 1)
		}
		o := testOptions()
		o.LookupEnv = func(k string) (string, bool) {
			if k == "RELAYLEAF_UPSTREAM_KEY" {
				return "upstream-private-key", true
			}
			return "", false
		}
		snap := parse(t, s, o)
		if _, ok := snap.Authenticate(validClient, o.Now()); ok {
			t.Fatal("未启用的密钥进入了索引")
		}
	}
	none := strings.Replace(base, "auth: {type: bearer}", "auth: {type: none}", 1)
	none = strings.Replace(none, "        secret_ref: env:RELAYLEAF_UPSTREAM_KEY\n        secret_version: v1\n", "", 1)
	o := testOptions()
	calls := 0
	env := o.LookupEnv
	o.LookupEnv = func(k string) (string, bool) { calls++; return env(k) }
	parse(t, none, o)
	if calls != 1 {
		t.Fatal("none 认证解析了不需要的密钥内容")
	}
	retiring := strings.Replace(base, "      secret_version: v1", "      retiring_secret: {secret_ref: env:MISSING, secret_version: v0, valid_until: '2026-10-28T00:00:00Z'}\n      secret_version: v1", 1)
	parse(t, retiring, testOptions())
	retiring = strings.Replace(retiring, "2026-10-28T00:00:00Z", "2026-10-29T00:00:00Z", 1)
	if _, err := loadBytes(context.Background(), "fixture.yaml", []byte(retiring), testOptions()); err == nil {
		t.Fatal("接受了缺少有效退役密钥的配置")
	}
}
func TestSecretBoundariesAndRedaction(t *testing.T) {
	for name, material := range map[string]string{"short": "tiny", "overlong": strings.Repeat("a", 513), "too_large": strings.Repeat("a", MaxSecretBytes+1), "non_ascii": strings.Repeat("a", 33) + "中", "embedded_equal": strings.Repeat("a", 32) + "=x", "whitespace": validClient + " ", "control": validClient + "\r", "env_newline": validClient + "\n"} {
		t.Run(name, func(t *testing.T) {
			o := testOptions()
			o.LookupEnv = func(k string) (string, bool) {
				if k == "RELAYLEAF_CLIENT_KEY" {
					return material, true
				}
				return "upstream-private-key", true
			}
			if _, err := loadBytes(context.Background(), "fixture.yaml", Example(), o); err == nil {
				t.Fatal("无效密钥内容被接受")
			} else if strings.Contains(err.Error(), material) {
				t.Fatal("密钥内容泄露")
			}
		})
	}
	snap := parse(t, string(Example()), testOptions())
	digest := sha256.Sum256([]byte(validClient))
	values := []any{snap, *snap, snap.Providers(), snap.Providers()["example-provider"].Credentials["example-key"].Secret}
	for _, v := range values {
		for _, format := range []string{"%v", "%+v", "%#v"} {
			formatted := fmt.Sprintf(format, v)
			if strings.Contains(formatted, validClient) || strings.Contains(formatted, "upstream-private-key") || strings.Contains(formatted, hex.EncodeToString(digest[:])) {
				t.Fatalf("格式化输出泄露：%s", formatted)
			}
		}
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(b), validClient) || strings.Contains(string(b), "upstream-private-key") {
			t.Fatal("JSON 泄露了密钥内容")
		}
	}
}
func TestSecretFileAndMissingError(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "relayleaf.yaml")
	s := strings.Replace(string(Example()), "env:RELAYLEAF_CLIENT_KEY", "file:client.key", 1)
	if err := os.WriteFile(file, []byte(s), 0600); err != nil {
		t.Fatal(err)
	}
	o := testOptions()
	for _, suffix := range []string{"", "\n", "\r\n"} {
		if err := os.WriteFile(filepath.Join(dir, "client.key"), []byte(validClient+suffix), 0600); err != nil {
			t.Fatal(err)
		}
		snap, err := LoadFile(context.Background(), file, o)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := snap.Authenticate(validClient, o.Now()); !ok {
			t.Fatal("未处理文件换行符")
		}
	}
	for _, suffix := range []string{"\r", "\n\n", " \n"} {
		if err := os.WriteFile(filepath.Join(dir, "client.key"), []byte(validClient+suffix), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadFile(context.Background(), file, o); err == nil {
			t.Fatal("无效尾部内容被接受")
		}
	}
	if _, err := LoadFile(context.Background(), filepath.Join(dir, "absent.yaml"), Options{}); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("缺少错误链：%v", err)
	}
}
func FuzzConfig(f *testing.F) {
	for _, seed := range [][]byte{Example(), []byte("version: 1\nversion: 2"), []byte("x: &a [*a]"), []byte("x: null"), []byte("---\n---\n"), []byte("x: !unknown secret"), []byte("x: [0xffffffffffffffffffff]")} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		snapshot, err := loadBytes(context.Background(), "fuzz.yaml", b, Options{})
		if err == nil && snapshot == nil {
			t.Fatal("成功加载的候选配置为 nil")
		}
		if err != nil {
			var e *Error
			if !errors.As(err, &e) {
				t.Fatalf("解析错误缺少结构化信息：%v", err)
			}
		}
	})
}

func TestGraphBoundariesAndScopes(t *testing.T) {
	cases := map[string]func(*document){
		"duplicate_client_material": func(d *document) { d.ClientAuth.Keys["second-client"] = d.ClientAuth.Keys["local-client"] },
		"duplicate_current_retiring": func(d *document) {
			k := d.ClientAuth.Keys["local-client"]
			k.Retiring = &retiringDTO{SecretRef: k.SecretRef, SecretVersion: "v0", ValidUntil: "2026-10-29T00:00:00Z"}
			d.ClientAuth.Keys["local-client"] = k
		},
		"conflicting_route": func(d *document) { d.Routes["second-route"] = d.Routes["example-responses"] },
		"two_enabled_targets": func(d *document) {
			p := d.Providers["example-provider"]
			p.Models["second-model"] = p.Models["example-model"]
			r := d.Routes["example-responses"]
			target := r.Targets[0]
			target.Target = "example-provider/second-model/responses"
			r.Targets = append(r.Targets, target)
			d.Routes["example-responses"] = r
		},
		"capacity": func(d *document) { d.Limits.MaxRuntimeStateEntries = 2 },
		"scope_owner_mismatch": func(d *document) {
			p := d.Providers["example-provider"]
			p.ResourceScopes = map[string]resourceScopeDTO{"project": {Owner: map[string]string{"project": "p"}, ResourceTypes: []string{"openai.response"}}}
			cred := p.Credentials["example-key"]
			cred.ResourceScope = "project"
			p.Credentials["example-key"] = cred
			d.Providers["example-provider"] = p
		},
		"shared_missing_verification": func(d *document) {
			p := d.Providers["example-provider"]
			p.ResourceScopes = map[string]resourceScopeDTO{"project": {Owner: map[string]string{"project": "p"}, ResourceTypes: []string{"openai.response"}, Sharing: "verified_shared"}}
			d.Providers["example-provider"] = p
		},
		"foreign_quota_target": func(d *document) {
			p := d.Providers["example-provider"]
			p.QuotaScopes = map[string]quotaScopeDTO{"quota": {Owner: map[string]string{"project": "p"}, Targets: []string{"foreign/model/responses"}}}
			d.Providers["example-provider"] = p
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			var d document
			if err := yaml.Unmarshal(Example(), &d); err != nil {
				t.Fatal(err)
			}
			mutate(&d)
			b, err := yaml.Marshal(d)
			if err != nil {
				t.Fatal(err)
			}
			var node yaml.Node
			if err := yaml.Unmarshal(b, &node); err != nil {
				t.Fatal(err)
			}
			removeNullFields(&node)
			b, err = yaml.Marshal(&node)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = loadBytes(context.Background(), "fixture.yaml", b, testOptions()); err == nil {
				t.Fatal("无效候选配置被接受")
			}
			if strings.Contains(err.Error(), "此处不允许 null") {
				t.Fatalf("测试样本在待测边界之前失败：%v", err)
			}
		})
	}
	// Marshal 包含可选的 null；通过构造源配置样本移除它们。
	source := strings.Replace(string(Example()), "origin: https://api.example.com", "origin: https://api.example.com\n    resource_scopes:\n      project:\n        owner: {project: p}\n        resource_types: [openai.response]\n    quota_scopes:\n      quota:\n        owner: {project: p}\n        limits: {max_inflight: 2}", 1)
	source = strings.Replace(source, "secret_ref: env:RELAYLEAF_UPSTREAM_KEY", "identity: {attributes: {project: p}}\n        resource_scope: project\n        quota_scopes: [quota]\n        secret_ref: env:RELAYLEAF_UPSTREAM_KEY", 1)
	snap := parse(t, source, testOptions())
	p := snap.Providers()["example-provider"]
	scope := p.ResourceScopes["project"]
	scope.Owner["project"] = "mutated"
	if snap.Providers()["example-provider"].ResourceScopes["project"].Owner["project"] != "p" {
		t.Fatal("作用域修改影响了快照")
	}
}
func removeNullFields(n *yaml.Node) {
	if n.Kind == yaml.MappingNode {
		content := n.Content[:0]
		for i := 0; i < len(n.Content); i += 2 {
			k, v := n.Content[i], n.Content[i+1]
			if v.Tag == "!!null" {
				continue
			}
			removeNullFields(v)
			content = append(content, k, v)
		}
		n.Content = content
	} else {
		for _, v := range n.Content {
			removeNullFields(v)
		}
	}
}

func TestNullScopeAndDiagnosticPositions(t *testing.T) {
	base := string(Example())
	for _, id := range []string{"execution", "key.execution", "execution_defaults"} {
		t.Run(id, func(t *testing.T) {
			source := strings.Replace(base, "local-client:", id+":", 1)
			source = strings.Replace(source, "limits: {max_inflight: 4}", "limits: {max_inflight: null}", 1)
			_, err := loadBytes(context.Background(), "fixture.yaml", []byte(source), Options{})
			if err == nil || !strings.Contains(err.Error(), "此处不允许 null") {
				t.Fatalf("标识符影响了 null 策略：%v", err)
			}
		})
	}
	for _, field := range []string{"unknown_field: super-sensitive\n", "version: 1\n"} {
		source := base + field
		_, err := loadBytes(context.Background(), "fixture.yaml", []byte(source), Options{})
		var e *Error
		if !errors.As(err, &e) {
			t.Fatal(err)
		}
		key := strings.Split(field, ":")[0]
		if e.Path != "$."+key || e.Line != strings.Count(base, "\n")+1 || e.Column != 1 {
			t.Fatalf("诊断信息丢失了出错字段名：%+v", e)
		}
		if strings.Contains(e.Error(), "super-sensitive") {
			t.Fatal("字段值泄露")
		}
	}
}
func TestClosedRulesRetainedAndInherited(t *testing.T) {
	base := string(Example())
	source := strings.Replace(base, "  max_retries: 0", "  max_retries: 0\n  conditional_retry:\n    enabled: false\n    rules:\n      - id: overloaded\n        match: {status_codes: [503], message_equals: [busy]}\n        action: retry", 1)
	source = strings.Replace(source, "  circuit_breaker: {enabled: false}", "  circuit_breaker:\n    enabled: false\n    rules:\n      - id: observe-busy\n        match: {status_codes: [503]}\n        action: {type: observe_only}", 1)
	snap := parse(t, source, Options{})
	cred := snap.Providers()["example-provider"].Credentials["example-key"]
	if len(cred.Execution.RetryRules) != 1 || cred.Execution.RetryRules[0].ID != "overloaded" || len(snap.Health().Rules) != 1 {
		t.Fatal("禁用状态下的规则集在继承时丢失")
	}
	cred.Execution.RetryRules[0].Match.MessageEquals[0] = "changed"
	h := snap.Health()
	h.Rules[0].Match.StatusCodes[0] = 500
	if snap.Providers()["example-provider"].Credentials["example-key"].Execution.RetryRules[0].Match.MessageEquals[0] != "busy" || snap.Health().Rules[0].Match.StatusCodes[0] != 503 {
		t.Fatal("规则修改影响了快照")
	}
	bad := strings.Replace(source, "enabled: false\n    rules:\n      - id: overloaded", "enabled: true\n    rules:\n      - id: overloaded", 1)
	if _, err := loadBytes(context.Background(), "fixture.yaml", []byte(bad), Options{}); err == nil || !strings.Contains(err.Error(), "不支持") {
		t.Fatalf("启用的不支持规则被接受：%v", err)
	}
	bad = strings.Replace(source, "match: {status_codes: [503], message_equals: [busy]}", "match: {}", 1)
	if _, err := loadBytes(context.Background(), "fixture.yaml", []byte(bad), Options{}); err == nil {
		t.Fatal("禁用状态下的无效匹配条件被接受")
	}
}
func TestCapabilityClassification(t *testing.T) {
	for _, cap := range []string{"file_input", "audio_input", "video_input", "server_tools", "image_output", "audio_output", "typo"} {
		source := strings.Replace(string(Example()), "credentials: [example-key]", "credentials: [example-key]\n            capabilities: ["+cap+"]", 1)
		_, err := loadBytes(context.Background(), "fixture.yaml", []byte(source), Options{})
		if err == nil {
			t.Fatal("不支持的能力被接受")
		}
		expected := "不支持"
		if cap == "typo" {
			expected = "未知能力"
		}
		if !strings.Contains(err.Error(), expected) {
			t.Fatalf("能力分类错误：%v", err)
		}
	}
}
func TestReservedFixedAndAuthenticationHeaders(t *testing.T) {
	for _, name := range []string{"Host", "Connection", "Content-Length", "Content-Type", "Content-Encoding", "Transfer-Encoding", "Upgrade", "Trailer", "TE", "Keep-Alive", "Proxy-Connection", "Proxy-Authorization", "Proxy-Authenticate", "OpenAI-Project", "Cookie"} {
		for _, header := range []string{name, strings.ToLower(name)} {
			t.Run(header, func(t *testing.T) {
				for _, replacement := range []string{"auth: {type: bearer}\n        headers: {" + header + ": gzip}", "auth: {type: header, name: " + header + "}"} {
					source := strings.Replace(string(Example()), "auth: {type: bearer}", replacement, 1)
					if _, err := loadBytes(context.Background(), "fixture.yaml", []byte(source), Options{}); err == nil {
						t.Fatal("保留请求头被接受")
					}
				}
			})
		}
	}
}
func TestQuotaBoundaryCanonicalization(t *testing.T) {
	for _, kind := range []string{"omitted_explicit_all", "reordered_all", "explicit_empty", "different_owner"} {
		t.Run(kind, func(t *testing.T) {
			var d document
			if err := yaml.Unmarshal(Example(), &d); err != nil {
				t.Fatal(err)
			}
			p := d.Providers["example-provider"]
			second := p.Models["example-model"]
			b := second.Bindings["responses"]
			disabled := false
			b.Enabled = &disabled
			second.Bindings = map[string]bindingDTO{"responses": b}
			p.Models["second-model"] = second
			all := []string{"example-provider/example-model/responses", "example-provider/second-model/responses"}
			first := quotaScopeDTO{Owner: map[string]string{"project": "p"}}
			secondScope := first
			switch kind {
			case "omitted_explicit_all":
				secondScope.Targets = all
			case "reordered_all":
				first.Targets = all
				secondScope.Targets = []string{all[1], all[0]}
			case "explicit_empty":
				first.Targets = []string{}
			case "different_owner":
				secondScope.Owner = map[string]string{"project": "q"}
			}
			p.QuotaScopes = map[string]quotaScopeDTO{"q1": first}
			if kind != "explicit_empty" {
				p.QuotaScopes["q2"] = secondScope
			}
			d.Providers["example-provider"] = p
			bytes, err := yaml.Marshal(d)
			if err != nil {
				t.Fatal(err)
			}
			var node yaml.Node
			if err := yaml.Unmarshal(bytes, &node); err != nil {
				t.Fatal(err)
			}
			removeNullFields(&node)
			quotaNode := yamlField(yamlField(yamlField(node.Content[0], "providers"), "example-provider"), "quota_scopes")
			if first.Targets == nil {
				deleteYAMLField(yamlField(quotaNode, "q1"), "targets")
			}
			if secondScope.Targets == nil && kind != "explicit_empty" {
				deleteYAMLField(yamlField(quotaNode, "q2"), "targets")
			}
			bytes, err = yaml.Marshal(&node)
			if err != nil {
				t.Fatal(err)
			}
			snap, err := loadBytes(context.Background(), "fixture.yaml", bytes, Options{})
			if kind == "different_owner" {
				if err != nil {
					t.Fatal(err)
				}
				if len(snap.Providers()["example-provider"].QuotaScopes["q1"].Targets) != 2 {
					t.Fatal("未解析省略的配额目标")
				}
			} else if err == nil || (!strings.Contains(err.Error(), "配额所有者与有效目标边界重复") && !strings.Contains(err.Error(), "显式设置的 targets")) {
				t.Fatalf("等价或空的配额边界被接受：%v", err)
			}
		})
	}
}
func yamlField(n *yaml.Node, key string) *yaml.Node {
	if n != nil {
		for i := 0; i < len(n.Content); i += 2 {
			if n.Content[i].Value == key {
				return n.Content[i+1]
			}
		}
	}
	return nil
}
func deleteYAMLField(n *yaml.Node, key string) {
	if n == nil {
		return
	}
	for i := 0; i < len(n.Content); i += 2 {
		if n.Content[i].Value == key {
			n.Content = append(n.Content[:i], n.Content[i+2:]...)
			return
		}
	}
}
func TestUpstreamDeploymentPath(t *testing.T) {
	for _, upstream := range []string{"/openai/v1/responses", "/custom/responses", "/vendor/%2f/%E4%B8%AD/responses", "/custom/%3F/%23/responses"} {
		t.Run(upstream, func(t *testing.T) {
			source := strings.Replace(string(Example()), "path_template: /v1/responses", "path_template: '"+upstream+"'", 1)
			snap := parse(t, source, Options{})
			if got := snap.Providers()["example-provider"].Endpoints["responses"].PathTemplate; got != upstream {
				t.Fatalf("上游转义发生变化：%q", got)
			}
		})
	}
	for _, upstream := range []string{"custom/responses", "https://host/responses", "//host/responses", "/responses?", "/responses?x=1", "/responses#", "/responses#fragment", "/responses%zz", "/responses/%0a", "/responses/{model}", "/responses/{other}", "/responses\n"} {
		t.Run("invalid_"+upstream, func(t *testing.T) {
			encoded, err := json.Marshal(upstream)
			if err != nil {
				t.Fatal(err)
			}
			source := strings.Replace(string(Example()), "path_template: /v1/responses", "path_template: "+strings.TrimSpace(string(encoded)), 1)
			if _, err := loadBytes(context.Background(), "fixture.yaml", []byte(source), Options{}); err == nil || !strings.Contains(err.Error(), "path_template") {
				t.Fatalf("无效上游路径被接受：%v", err)
			}
		})
	}
	c := compiler{file: "fixture.yaml"}
	for _, upstream := range []string{"/vendor/models/{model}:generateContent", "/custom/{model}:generateContent"} {
		if err := c.upstreamPath(upstream, "gemini.generate_content", "generate_content", "$.path_template"); err != nil {
			t.Fatal(err)
		}
	}
	for _, upstream := range []string{"/models/{model}/{model}:generateContent", "/models/{other}:generateContent", "/models/{model}/generateContent", "/models/pre{model}:generateContent", "/models/{model}:generateContent/extra", "/models/{model}:streamGenerateContent"} {
		if err := c.upstreamPath(upstream, "gemini.generate_content", "generate_content", "$.path_template"); err == nil {
			t.Fatalf("无效模型占位符被接受：%s", upstream)
		}
	}
}
func TestOriginPortAndDelimiterValidation(t *testing.T) {
	for _, origin := range []string{"https://host?", "https://host#", "https://host?#", "https://host:0", "https://host:65536", "https://host:", "https://[::1]:0", "https://host/path"} {
		t.Run(origin, func(t *testing.T) {
			encoded, _ := json.Marshal(origin)
			source := strings.Replace(string(Example()), "https://api.example.com", string(encoded), 1)
			if _, err := loadBytes(context.Background(), "fixture.yaml", []byte(source), Options{}); err == nil || !strings.Contains(err.Error(), "origin") {
				t.Fatalf("无效 origin 被接受：%v", err)
			}
		})
	}
	for _, origin := range []string{"https://host:443", "http://127.0.0.1:1", "https://[::1]:65535", "https://api.example.com/"} {
		t.Run(origin, func(t *testing.T) {
			source := strings.Replace(string(Example()), "https://api.example.com", origin, 1)
			snap := parse(t, source, Options{})
			got := snap.Providers()["example-provider"].Origin
			if got != strings.TrimSuffix(origin, "/") {
				t.Fatalf("origin 未规范化：%q", got)
			}
		})
	}
}
