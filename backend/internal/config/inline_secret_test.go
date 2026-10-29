package config

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const inlineUpstream = "inline-upstream-private-material"

func inlineQuoted(t *testing.T, value string) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return strings.ReplaceAll(string(encoded), "\x7f", `\u007f`)
}

func inlineConfig(t *testing.T) string {
	t.Helper()
	source := strings.Replace(string(Example()), "secret_ref: env:RELAYLEAF_CLIENT_KEY", "secret: "+inlineQuoted(t, validClient), 1)
	return strings.Replace(source, "secret_ref: env:RELAYLEAF_UPSTREAM_KEY", "secret: "+inlineQuoted(t, inlineUpstream), 1)
}

func TestInlineSecretCheckAndResolution(t *testing.T) {
	for _, resolve := range []bool{false, true} {
		t.Run(fmt.Sprint(resolve), func(t *testing.T) {
			options := testOptions()
			options.ResolveSecrets = resolve
			options.LookupEnv = func(string) (string, bool) {
				t.Fatal("纯内联配置读取了环境变量")
				return "", false
			}
			snapshot := parse(t, inlineConfig(t), options)
			if snapshot.Summary().SecretsResolved != resolve {
				t.Fatal("解析状态与检查选项不一致")
			}
			_, authenticated := snapshot.Authenticate(validClient, options.Now())
			if authenticated != resolve {
				t.Fatal("普通检查建立了客户端鉴权材料，或解析检查未建立鉴权材料")
			}
			material := snapshot.Providers()["example-provider"].Credentials["example-key"].Secret.Reveal()
			if resolve && material != inlineUpstream || !resolve && material != "" {
				t.Fatal("上游运行材料与解析选项不一致")
			}
		})
	}
}

func TestInlineSecretLiteralHeaderValues(t *testing.T) {
	for _, value := range []string{"env:MISSING_INLINE_ENV", "file:missing-inline.key", "  ${MISSING_INLINE_ENV}  ", "中文上游材料"} {
		t.Run(value, func(t *testing.T) {
			source := strings.Replace(inlineConfig(t), inlineQuoted(t, inlineUpstream), inlineQuoted(t, value), 1)
			source = strings.Replace(source, "auth: {type: bearer}", "auth: {type: header, name: X-API-Key}", 1)
			options := testOptions()
			options.LookupEnv = func(string) (string, bool) {
				t.Fatal("内联字符串被当作外部引用解析")
				return "", false
			}
			snapshot := parse(t, source, options)
			if snapshot.Providers()["example-provider"].Credentials["example-key"].Secret.Reveal() != value {
				t.Fatal("内联字符串被裁剪、替换或解析")
			}
		})
	}
	for _, value := range []string{"a", strings.Repeat("a", MaxSecretBytes)} {
		source := strings.Replace(inlineConfig(t), inlineQuoted(t, inlineUpstream), inlineQuoted(t, value), 1)
		parse(t, source, testOptions())
	}
	for _, length := range []int{32, 512} {
		value := strings.Repeat("c", length)
		source := strings.Replace(inlineConfig(t), inlineQuoted(t, validClient), inlineQuoted(t, value), 1)
		snapshot := parse(t, source, testOptions())
		if _, ok := snapshot.Authenticate(value, testOptions().Now()); !ok {
			t.Fatal("有效长度边界的客户端密钥未通过鉴权")
		}
	}
}

func TestInlineSecretSourceStructure(t *testing.T) {
	locations := []struct {
		name, old, indent, path string
	}{
		{"client", "secret_ref: env:RELAYLEAF_CLIENT_KEY", "      ", "$.client_auth.keys.local-client"},
		{"credential", "secret_ref: env:RELAYLEAF_UPSTREAM_KEY", "        ", "$.providers.example-provider.credentials.example-key"},
		{"retiring", "secret_ref: env:INLINE_OLD", "        ", "$.client_auth.keys.local-client.retiring_secret"},
	}
	for _, location := range locations {
		t.Run(location.name, func(t *testing.T) {
			base := string(Example())
			if location.name == "retiring" {
				base = strings.Replace(base, "      secret_version: v1", "      retiring_secret:\n        secret_ref: env:INLINE_OLD\n        secret_version: old\n        valid_until: '2026-10-29T00:00:00Z'\n      secret_version: v1", 1)
			}
			for name, field := range map[string]string{
				"empty": "secret: ''", "null": "secret: null", "implicit_null": "secret:",
				"integer": "secret: 123", "boolean": "secret: true", "mapping": "secret: {private: hidden-inline-material}", "sequence": "secret: [hidden-inline-material]",
				"both":              "secret: " + inlineQuoted(t, validClient) + "\n" + location.indent + location.old,
				"both_empty_ref":    "secret: " + inlineQuoted(t, validClient) + "\n" + location.indent + "secret_ref: ''",
				"both_null_ref":     "secret: " + inlineQuoted(t, validClient) + "\n" + location.indent + "secret_ref: null",
				"both_empty_inline": "secret: ''\n" + location.indent + location.old,
				"both_null_inline":  "secret: null\n" + location.indent + location.old,
			} {
				t.Run(name, func(t *testing.T) {
					source := strings.Replace(base, location.old, field, 1)
					assertInlineError(t, source, location.path, "hidden-inline-material", validClient)
				})
			}
		})
	}
}

func assertInlineError(t *testing.T, source, path string, materials ...string) {
	t.Helper()
	options := testOptions()
	options.ResolveSecrets = false
	options.LookupEnv = func(string) (string, bool) {
		t.Fatal("普通检查读取了外部秘密")
		return "", false
	}
	_, err := loadBytes(context.Background(), "inline-fixture.yaml", []byte(source), options)
	var diagnostic *Error
	if !errors.As(err, &diagnostic) || !strings.HasPrefix(diagnostic.Path, path) || diagnostic.Line <= 0 || diagnostic.Column <= 0 {
		t.Fatalf("无效内联配置缺少对应路径的诊断：%v", err)
	}
	if strings.Contains(diagnostic.Message, "未知字段") {
		t.Fatal("内联秘密字段未被识别，样本没有到达待测行为")
	}
	for _, material := range materials {
		if material != "" && strings.Contains(err.Error(), material) {
			t.Fatal("内联校验错误回显了秘密材料")
		}
	}
}

func TestInlineSecretInvalidMaterialsInOrdinaryCheck(t *testing.T) {
	for name, value := range map[string]string{
		"short": "tiny-private-material", "overlong": strings.Repeat("c", 513), "non_ascii": validClient + "中",
		"equal_inside": validClient + "=x", "leading_space": " " + validClient, "trailing_space": validClient + " ",
		"newline": validClient + "\n", "tab": validClient + "\t", "control": validClient + "\x7f",
	} {
		t.Run(name, func(t *testing.T) {
			source := strings.Replace(inlineConfig(t), inlineQuoted(t, validClient), inlineQuoted(t, value), 1)
			assertInlineError(t, source, "$.client_auth.keys.local-client.secret", value)
		})
	}
	for name, value := range map[string]string{"control": "private\nmaterial", "too_large": strings.Repeat("x", MaxSecretBytes+1), "bearer_whitespace": "private material"} {
		t.Run("upstream_"+name, func(t *testing.T) {
			source := strings.Replace(inlineConfig(t), inlineQuoted(t, inlineUpstream), inlineQuoted(t, value), 1)
			assertInlineError(t, source, "$.providers.example-provider.credentials.example-key.secret", value)
		})
	}
	for name, source := range map[string]string{
		"disabled_client": strings.Replace(inlineConfig(t), "    local-client:\n      enabled: true", "    local-client:\n      enabled: false", 1),
		"expired_client":  strings.Replace(inlineConfig(t), "    local-client:", "    local-client:\n      expires_at: '2026-10-28T00:00:00Z'", 1),
	} {
		t.Run(name, func(t *testing.T) {
			source = strings.Replace(source, inlineQuoted(t, validClient), "'hidden invalid inline value'", 1)
			assertInlineError(t, source, "$.client_auth.keys.local-client.secret", "hidden invalid inline value")
		})
	}
	for _, deadline := range []string{"2026-10-28T00:00:00Z", "2026-10-29T00:00:00Z"} {
		source := strings.Replace(inlineConfig(t), "      secret_version: v1", "      retiring_secret: {secret: tiny-private-material, secret_version: old, valid_until: '"+deadline+"'}\n      secret_version: v1", 1)
		assertInlineError(t, source, "$.client_auth.keys.local-client.retiring_secret.secret", "tiny-private-material")
	}
	for _, mutation := range []struct{ old, new string }{
		{"  example-provider:\n    enabled: true", "  example-provider:\n    enabled: false"},
		{"      example-key:\n        enabled: true", "      example-key:\n        enabled: false"},
	} {
		for _, value := range []string{"private\nmaterial", "private invalid bearer"} {
			source := strings.Replace(inlineConfig(t), mutation.old, mutation.new, 1)
			source = strings.Replace(source, "          responses:\n            enabled: true", "          responses:\n            enabled: false", 1)
			source = strings.Replace(source, "  example-responses:\n    protocol:", "  example-responses:\n    enabled: false\n    protocol:", 1)
			source = strings.Replace(source, inlineQuoted(t, inlineUpstream), inlineQuoted(t, value), 1)
			assertInlineError(t, source, "$.providers.example-provider.credentials.example-key.secret", value)
		}
	}
}

func TestInlineSecretVersionsAndNoAuthentication(t *testing.T) {
	base := inlineConfig(t)
	for name, source := range map[string]string{
		"missing_client_source":     strings.Replace(base, "      secret: "+inlineQuoted(t, validClient)+"\n", "", 1),
		"missing_client_version":    strings.Replace(base, "      secret_version: v1\n", "", 1),
		"missing_upstream_version":  strings.Replace(base, "        secret_version: v1\n", "", 1),
		"missing_required_upstream": strings.Replace(strings.Replace(base, "        secret: "+inlineQuoted(t, inlineUpstream)+"\n", "", 1), "        secret_version: v1\n", "", 1),
		"reserved_upstream_version": strings.Replace(base, "        secret_version: v1", "        secret_version: none", 1),
		"retiring_same_version":     strings.Replace(base, "      secret_version: v1", "      retiring_secret: {secret: "+inlineQuoted(t, strings.Repeat("r", 32))+", secret_version: v1, valid_until: '2026-10-29T00:00:00Z'}\n      secret_version: v1", 1),
	} {
		t.Run(name, func(t *testing.T) {
			assertInlineError(t, source, "$")
		})
	}
	none := strings.Replace(base, "auth: {type: bearer}", "auth: {type: none}", 1)
	none = strings.Replace(none, "        secret: "+inlineQuoted(t, inlineUpstream)+"\n", "", 1)
	assertInlineError(t, none, "$.providers.example-provider.credentials.example-key.secret_version")
	none = strings.Replace(none, "        secret_version: v1\n", "", 1)
	snapshot := parse(t, none, testOptions())
	credential := snapshot.Providers()["example-provider"].Credentials["example-key"]
	if credential.Secret.Reveal() != "" || credential.Version != "none" {
		t.Fatal("无认证凭据没有保留无材料的运行状态")
	}
}

func TestInlineSecretResolutionRespectsInactiveObjects(t *testing.T) {
	for name, mutation := range map[string]struct {
		old, new string
		client   bool
	}{
		"disabled_client":     {"    local-client:\n      enabled: true", "    local-client:\n      enabled: false", true},
		"expired_client":      {"    local-client:", "    local-client:\n      expires_at: '2026-10-28T00:00:00Z'", true},
		"disabled_provider":   {"  example-provider:\n    enabled: true", "  example-provider:\n    enabled: false", false},
		"disabled_credential": {"      example-key:\n        enabled: true", "      example-key:\n        enabled: false", false},
		"none_auth":           {"auth: {type: bearer}", "auth: {type: none}", false},
	} {
		t.Run(name, func(t *testing.T) {
			source := strings.Replace(inlineConfig(t), mutation.old, mutation.new, 1)
			if name == "disabled_provider" || name == "disabled_credential" {
				source = strings.Replace(source, "          responses:\n            enabled: true", "          responses:\n            enabled: false", 1)
				source = strings.Replace(source, "  example-responses:\n    protocol:", "  example-responses:\n    enabled: false\n    protocol:", 1)
			}
			snapshot := parse(t, source, testOptions())
			_, authenticated := snapshot.Authenticate(validClient, testOptions().Now())
			if authenticated == mutation.client {
				t.Fatal("客户端启用或过期规则未应用")
			}
			if !mutation.client && snapshot.Providers()["example-provider"].Credentials["example-key"].Secret.Reveal() != "" {
				t.Fatal("无需认证材料的上游仍保留内联运行材料")
			}
		})
	}
	oldKey := strings.Repeat("r", 32)
	source := strings.Replace(inlineConfig(t), "      secret_version: v1", "      retiring_secret: {secret: "+inlineQuoted(t, oldKey)+", secret_version: old, valid_until: '2026-10-28T00:00:00Z'}\n      secret_version: v1", 1)
	snapshot := parse(t, source, testOptions())
	if _, ok := snapshot.Authenticate(oldKey, testOptions().Now().Add(-time.Second)); ok {
		t.Fatal("已经退役的内联密钥仍进入了鉴权索引")
	}
}

func TestInlineSecretMixedSourcesAndRetiringDeadline(t *testing.T) {
	oldKey := strings.Repeat("r", 32)
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "old.key"), []byte(oldKey+"\r\n"), 0600); err != nil {
		t.Fatal(err)
	}
	source := strings.Replace(string(Example()), "secret_ref: env:RELAYLEAF_CLIENT_KEY", "secret: "+inlineQuoted(t, validClient), 1)
	source = strings.Replace(source, "      secret_version: v1", "      retiring_secret: {secret_ref: file:old.key, secret_version: old, valid_until: '2026-10-29T00:00:00Z'}\n      secret_version: v1", 1)
	options := testOptions()
	options.LookupEnv = func(key string) (string, bool) {
		if key != "RELAYLEAF_UPSTREAM_KEY" {
			t.Fatal("内联客户端材料读取了环境变量")
		}
		return inlineUpstream, true
	}
	snapshot, err := loadBytes(context.Background(), filepath.Join(directory, "relayleaf.yaml"), []byte(source), options)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC)
	if _, ok := snapshot.Authenticate(oldKey, deadline.Add(-time.Nanosecond)); !ok {
		t.Fatal("退役密钥在有效期内被拒绝")
	}
	if _, ok := snapshot.Authenticate(oldKey, deadline); ok {
		t.Fatal("退役密钥在期限到达后仍被接受")
	}
	if _, ok := snapshot.Authenticate(validClient, deadline); !ok {
		t.Fatal("当前内联密钥被退役期限影响")
	}
	options.ResolveSecrets = false
	options.LookupEnv = func(string) (string, bool) {
		t.Fatal("混合来源的普通检查读取了环境变量")
		return "", false
	}
	if err := os.Remove(filepath.Join(directory, "old.key")); err != nil {
		t.Fatal(err)
	}
	parse(t, source, options)
}

func TestInlineSecretRetiringMaterialAndClientExpiration(t *testing.T) {
	oldKey := strings.Repeat("r", 32)
	source := strings.Replace(string(Example()), "      secret_version: v1", "      expires_at: '2026-10-28T12:00:00Z'\n      retiring_secret: {secret: "+inlineQuoted(t, oldKey)+", secret_version: old, valid_until: '2026-10-29T00:00:00Z'}\n      secret_version: v1", 1)
	snapshot := parse(t, source, testOptions())
	expires := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	for _, material := range []string{validClient, oldKey} {
		if _, ok := snapshot.Authenticate(material, expires.Add(-time.Nanosecond)); !ok {
			t.Fatal("当前或退役密钥在客户端过期前被拒绝")
		}
		if _, ok := snapshot.Authenticate(material, expires); ok {
			t.Fatal("客户端过期后仍接受当前或退役密钥")
		}
	}
}

func TestInlineSecretDuplicateAcrossSources(t *testing.T) {
	for _, reference := range []string{"env:RELAYLEAF_CLIENT_KEY", "file:duplicate.key"} {
		t.Run(reference, func(t *testing.T) {
			directory := t.TempDir()
			if err := os.WriteFile(filepath.Join(directory, "duplicate.key"), []byte(validClient), 0600); err != nil {
				t.Fatal(err)
			}
			for _, secondSource := range []string{
				strings.Replace(inlineConfig(t), "      secret_version: v1", "      retiring_secret: {secret_ref: "+reference+", secret_version: old, valid_until: '2026-10-29T00:00:00Z'}\n      secret_version: v1", 1),
				strings.Replace(inlineConfig(t), "providers:\n", "    second-client:\n      secret_ref: "+reference+"\n      secret_version: v1\n      allowed_routes: [example-responses]\nproviders:\n", 1),
			} {
				_, err := loadBytes(context.Background(), filepath.Join(directory, "relayleaf.yaml"), []byte(secondSource), testOptions())
				if err == nil || !strings.Contains(err.Error(), "重复") || strings.Contains(err.Error(), validClient) {
					t.Fatalf("跨来源重复密钥未被安全拒绝：%v", err)
				}
			}
		})
	}
}

func TestInlineSecretSnapshotAndProviderRedaction(t *testing.T) {
	snapshot := parse(t, inlineConfig(t), testOptions())
	digest := sha256.Sum256([]byte(validClient))
	for _, value := range []any{snapshot, *snapshot, snapshot.Providers(), snapshot.Providers()["example-provider"], snapshot.Providers()["example-provider"].Credentials["example-key"]} {
		outputs := []string{}
		for _, format := range []string{"%v", "%+v", "%#v"} {
			outputs = append(outputs, fmt.Sprintf(format, value))
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		outputs = append(outputs, string(encoded))
		for _, output := range outputs {
			for _, material := range []string{validClient, inlineUpstream, hex.EncodeToString(digest[:]), fmt.Sprintf("%v", digest), fmt.Sprintf("%#v", digest)} {
				if strings.Contains(output, material) {
					t.Fatal("快照或上游对象输出泄露了内联材料或客户端摘要")
				}
			}
		}
	}
}
