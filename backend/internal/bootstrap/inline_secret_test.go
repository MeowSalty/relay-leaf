package bootstrap

import (
	"crypto/sha256"
	"encoding/hex"
	"net"
	"strings"
	"testing"

	"relayleaf/internal/config"
)

const inlineClientKey = "inline-client-private-0123456789abcdef"
const inlineUpstreamKey = "inline-upstream-private-material"

func inlineCLIConfig() string {
	source := strings.Replace(string(config.Example()), "secret_ref: env:RELAYLEAF_CLIENT_KEY", "secret: '"+inlineClientKey+"'", 1)
	return strings.Replace(source, "secret_ref: env:RELAYLEAF_UPSTREAM_KEY", "secret: '"+inlineUpstreamKey+"'", 1)
}

func assertInlineCLIOutputRedacted(t *testing.T, output string) {
	t.Helper()
	digest := sha256.Sum256([]byte(inlineClientKey))
	for _, material := range []string{inlineClientKey, inlineUpstreamKey, hex.EncodeToString(digest[:])} {
		if strings.Contains(output, material) {
			t.Fatal("CLI 输出泄露了内联密钥或客户端摘要")
		}
	}
}

func TestCLIInlineSecretChecksWithoutEnvironment(t *testing.T) {
	t.Setenv("RELAYLEAF_CLIENT_KEY", "")
	t.Setenv("RELAYLEAF_UPSTREAM_KEY", "")
	path := writeConfig(t, inlineCLIConfig())
	for _, resolve := range []bool{false, true} {
		args := []string{"config", "check", "--config", path}
		status := "secrets_resolved=false"
		if resolve {
			args = append(args, "--resolve-secrets")
			status = "secrets_resolved=true"
		}
		code, out, errs := command(t, args...)
		if code != 0 || errs != "" || !strings.Contains(out, status) {
			t.Fatalf("内联配置检查未返回预期状态：%d %s %s", code, out, errs)
		}
		assertInlineCLIOutputRedacted(t, out+errs)
	}
}

func TestCLIInlineSecretValidationErrorsAreRedacted(t *testing.T) {
	for name, source := range map[string]string{
		"source_conflict":  strings.Replace(inlineCLIConfig(), "      secret: '"+inlineClientKey+"'", "      secret: '"+inlineClientKey+"'\n      secret_ref: env:RELAYLEAF_CLIENT_KEY", 1),
		"invalid_client":   strings.Replace(inlineCLIConfig(), inlineClientKey, inlineClientKey+" ", 1),
		"invalid_upstream": strings.Replace(inlineCLIConfig(), inlineUpstreamKey, inlineUpstreamKey+" ", 1),
	} {
		t.Run(name, func(t *testing.T) {
			path := writeConfig(t, source)
			for _, resolve := range []bool{false, true} {
				args := []string{"config", "check", "--config", path}
				if resolve {
					args = append(args, "--resolve-secrets")
				}
				code, out, errs := command(t, args...)
				pathPrefix := "$.client_auth.keys.local-client"
				if name == "invalid_upstream" {
					pathPrefix = "$.providers.example-provider.credentials.example-key.secret"
				}
				if code != 1 || !strings.Contains(errs, pathPrefix) {
					t.Fatalf("内联配置错误未报告对应路径：%d %s", code, errs)
				}
				assertInlineCLIOutputRedacted(t, out+errs)
			}
		})
	}
}

func TestCLIInlineSecretStartupReachesListener(t *testing.T) {
	t.Setenv("RELAYLEAF_CLIENT_KEY", "")
	t.Setenv("RELAYLEAF_UPSTREAM_KEY", "")
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = occupied.Close() })
	source := strings.Replace(inlineCLIConfig(), "127.0.0.1:17890", occupied.Addr().String(), 1)
	code, out, errs := command(t, "local", "--config", writeConfig(t, source))
	if code != 1 || !strings.Contains(errs, "绑定失败") {
		t.Fatalf("有效内联配置未通过启动配置初始化：%d %s", code, errs)
	}
	assertInlineCLIOutputRedacted(t, out+errs)
}
