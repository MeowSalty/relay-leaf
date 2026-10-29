package config

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestAccessGroupStrictDeclarations(t *testing.T) {
	base := string(Example())
	declaration := "access_groups:\n  general:\n    name: 普通上游\n"
	for name, replacement := range map[string]string{
		"null": "access_groups: null\n", "sequence": "access_groups: []\n", "scalar": "access_groups: hidden-value\n",
		"invalid_id": "access_groups: {'bad/id': {}}\n", "duplicate_id": "access_groups: {general: {}, general: {}}\n",
		"null_object": "access_groups: {general: null}\n", "unknown_field": "access_groups: {general: {unknown: hidden-value}}\n",
		"non_string_name": "access_groups: {general: {name: 123}}\n", "null_name": "access_groups: {general: {name: null}}\n",
	} {
		t.Run(name, func(t *testing.T) {
			assertGroupError(t, strings.Replace(base, declaration, replacement, 1), "$.access_groups")
		})
	}
	assertGroupError(t, strings.Replace(base, declaration, "", 1), "$.providers.example-provider.credentials.example-key.access_groups")
	assertGroupError(t, strings.Replace(base, declaration, "access_groups: {}\n", 1), "$.providers.example-provider.credentials.example-key.access_groups")
	minimal := strings.Replace(base, "providers:\n", "providers: {}\nunused_providers:\n", 1)
	// 仅保留不依赖访问组定义的空配置图。
	minimal = minimal[:strings.Index(minimal, "unused_providers:")] + "routes: {}\n" + base[strings.Index(base, "execution_defaults:"):]
	start := strings.Index(minimal, "  keys:\n")
	end := strings.Index(minimal, "providers: {}")
	minimal = minimal[:start] + "  keys: {}\n" + minimal[end:]
	for _, value := range []string{"", "access_groups: {}\n"} {
		s := parse(t, strings.Replace(minimal, declaration, value, 1), Options{})
		if s.AccessGroups() == nil || len(s.AccessGroups()) != 0 {
			t.Fatal("省略或空访问组没有编译为空映射")
		}
	}
}

func assertGroupError(t *testing.T, source, path string) {
	t.Helper()
	options := testOptions()
	options.LookupEnv = func(string) (string, bool) {
		t.Fatal("访问组图校验完成之前读取了秘密")
		return "", false
	}
	_, err := loadBytes(context.Background(), "groups.yaml", []byte(source), options)
	var diagnostic *Error
	if !errors.As(err, &diagnostic) || !strings.HasPrefix(diagnostic.Path, path) || diagnostic.Line <= 0 || diagnostic.Column <= 0 {
		t.Fatalf("访问组错误缺少对应路径与位置：%v", err)
	}
	if strings.Contains(err.Error(), "hidden-value") {
		t.Fatal("访问组错误回显了字段值")
	}
}

func TestAccessGroupReferenceValidation(t *testing.T) {
	locations := []struct{ old, path string }{
		{"        access_groups: [general]\n", "$.providers.example-provider.credentials.example-key.access_groups"},
		{"      allowed_access_groups: [general]\n", "$.client_auth.keys.local-client.allowed_access_groups"},
		{"      denied_access_groups: []\n", "$.client_auth.keys.local-client.denied_access_groups"},
	}
	for _, location := range locations {
		field := strings.TrimSpace(strings.Split(location.old, ":")[0])
		indent := location.old[:strings.Index(location.old, field)]
		for _, state := range []string{"enabled", "disabled_credential", "disabled_client", "expired_client"} {
			base := string(Example())
			switch state {
			case "disabled_credential":
				base = strings.Replace(base, "      example-key:\n        enabled: true", "      example-key:\n        enabled: false", 1)
			case "disabled_client":
				base = strings.Replace(base, "    local-client:\n      enabled: true", "    local-client:\n      enabled: false", 1)
			case "expired_client":
				base = strings.Replace(base, "    local-client:", "    local-client:\n      expires_at: '2026-10-27T00:00:00Z'", 1)
			}
			for name, value := range map[string]string{
				"null": "null", "wrong_type": "general", "non_string": "[123]", "mapping_member": "[{name: hidden-value}]",
				"duplicate": "[general, general]", "unknown": "[missing]", "case_sensitive": "[General]", "wildcard": "['*']", "invalid_id": "['bad/id']",
			} {
				t.Run(field+"/"+state+"/"+name, func(t *testing.T) {
					assertGroupError(t, strings.Replace(base, location.old, indent+field+": "+value+"\n", 1), location.path)
				})
			}
			if field != "denied_access_groups" {
				t.Run(field+"/"+state+"/missing", func(t *testing.T) {
					assertGroupError(t, strings.Replace(base, location.old, "", 1), location.path)
				})
			}
			if field == "access_groups" {
				t.Run(field+"/"+state+"/empty", func(t *testing.T) {
					assertGroupError(t, strings.Replace(base, location.old, indent+field+": []\n", 1), location.path)
				})
			}
		}
	}
}

func TestAccessGroupEmptyAllowAndDenyOverlap(t *testing.T) {
	base := string(Example())
	for _, denied := range []string{"", "      denied_access_groups: []\n", "      denied_access_groups: [general]\n"} {
		for _, allowed := range []string{"[]", "[general]"} {
			source := strings.Replace(base, "      denied_access_groups: []\n", denied, 1)
			source = strings.Replace(source, "allowed_access_groups: [general]", "allowed_access_groups: "+allowed, 1)
			snapshot := parse(t, source, testOptions())
			key, ok := snapshot.Authenticate(validClient, testOptions().Now())
			if !ok || key.DeniedAccessGroups == nil || allowed == "[]" && len(key.AllowedAccessGroups) != 0 {
				t.Fatal("空允许列表、默认拒绝列表或允许拒绝重叠未被保留")
			}
		}
	}
	base = strings.Replace(base, "allowed_access_groups: [general]", "allowed_access_groups: [missing]", 1)
	base = strings.Replace(base, "denied_access_groups: []", "denied_access_groups: [missing]", 1)
	assertGroupError(t, base, "$.client_auth.keys.local-client.allowed_access_groups")
}
