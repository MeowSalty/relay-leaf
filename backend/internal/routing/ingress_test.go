package routing

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"relayleaf/internal/catalog"
)

func TestEndpointRequiresExactPathAndMethod(t *testing.T) {
	for _, tc := range []struct {
		method, path string
		status       int
	}{
		{"POST", "/v1/responses", 0}, {"GET", "/v1/responses", 405},
		{"POST", "/v1/responses/", 404}, {"POST", "/v1/%72esponses", 404},
		{"POST", "/v1/responses/compact", 404}, {"GET", "/v1/models", 404},
	} {
		err := CheckEndpoint(httptest.NewRequest(tc.method, tc.path, nil))
		if tc.status == 0 && err != nil || tc.status != 0 && (err == nil || err.Status != tc.status) {
			t.Errorf("%s %s 的入口判定错误：%v", tc.method, tc.path, err)
		}
	}
}

func TestAuthenticationCountsAllCarriersBeforeValidation(t *testing.T) {
	token := strings.Repeat("k", 32)
	now := time.Now()
	snapshot := catalog.NewSnapshot(catalog.Data{ClientKeys: map[string]catalog.ClientKey{"c": {ID: "c", Enabled: true}}, Verifications: []catalog.Verification{{ClientID: "c", Material: catalog.NewSecret(token)}}})
	for _, tc := range []struct {
		name, query string
		header      http.Header
		code        string
	}{
		{"正常", "", http.Header{"Authorization": {"Bearer " + token}}, ""},
		{"大小写和空格", "", http.Header{"authorization": {" \tbEaReR   " + token + "\t "}}, ""},
		{"未提供", "", nil, "invalid_client_key"},
		{"材料不匹配", "", http.Header{"Authorization": {"Bearer " + strings.Repeat("x", 32)}}, "invalid_client_key"},
		{"重复头", "", http.Header{"Authorization": {"", "Bearer " + token}}, "ambiguous_client_auth"},
		{"头名大小写重复", "", http.Header{"Authorization": {"Bearer " + token}, "authorization": {"invalid"}}, "ambiguous_client_auth"},
		{"逗号合并", "", http.Header{"Authorization": {"Bearer " + token + ", Bearer " + token}}, "ambiguous_client_auth"},
		{"不同头载体", "", http.Header{"Authorization": {"bad"}, "X-Api-Key": {"bad"}}, "ambiguous_client_auth"},
		{"禁止的载体", "", http.Header{"X-Goog-Api-Key": {token}}, "invalid_client_auth"},
		{"查询载体", "key=" + token, nil, "invalid_client_auth"},
		{"转义重复查询", "key=x&%6bey=y", nil, "ambiguous_client_auth"},
		{"空查询也计数", "key&key=", nil, "ambiguous_client_auth"},
		{"头与查询", "key=%zz", http.Header{"Authorization": {"Bearer " + token}}, "ambiguous_client_auth"},
		{"分号身份", "key=x;key=y", nil, "invalid_client_auth"},
		{"查询大小写不同", "KEY=ordinary", http.Header{"Authorization": {"Bearer " + token}}, ""},
		{"查询只解码一次", "%256bey=ordinary", http.Header{"Authorization": {"Bearer " + token}}, ""},
		{"无效查询名称", "%zz=x", http.Header{"Authorization": {"Bearer " + token}}, "invalid_client_auth"},
		{"空Bearer", "", http.Header{"Authorization": {"Bearer "}}, "invalid_client_auth"},
		{"错误scheme", "", http.Header{"Authorization": {"Basic " + token}}, "invalid_client_auth"},
		{"token中空格", "", http.Header{"Authorization": {"Bearer " + token + " x"}}, "invalid_client_auth"},
		{"scheme后tab", "", http.Header{"Authorization": {"Bearer\t" + token}}, "invalid_client_auth"},
		{"管理Cookie忽略", "", http.Header{"Authorization": {"Bearer " + token}, "Cookie": {"admin=opaque"}}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/v1/responses?"+tc.query, nil)
			req.Header = tc.header
			key, err := Authenticate(req, snapshot, now)
			if tc.code == "" {
				if err != nil || key.ID != "c" {
					t.Fatalf("合法认证失败：%v", err)
				}
			} else if err == nil || err.Code != tc.code {
				t.Fatalf("认证错误码=%v，预期 %s", err, tc.code)
			}
		})
	}
}

func TestAuthenticationExpiryAndRetiringMaterial(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name           string
		enabled        bool
		expires, until time.Time
		valid          bool
	}{
		{"有效", true, now.Add(time.Hour), now.Add(time.Minute), true},
		{"已禁用", false, time.Time{}, time.Time{}, false},
		{"密钥到期", true, now, time.Time{}, false},
		{"轮换材料到期", true, time.Time{}, now, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			token := strings.Repeat("r", 32)
			snapshot := catalog.NewSnapshot(catalog.Data{ClientKeys: map[string]catalog.ClientKey{"c": {ID: "c", Enabled: tc.enabled, ExpiresAt: tc.expires}}, Verifications: []catalog.Verification{{ClientID: "c", Material: catalog.NewSecret(token), ValidUntil: tc.until}}})
			req := httptest.NewRequest("POST", "/v1/responses", nil)
			req.Header.Set("Authorization", "Bearer "+token)
			_, err := Authenticate(req, snapshot, now)
			if (err == nil) != tc.valid || err != nil && err.Code != "invalid_client_key" {
				t.Fatalf("有效期判定错误：%v", err)
			}
		})
	}
}

func TestFormatAcceptsOnlySingleSupportedJSONEncoding(t *testing.T) {
	for _, tc := range []struct {
		name             string
		types, encodings []string
		want             string
	}{
		{"JSON", []string{"application/json"}, nil, "identity"},
		{"扩展JSON", []string{"application/problem+json; charset=UTF-8"}, []string{"GZIP"}, "gzip"},
		{"identity", []string{"application/json; charset=utf-8"}, []string{"identity"}, "identity"},
		{"无媒体类型", nil, nil, ""}, {"错误媒体类型", []string{"text/json"}, nil, ""},
		{"重复类型", []string{"application/json", "application/json"}, nil, ""},
		{"错误字符集", []string{"application/json; charset=gbk"}, nil, ""},
		{"空后缀", []string{"application/+json"}, nil, ""},
		{"多编码", []string{"application/json"}, []string{"gzip, identity"}, ""},
		{"重复编码", []string{"application/json"}, []string{"gzip", "gzip"}, ""},
		{"未知编码", []string{"application/json"}, []string{"br"}, ""},
		{"空编码", []string{"application/json"}, []string{""}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/v1/responses", nil)
			if tc.types != nil {
				req.Header["Content-Type"] = tc.types
			}
			if tc.encodings != nil {
				req.Header["Content-Encoding"] = tc.encodings
			}
			got, err := CheckFormat(req)
			if tc.want == "" {
				if err == nil || err.Status != 415 {
					t.Fatalf("不支持格式未拒绝：%v", err)
				}
			} else if err != nil || got != tc.want {
				t.Fatalf("合法格式得到 %q：%v", got, err)
			}
		})
	}
}
