package routing

import (
	"bytes"
	"context"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"relayleaf/internal/protocol"
)

var identityHeaders = map[string]string{"openai_organization": "OpenAI-Organization", "openai_project": "OpenAI-Project", "google_user_project": "X-Goog-User-Project"}

func deleteHeader(h http.Header, name string) {
	for key := range h {
		if strings.EqualFold(key, name) {
			delete(h, key)
		}
	}
}

// BuildRequest 只由配置产生上游 URL，正文仅局部替换模型。
func BuildRequest(ctx context.Context, inbound *http.Request, candidate Candidate, facts protocol.RequestFacts, raw, decoded []byte) (*http.Request, *protocol.GatewayError) {
	_, formatErr := CheckFormat(inbound)
	if formatErr != nil {
		return nil, formatErr
	}
	changed := facts.Model != candidate.Target.UpstreamName
	body := raw
	if changed {
		body = protocol.ReplaceModel(decoded, facts, candidate.Target.UpstreamName)
	}
	u, err := url.Parse(strings.TrimRight(candidate.Provider.Origin, "/") + candidate.Endpoint.PathTemplate)
	if err != nil || u.Host == "" || u.Scheme != "http" && u.Scheme != "https" || u.User != nil || u.Fragment != "" || u.RawQuery != "" {
		return nil, gatewayError(503, "no_available_target", "上游地址配置无效")
	}
	var kept []string
	if inbound.URL != nil && inbound.URL.RawQuery != "" {
		for _, part := range strings.Split(inbound.URL.RawQuery, "&") {
			key, value, _ := strings.Cut(part, "=")
			name, err := url.QueryUnescape(key)
			_, valueErr := url.QueryUnescape(value)
			if err != nil || valueErr != nil || strings.Contains(part, ";") {
				return nil, gatewayError(400, "invalid_request_metadata", "查询参数编码无效")
			}
			_, fixed := candidate.Endpoint.Query[name]
			if name == "key" || fixed || candidate.Endpoint.AuthType == "query" && name == candidate.Endpoint.AuthName {
				continue
			}
			kept = append(kept, part)
		}
	}
	var fixedNames []string
	for name := range candidate.Endpoint.Query {
		fixedNames = append(fixedNames, name)
	}
	sort.Strings(fixedNames)
	for _, name := range fixedNames {
		kept = append(kept, url.QueryEscape(name)+"="+url.QueryEscape(candidate.Endpoint.Query[name]))
	}
	if candidate.Endpoint.AuthType == "query" {
		kept = append(kept, url.QueryEscape(candidate.Endpoint.AuthName)+"="+url.QueryEscape(candidate.Credential.Secret.Reveal()))
	}
	u.RawQuery = strings.Join(kept, "&")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		return nil, gatewayError(503, "no_available_target", "无法构造上游请求")
	}
	req.Header = inbound.Header.Clone()
	for _, value := range headerValues(req.Header, "Connection") {
		for _, name := range strings.Split(value, ",") {
			deleteHeader(req.Header, strings.TrimSpace(name))
		}
	}
	for name := range req.Header {
		lower := strings.ToLower(name)
		if authHeaders[lower] || strings.HasPrefix(lower, "x-relayleaf-") {
			delete(req.Header, name)
		}
	}
	for _, name := range []string{"Connection", "Keep-Alive", "Proxy-Connection", "Proxy-Authorization", "Proxy-Authenticate", "TE", "Trailer", "Transfer-Encoding", "Upgrade", "Cookie", "Host", "Content-Length", "OpenAI-Organization", "OpenAI-Project", "X-Goog-User-Project"} {
		deleteHeader(req.Header, name)
	}
	deleteHeader(req.Header, candidate.Endpoint.AuthName)
	// 正文格式由网关验证；不能让 Connection 的提名破坏其编码声明。
	deleteHeader(req.Header, "Content-Type")
	req.Header.Set("Content-Type", headerValues(inbound.Header, "Content-Type")[0])
	deleteHeader(req.Header, "Content-Encoding")
	if !changed && len(headerValues(inbound.Header, "Content-Encoding")) > 0 {
		req.Header.Set("Content-Encoding", headerValues(inbound.Header, "Content-Encoding")[0])
	}
	if changed {
		for _, name := range []string{"Content-Encoding", "Content-MD5", "Digest", "Content-Digest", "Repr-Digest"} {
			deleteHeader(req.Header, name)
		}
	}
	for name, value := range candidate.Endpoint.Headers {
		deleteHeader(req.Header, name)
		req.Header.Set(name, value)
	}
	for selector, value := range candidate.Credential.Selectors {
		if name, ok := identityHeaders[selector]; ok {
			req.Header.Set(name, value)
		}
	}
	switch candidate.Endpoint.AuthType {
	case "bearer":
		req.Header.Set("Authorization", "Bearer "+candidate.Credential.Secret.Reveal())
	case "header":
		req.Header.Set(candidate.Endpoint.AuthName, candidate.Credential.Secret.Reveal())
	}
	return req, nil
}
