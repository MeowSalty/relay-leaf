package routing

import (
	"mime"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"relayleaf/internal/catalog"
	"relayleaf/internal/protocol"
)

func gatewayError(status int, code, message string) *protocol.GatewayError {
	return &protocol.GatewayError{Status: status, Code: code, Message: message}
}

// CheckEndpoint 只接受声明的 Responses 创建入口，不归一化路径。
func CheckEndpoint(r *http.Request) *protocol.GatewayError {
	if r.URL == nil || r.URL.EscapedPath() != "/v1/responses" {
		return gatewayError(404, "route_not_found", "请求路径未配置")
	}
	if r.Method != http.MethodPost {
		return gatewayError(405, "method_not_allowed", "此入口仅接受 POST")
	}
	return nil
}

var authHeaders = map[string]bool{"authorization": true, "x-api-key": true, "api-key": true, "x-goog-api-key": true}
var bearerToken = regexp.MustCompile(`^[A-Za-z0-9._~+/-]+=*$`)

// Authenticate 先统计所有保留载体，再检查材料，不读取正文。
func Authenticate(r *http.Request, snapshot *catalog.Snapshot, now time.Time) (catalog.ClientKey, *protocol.GatewayError) {
	count, name, value := 0, "", ""
	malformed := false
	for key, values := range r.Header {
		lower := strings.ToLower(key)
		if !authHeaders[lower] {
			continue
		}
		if len(values) == 0 {
			count++
			name = lower
		}
		for _, v := range values {
			count += 1 + strings.Count(v, ",")
			name, value = lower, v
		}
	}
	if r.URL != nil {
		for _, part := range strings.Split(r.URL.RawQuery, "&") {
			if part == "" {
				continue
			}
			key, v, hasValue := strings.Cut(part, "=")
			decodedKey, err := url.QueryUnescape(key)
			if err != nil {
				malformed = true
				continue
			}
			if decodedKey == "key" {
				count++
				name, value = "query", v
				_, err = url.QueryUnescape(v)
				malformed = malformed || !hasValue || err != nil || strings.Contains(part, ";")
			} else if strings.Contains(part, ";") {
				// 分号不是约定的查询分隔符，不能借它藏入第二个身份。
				malformed = true
			}
		}
	}
	if count > 1 {
		return catalog.ClientKey{}, gatewayError(400, "ambiguous_client_auth", "客户端认证载体不唯一")
	}
	if malformed || count == 1 && name != "authorization" {
		return catalog.ClientKey{}, gatewayError(400, "invalid_client_auth", "客户端认证载体或格式无效")
	}
	if count == 0 {
		return catalog.ClientKey{}, gatewayError(401, "invalid_client_key", "客户端密钥无效")
	}
	value = strings.Trim(value, " \t")
	scheme, token, hasSpace := strings.Cut(value, " ")
	token = strings.TrimLeft(token, " ")
	if !hasSpace || !strings.EqualFold(scheme, "Bearer") || token == "" || !bearerToken.MatchString(token) {
		return catalog.ClientKey{}, gatewayError(400, "invalid_client_auth", "客户端 Bearer 格式无效")
	}
	if snapshot != nil {
		if key, ok := snapshot.Authenticate(token, now); ok {
			return key, nil
		}
	}
	return catalog.ClientKey{}, gatewayError(401, "invalid_client_key", "客户端密钥无效")
}

// headerValues 同时处理手工构造请求中的头名大小写变体。
func headerValues(h http.Header, name string) []string {
	var values []string
	for key, v := range h {
		if strings.EqualFold(key, name) {
			if len(v) == 0 {
				values = append(values, "")
			} else {
				values = append(values, v...)
			}
		}
	}
	return values
}

// CheckFormat 验证单一 JSON 媒体类型和 identity/gzip 请求编码。
func CheckFormat(r *http.Request) (string, *protocol.GatewayError) {
	invalid := func() (string, *protocol.GatewayError) {
		return "", gatewayError(415, "unsupported_request_format", "请求需要 UTF-8 JSON 和单一 identity 或 gzip 编码")
	}
	types := headerValues(r.Header, "Content-Type")
	if len(types) != 1 {
		return invalid()
	}
	media, params, err := mime.ParseMediaType(types[0])
	if err != nil || media != "application/json" && !(strings.HasPrefix(media, "application/") && strings.HasSuffix(media, "+json") && len(media) > len("application/+json")) {
		return invalid()
	}
	if charset, ok := params["charset"]; ok && !strings.EqualFold(charset, "utf-8") {
		return invalid()
	}
	encodings := headerValues(r.Header, "Content-Encoding")
	if len(encodings) == 0 {
		return "identity", nil
	}
	if len(encodings) != 1 {
		return invalid()
	}
	encoding := strings.ToLower(strings.Trim(encodings[0], " \t"))
	if encoding != "identity" && encoding != "gzip" {
		return invalid()
	}
	return encoding, nil
}
