package relay

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"relayleaf/internal/observe"
	"relayleaf/internal/protocol"
)

func gatewayError(status int, code string) *protocol.GatewayError {
	messages := map[string]string{
		"request_body_too_large":     "请求正文超过允许的大小。",
		"gateway_capacity_exceeded":  "网关处理容量不足，请稍后重试。",
		"client_concurrency_limited": "客户端并发请求已达到上限。",
		"no_available_target":        "当前没有可用的上游调用许可。",
		"request_body_timeout":       "读取请求正文超时。",
		"request_deadline_exceeded":  "调用方截止时间已到。",
		"upstream_timeout":           "等待上游响应超时。",
		"upstream_transport_error":   "上游连接或传输失败。",
		"upstream_empty_response":    "上游未返回有效输出，且没有可用重试次数。",
		"unsupported_request_format": "请求格式或编码不受支持。",
		"invalid_request_metadata":   "请求正文或必要元数据无效。",
	}
	message := messages[code]
	if message == "" {
		message = "请求无法处理。"
	}
	return &protocol.GatewayError{Status: status, Code: code, Message: message}
}

func (h *Handler) reject(ctx context.Context, w http.ResponseWriter, r *http.Request, record *observe.RequestRecord, e *protocol.GatewayError) {
	// 未读或未完整读入的正文不交给 HTTP 连接复用过程继续等待。
	if r != nil {
		r.Close = true
		w.Header().Set("Connection", "close")
	}
	record.HTTPStatus, record.ErrorCode, record.ErrorSource = e.Status, e.Code, "gateway"
	if e.Code == "request_deadline_exceeded" {
		record.ErrorSource = "caller"
	}
	if ctx.Err() != nil && !errors.Is(context.Cause(ctx), context.DeadlineExceeded) && e.Code != "request_body_timeout" && e.Code != "request_deadline_exceeded" {
		var phase *executionFailure
		if !errors.As(context.Cause(ctx), &phase) {
			return
		}
	}
	h.writeError(w, e, record.ID)
}

func (h *Handler) writeError(w http.ResponseWriter, e *protocol.GatewayError, requestID string) {
	header := w.Header()
	for key := range header {
		header.Del(key)
	}
	header.Set("Content-Type", "application/json; charset=utf-8")
	header.Set("Cache-Control", "no-store")
	header.Set("X-RelayLeaf-Request-Id", requestID)
	header.Set("X-RelayLeaf-Error-Code", e.Code)
	header.Set("Connection", "close")
	if e.Status == http.StatusUnauthorized {
		header.Set("WWW-Authenticate", `Bearer realm="RelayLeaf"`)
	}
	if e.Status == http.StatusMethodNotAllowed {
		header.Set("Allow", "POST")
	}
	kind := "invalid_request_error"
	if e.Status == http.StatusTooManyRequests {
		kind = "rate_limit_error"
	} else if e.Status >= 500 {
		kind = "server_error"
	}
	payload := struct {
		Error struct {
			Message string `json:"message"`
			Type    string `json:"type"`
			Code    string `json:"code"`
		} `json:"error"`
	}{}
	payload.Error.Message, payload.Error.Type, payload.Error.Code = e.Message, kind, e.Code
	body, err := json.Marshal(payload)
	if err != nil {
		return
	}
	rc := http.NewResponseController(w)
	deadline := time.Now().Add(h.snapshot.Execution().DownstreamWriteIdleTimeout)
	_ = rc.SetWriteDeadline(deadline)
	stopped := make(chan struct{})
	stop := context.AfterFunc(h.ctx, func() { _ = rc.SetWriteDeadline(time.Now()); close(stopped) })
	defer func() {
		if !stop() {
			<-stopped
		}
		_ = rc.SetWriteDeadline(time.Time{})
	}()
	w.WriteHeader(e.Status)
	_, _ = w.Write(body)
	_ = rc.Flush()
}

func (h *Handler) finishFailure(ctx context.Context, w http.ResponseWriter, record *observe.RequestRecord, err error, committed bool) {
	code, source, kind, status := "upstream_transport_error", "upstream", "transport_error", 502
	var phase *executionFailure
	cause := context.Cause(ctx)
	switch {
	case errors.Is(cause, errShutdown):
		code, source, kind, status = "server_shutdown", "gateway", "server_shutdown", 503
	case errors.Is(cause, context.DeadlineExceeded):
		code, source, kind, status = "request_deadline_exceeded", "caller", "caller_deadline", 504
	case errors.As(cause, &phase):
		code, source, kind, status = "upstream_timeout", "upstream", phase.kind, 504
	case errors.Is(cause, context.Canceled):
		code, source, kind, status = "client_cancelled", "caller", "caller_cancelled", 0
	case errors.As(err, &phase):
		code, source, kind, status = "upstream_timeout", "upstream", phase.kind, 504
	}
	if kind == "downstream_write_timeout" || kind == "downstream_write_error" {
		code, source, status = kind, "downstream", 0
	}
	record.ErrorCode, record.ErrorSource = code, source
	if record.Attempt != nil {
		record.Attempt.FailureKind, record.Attempt.ErrorSource = kind, source
	}
	if !committed && status != 0 && source != "downstream" && code != "server_shutdown" {
		record.HTTPStatus = status
		h.writeError(w, gatewayError(status, code), record.ID)
	}
}
