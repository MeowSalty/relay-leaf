package relay

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"relayleaf/internal/catalog"
	"relayleaf/internal/testkit"
)

func TestRestrictionBoundaryRetryAfterIsNotQuotaResetEvidence(t *testing.T) {
	for _, retry := range []string{"0", "7"} {
		t.Run(retry, func(t *testing.T) {
			var count atomic.Int64
			clock := testkit.NewClock(time.Now())
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				count.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Retry-After", retry)
				w.WriteHeader(429)
				_, _ = io.WriteString(w, `{"error":{"type":"insufficient_quota","code":"credit_balance_exhausted"}}`)
			}))
			defer up.Close()
			h, down := observationGateway(t, up, nil, HandlerOptions{Now: clock.Now})
			resp, _, err := observationCall(t, down.URL, `{"model":"public-model"}`)
			if err != nil || resp.StatusCode != 429 {
				t.Fatalf("原错误未透传：%d %v", resp.StatusCode, err)
			}
			observationRecords(t, h, 1)
			permanent := false
			for _, r := range h.Stats().Restrictions {
				if r.Kind == "quota_exhausted" && r.Permanent {
					permanent = true
				}
			}
			if !permanent {
				t.Errorf("普通Retry-After被当作额度reset证据：%+v", h.Stats().Restrictions)
			}
			clock.Advance(time.Hour)
			second, _, err := observationCall(t, down.URL, `{"model":"public-model"}`)
			if err != nil || second.StatusCode != 503 || count.Load() != 1 {
				t.Errorf("未经验证的恢复时间重新放行耗尽额度：%d 出站%d 错误%v", second.StatusCode, count.Load(), err)
			}
			observationRecords(t, h, 2)
			observationNoOccupancy(t, h)
		})
	}
}

func TestRestrictionBoundaryHeaderFeedbackPrecedesErrorBody(t *testing.T) {
	gate := testkit.NewGate()
	defer gate.Open()
	var count atomic.Int64
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(429)
		w.(http.Flusher).Flush()
		if gate.Wait(r.Context()) == nil {
			_, _ = io.WriteString(w, `{"error":{"type":"rate_limit_error","code":"rate_limit_exceeded"}}`)
		}
	}))
	defer up.Close()
	h, down := observationGateway(t, up, nil, HandlerOptions{})
	first, err := http.DefaultClient.Do(observationRequest(t, down.URL, `{"model":"public-model"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer first.Body.Close()
	select {
	case <-gate.Reached():
	case <-time.After(time.Second):
		t.Fatal("上游错误正文未阻塞")
	}
	second, _, err := observationCall(t, down.URL, `{"model":"public-model"}`)
	if err != nil || second.StatusCode != 503 || count.Load() != 1 {
		t.Fatalf("错误正文未完成时头反馈未阻止新尝试：状态%d 出站%d 错误%v", second.StatusCode, count.Load(), err)
	}
	gate.Open()
	if _, err := io.ReadAll(first.Body); err != nil {
		t.Fatal(err)
	}
	observationRecords(t, h, 2)
	observationNoOccupancy(t, h)
}

func TestRestrictionBoundaryObservationLimitDoesNotInventFirstEventTimeout(t *testing.T) {
	// 首个数据事件完整且持续有字节，只是语义事件超过本地观察上限。
	event := []byte("event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"object\":\"response\",\"status\":\"in_progress\",\"output\":[]},\"padding\":\"" + strings.Repeat("x", 128) + "\"}\n\n")
	var expected bytes.Buffer
	expected.Write(event)
	for i := 0; i < 10; i++ {
		expected.WriteString(": 活跃心跳\n\n")
	}
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write(event)
		w.(http.Flusher).Flush()
		for i := 0; i < 10; i++ {
			timer := time.NewTimer(20 * time.Millisecond)
			select {
			case <-timer.C:
			case <-r.Context().Done():
				timer.Stop()
				return
			}
			_, _ = io.WriteString(w, ": 活跃心跳\n\n")
			w.(http.Flusher).Flush()
		}
	}))
	defer up.Close()
	h, down := observationGateway(t, up, func(d *catalog.Data) {
		d.Observability.MaxObservedSSEEventBytes = 32
		observationExecution(d, func(e *catalog.Execution) {
			e.StreamFirstEventTimeout = 100 * time.Millisecond
			e.StreamIdleTimeout = time.Second
		})
	}, HandlerOptions{})
	resp, raw, err := observationCall(t, down.URL, `{"model":"public-model","stream":true}`)
	if err != nil || resp.StatusCode != 200 || !bytes.Equal(raw, expected.Bytes()) {
		t.Errorf("本地观察超限截断持续活跃流：状态%d 收到%d/%d字节 错误%v", resp.StatusCode, len(raw), expected.Len(), err)
	}
	record := observationRecords(t, h, 1)[0]
	if record.Attempt.FailureKind == "stream_first_event_timeout" {
		t.Errorf("观察能力不足错误归因为上游首事件超时：%+v", record)
	}
	observationNoOccupancy(t, h)
}

func TestRestrictionBoundaryLegalTerminalStopsFirstContentWait(t *testing.T) {
	raw := observationSample(t, "incomplete.sse")
	var expected bytes.Buffer
	expected.Write(raw)
	for i := 0; i < 10; i++ {
		expected.WriteString(": 终态后的活跃尾部\n\n")
	}
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write(raw)
		w.(http.Flusher).Flush()
		for i := 0; i < 10; i++ {
			timer := time.NewTimer(20 * time.Millisecond)
			select {
			case <-timer.C:
			case <-r.Context().Done():
				timer.Stop()
				return
			}
			_, _ = io.WriteString(w, ": 终态后的活跃尾部\n\n")
			w.(http.Flusher).Flush()
		}
	}))
	defer up.Close()
	h, down := observationGateway(t, up, func(d *catalog.Data) {
		observationExecution(d, func(e *catalog.Execution) {
			e.StreamFirstContentEnabled = true
			e.StreamFirstContentTimeout = 100 * time.Millisecond
			e.StreamIdleTimeout = time.Second
		})
	}, HandlerOptions{})
	resp, received, err := observationCall(t, down.URL, `{"model":"public-model","stream":true}`)
	if err != nil || resp.StatusCode != 200 || !bytes.Equal(received, expected.Bytes()) {
		t.Errorf("合法限制终局未停止首内容等待：状态%d 收到%d/%d字节 错误%v", resp.StatusCode, len(received), expected.Len(), err)
	}
	record := observationRecords(t, h, 1)[0]
	if record.Attempt.FailureKind == "stream_first_content_timeout" {
		t.Errorf("合法终局后的尾部错误触发首内容时限：%+v", record)
	}
	observationNoOccupancy(t, h)
}
