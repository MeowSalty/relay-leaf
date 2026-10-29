package protocol

import (
	"strings"
	"testing"
	"time"
)

func TestBoundaryRequestControlStringsRespectStateBudget(t *testing.T) {
	control := strings.Repeat("x", 8192)
	for _, tc := range []struct{ name, body string }{
		{"输入条目类型", `{"model":"m","input":[{"type":"` + control + `"}]}`},
		{"工具类型", `{"model":"m","tools":[{"type":"` + control + `"}]}`},
		{"工具选择", `{"model":"m","tool_choice":"` + control + `"}`},
		{"选择对象类型", `{"model":"m","tool_choice":{"type":"` + control + `"}}`},
		{"允许列表模式", `{"model":"m","tool_choice":{"type":"allowed_tools","mode":"` + control + `","tools":[]}}`},
		{"容器类型", `{"model":"m","tools":[{"type":"code_interpreter","container":{"type":"` + control + `","id":"container_1"}}]}`},
		{"上下文类型", `{"model":"m","context_management":[{"type":"` + control + `"}]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			facts, err := ScanRequest([]byte(tc.body), 1024)
			if err == nil || err.Status != 503 || err.Code != "gateway_capacity_exceeded" {
				t.Fatalf("控制字符串超过状态预算仍被解码接受：错误=%v，重放资格=%s", err, facts.Replay)
			}
		})
	}
}

func TestBoundaryContainerTypeRejectsAmbiguousInterpretation(t *testing.T) {
	for _, typ := range []string{"false", "null", "123", "{}", "[]"} {
		body := []byte(`{"model":"m","tools":[{"type":"code_interpreter","container":{"type":` + typ + `,"id":"container_1"}}]}`)
		facts, err := ScanRequest(body, 4096)
		if err == nil || err.Code != "invalid_request_metadata" {
			t.Fatalf("受控 container.type 的非法类型未拒绝：错误=%v，资源类型=%v", err, facts.ResourceTypes)
		}
	}
}

func TestBoundaryRequestControlBudgetDoesNotCountConversationData(t *testing.T) {
	data := strings.Repeat("x", 8192)
	for _, body := range []string{
		`{"model":"m","input":"` + data + `"}`,
		`{"model":"m","tools":[{"type":"function","name":"f","parameters":{"description":"` + data + `"}}]}`,
		`{"model":"m","input":[{"type":"function_call","call_id":"c","name":"f","arguments":"` + data + `"}]}`,
	} {
		facts, err := ScanRequest([]byte(body), 1024)
		if err != nil || facts.Replay != ReplayAllowed {
			t.Fatalf("普通对话、schema 或 arguments 被计入控制字符串额度：%v", err)
		}
	}
	// 大量已经释放的条目类型不累加占用同一次请求的必要状态额度。
	body := `{"model":"m","input":[` + strings.TrimSuffix(strings.Repeat(`{"type":"input_text","text":"data"},`, 100), ",") + `]}`
	if _, err := ScanRequest([]byte(body), 1024); err != nil {
		t.Fatalf("临时控制字符串额度未按条目释放：%v", err)
	}
}

func TestBoundaryRequestEscapedControlChargesDecoderTemporaryBytes(t *testing.T) {
	body := []byte(`{"model":"m","input":[{"type":"` + strings.Repeat(`\u0078`, 70) + `"}]}`)
	if _, err := ScanRequest(body, 1024); err == nil || err.Code != "gateway_capacity_exceeded" {
		t.Fatalf("转义字符串的解码临时数组未纳入额度：%v", err)
	}
}

func TestBoundarySSETracksFirstEventAfterOversizedFrame(t *testing.T) {
	o := NewResponsesObserver(ObserverOptions{Mode: "stream", ContentType: "text/event-stream", StatusCode: 200, Replay: ReplayAllowed, MaxEventBytes: 256, MaxStateBytes: 4096, MaxJSONBytes: 4096, MaxErrorBytes: 4096})
	defer o.Close()
	start := time.Now()
	o.Feed([]byte("event: response.extension\ndata: "+strings.Repeat("x", 400)+"\n\n"), start)
	o.Feed([]byte("event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"object\":\"response\",\"status\":\"in_progress\"}}\n\n"), start.Add(time.Second))
	if o.Facts().FirstEvent.IsZero() {
		t.Fatal("SSE 超限帧后未恢复首事件识别")
	}
}

func TestBoundaryObservedAmbiguousOrUnknownExecutionCannotAllowReplay(t *testing.T) {
	for _, tc := range []struct{ name, control string }{
		{"重复工具类型", `"tools":[{"type":"function","type":"code_interpreter"}]`},
		{"未知工具选择", `"tool_choice":{"type":"future_server_selector"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := NewResponsesObserver(ObserverOptions{Mode: "non_stream", ContentType: "application/json", StatusCode: 200, Replay: ReplayAllowed, MaxJSONBytes: 4096, MaxStateBytes: 4096, MaxErrorBytes: 4096})
			defer o.Close()
			o.Feed([]byte(`{"object":"response","status":"completed","output":[],`+tc.control+`}`), time.Now())
			o.End(nil)
			facts := o.Facts()
			if facts.Replay == ReplayAllowed {
				t.Fatalf("观察到歧义或未知执行控制后仍允许重放：output=%s，coverage=%s", facts.Output, facts.Coverage)
			}
		})
	}
}
