package protocol

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"relayleaf/internal/testkit"
)

func sample(t testing.TB, name string) []byte {
	t.Helper()
	raw, err := testkit.ReadSample(name)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func TestRequestModelReplacementPreservesOtherBytes(t *testing.T) {
	for _, name := range []string{"request-rich.json", "request-tool-arguments.json"} {
		t.Run(name, func(t *testing.T) {
			raw := sample(t, name)
			facts, err := ScanRequest(raw, 65536)
			if err != nil {
				t.Fatal(err)
			}
			original := bytes.Clone(raw)
			changed := ReplaceModel(raw, facts, "upstream-新🌿")
			if !bytes.Equal(raw, original) {
				t.Fatal("扫描或替换修改了原请求")
			}
			if !bytes.Equal(raw[:facts.ModelSpan.Start], changed[:facts.ModelSpan.Start]) {
				t.Fatal("模型前缀字节发生变化")
			}
			suffix := raw[facts.ModelSpan.End:]
			if !bytes.HasSuffix(changed, suffix) {
				t.Fatal("模型后缀字节发生变化")
			}
			same := ReplaceModel(raw, facts, facts.Model)
			if len(raw) > 0 && &same[0] != &raw[0] {
				t.Fatal("相同模型未复用原切片")
			}
			if name == "request-rich.json" && facts.Replay != ReplayUnknown {
				t.Fatalf("未知根控制的重放约束错误：%+v", facts)
			}
		})
	}
}
func TestRequestNecessaryControlsAndResources(t *testing.T) {
	cases := []struct{ name, raw, code, replay, cap, resource string }{
		{"普通请求", `{"model":"m","input":"type=model"}`, "", string(ReplayAllowed), "", ""},
		{"模型重复", `{"model":"m","model":"m"}`, "ambiguous_request_metadata", "", "", ""},
		{"模式重复", `{"model":"m","stream":true,"stream":true}`, "ambiguous_request_metadata", "", "", ""},
		{"nullable模式", `{"model":"m","stream":null,"background":null}`, "", string(ReplayAllowed), "", ""},
		{"后台拒绝", `{"model":"m","background":true}`, "unsupported_request_mode", "", "", ""},
		{"零输出拒绝", `{"model":"m","max_output_tokens":0}`, "unsupported_request_mode", "", "", ""},
		{"上限溢出", `{"model":"m","max_output_tokens":9223372036854775808}`, "invalid_request_metadata", "", "", ""},
		{"null上限", `{"model":"m","max_output_tokens":null}`, "", string(ReplayAllowed), "", ""},
		{"预热拒绝", `{"model":"m","prompt_cache_options":{"prewarm":true}}`, "unsupported_request_mode", "", "", ""},
		{"预热类型", `{"model":"m","prompt_cache_options":{"prewarm":null}}`, "invalid_request_metadata", "", "", ""},
		{"控制祖先重复", `{"model":"m","input":[],"input":[]}`, "ambiguous_request_metadata", "", "", ""},
		{"工具类型重复", `{"model":"m","tools":[{"type":"function","type":"function","name":"f"}]}`, "ambiguous_request_metadata", "", "", ""},
		{"工具schema作为数据", `{"model":"m","tools":[{"type":"function","name":"f","parameters":{"model":1,"model":2,"type":"code_interpreter"}}]}`, "", string(ReplayAllowed), "tool_calling", ""},
		{"工具参数作为数据", `{"model":"m","input":[{"type":"function_call","call_id":"c","name":"f","arguments":"{\"model\":1,\"type\":\"file\"}"}]}`, "", string(ReplayAllowed), "tool_calling", ""},
		{"内联图像", `{"model":"m","input":[{"role":"user","content":[{"type":"input_image","image_url":"data:image/png;base64,AAA"}]}]}`, "", string(ReplayAllowed), "image_input", ""},
		{"嵌套工具文件", `{"model":"m","input":[{"type":"function_call_output","call_id":"c","output":[{"type":"input_file","file_id":"file1"}]}]}`, "", string(ReplayAllowed), "file_input", "openai.file"},
		{"已有会话", `{"model":"m","conversation":{"id":"conv1"}}`, "", string(ReplayForbidden), "", "openai.conversation"},
		{"会话引用冲突", `{"model":"m","conversation":"conv1","previous_response_id":"resp1"}`, "ambiguous_request_metadata", "", "", ""},
		{"只读引用", `{"model":"m","previous_response_id":"resp1","store":true}`, "", string(ReplayAllowed), "", "openai.response"},
		{"模板未知", `{"model":"m","prompt":{"id":"p","variables":{"type":"mcp"}}}`, "", string(ReplayUnknown), "", "openai.prompt"},
		{"服务端工具", `{"model":"m","tools":[{"type":"code_interpreter","container":{"type":"auto","file_ids":["file1"]}}]}`, "", string(ReplayForbidden), "server_tools", "openai.file"},
		{"未知容器", `{"model":"m","tools":[{"type":"code_interpreter","container":{"mystery":true}}]}`, "unsupported_request_mode", "", "", ""},
		{"未知工具", `{"model":"m","tools":[{"type":"vendor_new"}]}`, "", string(ReplayUnknown), "", ""},
		{"未知模式键", `{"model":"m","stream_options":{"vendor_control":true}}`, "", string(ReplayUnknown), "", ""},
		{"密文引用", `{"model":"m","input":[{"type":"reasoning","encrypted_content":"opaque"}]}`, "", string(ReplayAllowed), "", "openai.opaque"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f, err := ScanRequest([]byte(c.raw), 16384)
			if c.code != "" {
				if err == nil || err.Code != c.code {
					t.Fatalf("期望%s，实际%+v", c.code, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if string(f.Replay) != c.replay {
				t.Fatalf("重放约束：%+v", f)
			}
			if c.cap != "" && !contains(f.Capabilities, c.cap) {
				t.Fatalf("缺少能力%s：%+v", c.cap, f)
			}
			if c.resource != "" && !contains(f.ResourceTypes, c.resource) {
				t.Fatalf("缺少资源%s：%+v", c.resource, f)
			}
		})
	}
}
func contains(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}
func TestRequestSyntaxAndCapacity(t *testing.T) {
	for _, raw := range [][]byte{[]byte(`[]`), []byte(`{"model":"m"} trailing`), []byte{'{', '"', 'm', 'o', 'd', 'e', 'l', '"', ':', '"', 0xff, '"', '}'}} {
		if _, err := ScanRequest(raw, 4096); err == nil || err.Status != 415 {
			t.Fatalf("非法JSON未拒绝：%q %v", raw, err)
		}
	}
	if _, err := ScanRequest([]byte(`{"model":"m"}`), 8); err == nil || err.Code != "gateway_capacity_exceeded" {
		t.Fatalf("必要状态预算未生效：%v", err)
	}
}
func observerOptions(mode string) ObserverOptions {
	content := "application/json"
	if mode == "stream" {
		content = "text/event-stream"
	}
	return ObserverOptions{Mode: mode, ContentType: content, StatusCode: 200, Replay: ReplayAllowed, MaxJSONBytes: 65536, MaxEventBytes: 65536, MaxStateBytes: 16384, MaxErrorBytes: 16384}
}
func observe(t *testing.T, raw []byte, opts ObserverOptions, end error, chunk int) ResponseFacts {
	t.Helper()
	o := NewResponsesObserver(opts)
	defer o.Close()
	at := time.Unix(100, 0)
	for i := 0; i < len(raw); i += chunk {
		j := min(i+chunk, len(raw))
		o.Feed(raw[i:j], at)
		at = at.Add(time.Millisecond)
	}
	o.End(end)
	return o.Facts()
}
func TestResponsesRawFixtures(t *testing.T) {
	cases := []struct {
		name, output, result string
		complete             bool
	}{
		{"completed.json", "nonempty", "completed", true}, {"completed-lf.sse", "nonempty", "completed", true}, {"completed-crlf.sse", "nonempty", "completed", true},
		{"empty.json", "empty", "completed", true}, {"empty-lf.sse", "empty", "completed", true},
		{"tools.json", "nonempty", "tool_handoff", true}, {"tools-lf.sse", "nonempty", "tool_handoff", true},
		{"refusal.json", "not_applicable", "refusal", true}, {"refusal-lf.sse", "not_applicable", "refusal", true},
		{"incomplete.sse", "not_applicable", "limited", true}, {"missing-terminal.sse", "unknown", "unknown", false},
		{"status-conflict.sse", "unknown", "unknown", false}, {"unknown-output.json", "unknown", "completed", false},
		{"unknown-event.sse", "unknown", "completed", false}, {"truncated.sse", "unknown", "unknown", false},
		{"protocol-error.sse", "unknown", "failed", false}, {"terminal-tail.sse", "unknown", "completed", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			mode := "non_stream"
			if strings.HasSuffix(c.name, ".sse") {
				mode = "stream"
			}
			raw := sample(t, c.name)
			for _, chunk := range []int{1, 7, len(raw) + 1} {
				f := observe(t, raw, observerOptions(mode), nil, chunk)
				if f.Output != c.output || f.Result != c.result || f.ProtocolComplete != c.complete {
					t.Fatalf("分块%d：期望%s/%s/%v，实际%+v", chunk, c.output, c.result, c.complete, f)
				}
			}
		})
	}
}
func TestTerminalAndTransportAreSeparate(t *testing.T) {
	o := NewResponsesObserver(observerOptions("stream"))
	defer o.Close()
	o.Feed(sample(t, "empty-lf.sse"), time.Now())
	before := o.Facts()
	if before.Output == "empty" || before.EOF || before.ProtocolComplete {
		t.Fatalf("EOF前提前判空：%+v", before)
	}
	o.End(errors.New("注入读错误"))
	if f := o.Facts(); f.Output == "empty" || f.EOF || f.ProtocolComplete {
		t.Fatalf("尾部错误被终态掩盖：%+v", f)
	}
	f := observe(t, sample(t, "terminal-tail-error.sse"), observerOptions("stream"), io.ErrUnexpectedEOF, 3)
	if f.ProtocolComplete || f.EOF || f.Output == "empty" {
		t.Fatalf("错误尾部错误分类：%+v", f)
	}
}
func TestContentAndErrorAreMonotonic(t *testing.T) {
	raw := append(sample(t, "completed-lf.sse"), sample(t, "protocol-error.sse")...)
	f := observe(t, raw, observerOptions("stream"), nil, 7)
	if f.Output != "nonempty" || !f.Error.Present || f.Result != "failed" || f.ProtocolComplete {
		t.Fatalf("内容或错误被后续事件覆盖：%+v", f)
	}
	raw = append(sample(t, "protocol-error.sse"), sample(t, "completed-lf.sse")...)
	f = observe(t, raw, observerOptions("stream"), nil, 7)
	if !f.Error.Present || f.Result != "failed" || f.Output != "nonempty" {
		t.Fatalf("错误被普通完成清除：%+v", f)
	}
}
func TestObservationLimitsRecoverAndDoNotGuessEmpty(t *testing.T) {
	opts := observerOptions("stream")
	opts.MaxEventBytes = 800
	raw := []byte("event: vendor\ndata: " + strings.Repeat("x", 1000) + "\n\n")
	raw = append(raw, sample(t, "completed-lf.sse")...)
	f := observe(t, raw, opts, nil, 11)
	if f.Output != "nonempty" || f.OutputCoverage == "complete" || f.ProtocolComplete {
		t.Fatalf("超大事件后恢复错误：%+v", f)
	}
	opts = observerOptions("non_stream")
	opts.MaxJSONBytes = 32
	f = observe(t, sample(t, "empty.json"), opts, nil, 3)
	if f.Output != "unknown" || f.OutputCoverage != "unavailable" {
		t.Fatalf("超限文档误判空：%+v", f)
	}
	opts = observerOptions("stream")
	opts.MaxStateBytes = 16
	f = observe(t, sample(t, "empty-lf.sse"), opts, nil, 3)
	if f.Output == "empty" {
		t.Fatalf("状态超限误判空：%+v", f)
	}
}

type testBudget struct{ used, peak, limit int64 }

func (b *testBudget) Reserve(n int64) bool {
	if n > b.limit-b.used {
		return false
	}
	b.used += n
	b.peak = max(b.peak, b.used)
	return true
}
func (b *testBudget) Release(n int64) {
	b.used -= n
	if b.used < 0 {
		panic("观察预算重复释放")
	}
}
func TestObserverBudgetAndFactsOwnership(t *testing.T) {
	b := &testBudget{limit: 1 << 20}
	opts := observerOptions("stream")
	opts.Budget = b
	o := NewResponsesObserver(opts)
	o.Feed(sample(t, "completed-lf.sse"), time.Now())
	o.End(nil)
	f := o.Facts()
	if f.Usage.Input == nil {
		t.Fatal("缺少样本用量")
	}
	*f.Usage.Input = 999
	if *o.Facts().Usage.Input == 999 {
		t.Fatal("Facts共享用量指针")
	}
	o.Close()
	o.Close()
	if b.used != 0 || b.peak == 0 {
		t.Fatalf("预算未完整释放：%+v", b)
	}
	b = &testBudget{limit: 10}
	opts.Budget = b
	o = NewResponsesObserver(opts)
	o.Feed(sample(t, "empty-lf.sse"), time.Now())
	o.End(nil)
	if o.Facts().Output == "empty" {
		t.Fatal("预算不足仍判空")
	}
	o.Close()
	if b.used != 0 {
		t.Fatalf("失败预算残留%d", b.used)
	}
}
func TestUnsupportedEncodingAndZeroBody(t *testing.T) {
	opts := observerOptions("non_stream")
	opts.ContentEncoding = "gzip"
	f := observe(t, sample(t, "completed.json.gz"), opts, nil, 5)
	if f.Output != "unknown" || f.Coverage != "unavailable" || f.ProtocolComplete {
		t.Fatalf("压缩响应被错误观察：%+v", f)
	}
	f = observe(t, nil, opts, nil, 1)
	if f.Output != "empty" || f.EmptyReason != "empty_transport_body" {
		t.Fatalf("真实零字节未确认：%+v", f)
	}
}
func TestUsageMissingDoesNotInvalidateOutput(t *testing.T) {
	f := observe(t, []byte(`{"object":"response","status":"completed","output":[],"usage":{"input_tokens":3}}`), observerOptions("non_stream"), nil, 3)
	if f.Output != "empty" || !f.ProtocolComplete || f.Usage.Status != "partial" || f.OutputCoverage != "complete" {
		t.Fatalf("部分用量污染输出：%+v", f)
	}
	f = observe(t, []byte(`{"object":"response","status":"completed","output":[],"usage":{"input_tokens":1,"input_tokens":2}}`), observerOptions("non_stream"), nil, 3)
	if f.Output != "empty" || f.OutputCoverage != "complete" {
		t.Fatalf("重复用量污染输出：%+v", f)
	}
}
func TestObserverExecutionConstraintOnlyTightens(t *testing.T) {
	o := NewResponsesObserver(observerOptions("stream"))
	defer o.Close()
	o.Feed([]byte("event: response.web_search_call.searching\ndata: {\"type\":\"response.web_search_call.searching\",\"output_index\":0}\n\n"), time.Now())
	if o.Facts().Replay != ReplayForbidden || o.Facts().Output == "nonempty" {
		t.Fatalf("搜索状态误当输出或未禁止重放：%+v", o.Facts())
	}
	o.Feed(sample(t, "completed-lf.sse"), time.Now())
	o.End(nil)
	if o.Facts().Replay != ReplayForbidden {
		t.Fatal("后续完成放宽了重放约束")
	}
}
func TestMultiLineDataHeartbeatAndOpaqueOutputs(t *testing.T) {
	f := observe(t, sample(t, "multiline-heartbeat.sse"), observerOptions("stream"), nil, 1)
	if f.Output != "empty" || !f.ProtocolComplete || f.FirstEvent.IsZero() {
		t.Fatalf("多行data未正确观察：%+v", f)
	}
	for _, typ := range []string{"reasoning", "compaction"} {
		raw := []byte(`{"object":"response","status":"completed","output":[{"type":"` + typ + `","encrypted_content":"opaque"}]}`)
		f = observe(t, raw, observerOptions("non_stream"), nil, 3)
		if f.Output != "nonempty" {
			t.Fatalf("密文输出误判空：%+v", f)
		}
	}
}
func TestUnknownOutputFieldCannotProveEmpty(t *testing.T) {
	f := observe(t, []byte(`{"object":"response","status":"completed","output":[{"type":"message","role":"assistant","content":[],"vendor_payload":"unobserved"}]}`), observerOptions("non_stream"), nil, 3)
	if f.Output == "empty" || f.OutputCoverage == "complete" {
		t.Fatalf("未知输出字段被忽略：%+v", f)
	}
}
func TestStopObservingDoesNotInventTransportEnd(t *testing.T) {
	b := &testBudget{limit: 1 << 20}
	opts := observerOptions("stream")
	opts.Budget = b
	o := NewResponsesObserver(opts)
	o.Feed(sample(t, "completed-lf.sse"), time.Now())
	o.StopObserving("error_observation_timeout")
	o.StopObserving("ignored_repeat")
	before := o.Facts()
	if before.Output != "nonempty" || before.EOF || before.ProtocolComplete {
		t.Fatalf("停止观察伪造了传输收尾：%+v", before)
	}
	o.Feed(sample(t, "protocol-error.sse"), time.Now())
	o.End(nil)
	after := o.Facts()
	if !after.EOF || after.Output != "nonempty" || after.Error.Present || after.ProtocolComplete {
		t.Fatalf("停止后的EOF或事实错误：%+v", after)
	}
	o.Close()
	if b.used != 0 {
		t.Fatalf("停止观察预算残留%d", b.used)
	}
}
func TestOversizedSSEFrameKeepsIndependentEventFacts(t *testing.T) {
	for _, newline := range []string{"\n", "\r\n", "\r"} {
		t.Run(fmt.Sprintf("换行%q", newline), func(t *testing.T) {
			opts := observerOptions("stream")
			opts.MaxEventBytes = 16
			b := &testBudget{limit: 128}
			opts.Budget = b
			o := NewResponsesObserver(opts)
			defer o.Close()
			at := time.Unix(123, 0)
			heartbeat := []byte("event: heartbeat" + newline + "data: " + strings.Repeat("x", 1000) + newline + newline)
			for _, v := range heartbeat {
				o.Feed([]byte{v}, at)
			}
			if !o.Facts().FirstEvent.IsZero() {
				t.Fatal("超长已识别心跳误算首事件")
			}
			partial := []byte("event: response.created" + newline + "data: " + strings.Repeat("x", 1000) + newline)
			for _, v := range partial {
				o.Feed([]byte{v}, at)
			}
			if !o.Facts().FirstEvent.IsZero() {
				t.Fatal("未结束帧提前记录首事件")
			}
			o.Feed([]byte(newline), at.Add(time.Second))
			f := o.Facts()
			if !f.FirstEvent.Equal(at.Add(time.Second)) || f.Output == "nonempty" || f.OutputCoverage == "complete" {
				t.Fatalf("超长事件的分帧与语义未分离：%+v", f)
			}
			if b.peak > 128 {
				t.Fatalf("超长载荷被无限保留：%+v", b)
			}
			o.End(nil)
			o.Close()
			if b.used != 0 {
				t.Fatalf("超长帧预算残留%d", b.used)
			}
		})
	}
}

func FuzzRequestScan(f *testing.F) {
	for _, name := range []string{"request-rich.json", "request-tool-arguments.json"} {
		f.Add(sample(f, name))
	}
	f.Add([]byte(`{"model":"m","input":[]}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 1<<20 {
			t.Skip()
		}
		original := bytes.Clone(raw)
		facts, err := ScanRequest(raw, 65536)
		if !bytes.Equal(raw, original) {
			t.Fatal("扫描修改原字节")
		}
		if err == nil {
			span := facts.ModelSpan
			if span.Start < 0 || span.End > len(raw) || span.Start >= span.End {
				t.Fatalf("模型区间无效：%+v", span)
			}
			changed := ReplaceModel(raw, facts, "上游🌿")
			if !bytes.Equal(raw[:span.Start], changed[:span.Start]) || !bytes.HasSuffix(changed, raw[span.End:]) {
				t.Fatal("模型替换改变其他字节")
			}
		}
	})
}
func FuzzSSE(f *testing.F) {
	for _, name := range []string{"completed-lf.sse", "tools-lf.sse", "unknown-event.sse", "truncated.sse", "multiline-heartbeat.sse"} {
		f.Add(sample(f, name), uint8(7))
	}
	f.Fuzz(func(t *testing.T, raw []byte, chunk uint8) {
		if len(raw) > 1<<20 {
			t.Skip()
		}
		opts := observerOptions("stream")
		opts.MaxEventBytes = 4096
		opts.MaxStateBytes = 4096
		b := &testBudget{limit: 16384}
		opts.Budget = b
		o := NewResponsesObserver(opts)
		size := int(chunk) + 1
		for i := 0; i < len(raw); i += size {
			o.Feed(raw[i:min(i+size, len(raw))], time.Unix(1, 0))
		}
		before := o.Facts()
		if before.Output == "empty" {
			t.Fatal("EOF前判空")
		}
		o.End(nil)
		o.Close()
		if b.used != 0 {
			t.Fatalf("观察预算残留%d", b.used)
		}
	})
}
