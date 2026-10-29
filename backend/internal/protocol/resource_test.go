package protocol

import (
	"bytes"
	"slices"
	"strings"
	"testing"
)

func TestRequestCombinedResourceReferencesPreserveBytes(t *testing.T) {
	raw := []byte(` {
  "model" : "public\u002dmodel", "previous_response_id":"resp_1",
  "input":[
    {"role":"user","content":[{"type":"input_file","file_id":"file_1"},{"type":"input_image","file_id":"file_2"}]},
    {"type":"code_interpreter_call","container_id":"cntr_history","id":"ci_1","code":"1e1000","outputs":[]},
    {"type":"computer_call_output","call_id":"call_1","output":{"type":"computer_screenshot","file_id":"file_3"}},
    {"type":"function_call_output","call_id":"call_2","output":[{"type":"input_file","file_id":"file_4"}]}
  ],
  "tools":[{"type":"file_search","vector_store_ids":["vs_1","vs_2"]},{"type":"code_interpreter","container":{"type":"auto","memory_limit":"4g","file_ids":["file_5"]}}],
  "metadata":{"unknown":{"model":1e1000,"model":-0,"files":{"type":"code_interpreter"}}}
} `)
	original := bytes.Clone(raw)
	facts, err := ScanRequest(raw, 8192)
	if err != nil {
		t.Fatal(err)
	}
	assertResourceFacts(t, facts, ReplayForbidden, []string{"file_input", "image_input", "server_tools", "tool_calling"}, []string{"openai.container", "openai.file", "openai.response", "openai.vector_store"})
	changed := ReplaceModel(raw, facts, "上游模型🌿")
	if !bytes.Equal(raw, original) || !bytes.Equal(raw[:facts.ModelSpan.Start], changed[:facts.ModelSpan.Start]) || !bytes.HasSuffix(changed, raw[facts.ModelSpan.End:]) {
		t.Fatal("联合资源扫描或模型替换改写了原字节区间")
	}
	if same := ReplaceModel(raw, facts, facts.Model); &same[0] != &raw[0] {
		t.Fatal("相同模型未复用原切片")
	}
}

func assertResourceFacts(t testing.TB, facts RequestFacts, replay ReplayConstraint, capabilities, resources []string) {
	t.Helper()
	caps, refs := slices.Clone(facts.Capabilities), slices.Clone(facts.ResourceTypes)
	slices.Sort(caps)
	slices.Sort(refs)
	if facts.Replay != replay || !slices.Equal(caps, capabilities) || !slices.Equal(refs, resources) {
		t.Fatalf("请求事实不符合资源与执行约束：%+v，期望重放=%s，能力=%v，资源=%v", facts, replay, capabilities, resources)
	}
}

func TestRequestHistoricalServerResources(t *testing.T) {
	for _, tc := range []struct {
		name, fields string
		caps, refs   []string
	}{
		{"历史容器重用", `"input":[{"type":"code_interpreter_call","container_id":"cntr_1"}],"tools":[{"type":"code_interpreter","container":{"type":"auto"}}]`, []string{"server_tools"}, []string{"openai.container"}},
		{"显式容器", `"tools":[{"type":"code_interpreter","container":"cntr_1"}]`, []string{"server_tools"}, []string{"openai.container"}},
		{"新建容器文件", `"tools":[{"type":"code_interpreter","container":{"file_ids":["file_1"],"type":"auto","memory_limit":"1g"}}]`, []string{"server_tools"}, []string{"openai.file"}},
		{"截图文件", `"input":[{"output":{"file_id":"file_1","type":"computer_screenshot"},"call_id":"call_1","type":"computer_call_output"}]`, []string{"image_input", "server_tools"}, []string{"openai.file"}},
		{"截图关联标识", `"input":[{"type":"computer_call_output","call_id":"call_1","output":{"type":"computer_screenshot","image_url":"data:image/png;base64,AAA"}}]`, []string{"image_input", "server_tools"}, nil},
		{"截图外部地址", `"input":[{"type":"computer_call_output","call_id":"call_1","output":{"type":"computer_screenshot","image_url":"https://example.invalid/screenshot.png"}}]`, []string{"image_input", "server_tools"}, nil},
		{"历史搜索", `"input":[{"type":"file_search_call","id":"fs_1","queries":["test"],"results":[]}]`, []string{"server_tools"}, nil},
		{"历史计算机调用", `"input":[{"type":"computer_call","call_id":"call_1","action":{"type":"screenshot"}}]`, []string{"server_tools"}, nil},
		{"历史工具审批", `"input":[{"type":"mcp_approval_response","approval_request_id":"approval_1","approve":true}]`, []string{"server_tools"}, nil},
		{"图像蒙版文件", `"tools":[{"type":"image_generation","input_image_mask":{"file_id":"file_1"}}]`, []string{"image_input", "image_output", "server_tools"}, []string{"openai.file"}},
		{"图像蒙版外部地址", `"tools":[{"type":"image_generation","input_image_mask":{"image_url":"https://example.invalid/mask.png"}}]`, []string{"image_input", "image_output", "server_tools"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			facts, err := ScanRequest([]byte(`{"model":"m",`+tc.fields+`}`), 8192)
			if err != nil {
				t.Fatal(err)
			}
			assertResourceFacts(t, facts, ReplayForbidden, tc.caps, tc.refs)
		})
	}
}

func TestRequestInlineDataDoesNotBindResourceScopes(t *testing.T) {
	raw := []byte(`{"model":"m","input":[{"role":"user","content":[{"type":"input_file","file_data":"data:application/pdf;base64,AAA","filename":"x.pdf"},{"type":"input_file","file_url":"https://example.invalid/x.pdf"},{"type":"input_image","image_url":"data:image/png;base64,AAA"},{"type":"input_image","image_url":"http://example.invalid/x.png"}]},{"type":"function_call","call_id":"call_1","arguments":"{\"container_id\":\"cntr_1\"}"},{"type":"function_call_output","call_id":"call_1","output":"file_id=file_1"}],"tools":[{"type":"function","name":"f","parameters":{"model":1,"model":2,"files":{"file_ids":["file_1"]},"type":"code_interpreter","container":{"id":"cntr_1"}}}]}`)
	facts, err := ScanRequest(raw, 8192)
	if err != nil {
		t.Fatal(err)
	}
	assertResourceFacts(t, facts, ReplayAllowed, []string{"file_input", "image_input", "tool_calling"}, nil)
	facts, err = ScanRequest([]byte(`{"model":"m","input":[{"type":"input_file","file_id":null,"file_data":"data:application/pdf;base64,AAA"},{"type":"input_image","file_id":null,"image_url":"https://example.invalid/x.png"}]}`), 8192)
	if err != nil {
		t.Fatal(err)
	}
	assertResourceFacts(t, facts, ReplayAllowed, []string{"file_input", "image_input"}, nil)
}

func TestRequestResourceMetadataRejectsAmbiguity(t *testing.T) {
	for _, tc := range []struct{ name, fields string }{
		{"文件引用重复", `"input":[{"type":"input_file","file_id":"file_1","file_id":"file_2"}]`},
		{"图片引用转义重复", `"input":[{"type":"input_image","file_id":"file_1","file\u005fid":"file_2"}]`},
		{"历史容器重复", `"input":[{"type":"code_interpreter_call","container_id":"cntr_1","container_id":"cntr_2"}]`},
		{"历史类型重复", `"input":[{"type":"code_interpreter_call","container_id":"cntr_1","type":"function_call"}]`},
		{"截图祖先重复", `"input":[{"type":"computer_call_output","output":{"type":"computer_screenshot","file_id":"file_1"},"output":{"type":"computer_screenshot","image_url":"data:image/png;base64,AAA"}}]`},
		{"截图文件重复", `"input":[{"type":"computer_call_output","output":{"type":"computer_screenshot","file_id":"file_1","file_id":"file_2"}}]`},
		{"截图类型重复", `"input":[{"type":"computer_call_output","output":{"type":"computer_screenshot","type":"computer_screenshot"}}]`},
		{"消息祖先重复", `"input":[{"role":"user","content":[{"type":"input_file","file_id":"file_1"}],"content":[]}]`},
		{"工具结果祖先重复", `"input":[{"type":"function_call_output","output":[{"type":"input_file","file_id":"file_1"}],"output":"data"}]`},
		{"工具容器祖先重复", `"tools":[{"type":"code_interpreter","container":"cntr_1","container":{"type":"auto"}}]`},
		{"容器类型重复", `"tools":[{"type":"code_interpreter","container":{"type":"auto","type":"auto"}}]`},
		{"容器文件重复", `"tools":[{"type":"code_interpreter","container":{"type":"auto","file_ids":["file_1"],"file_ids":[]}}]`},
		{"工具文件重复", `"tools":[{"type":"code_interpreter","file_ids":["file_1"],"file_ids":[]}]`},
		{"向量库重复", `"tools":[{"type":"file_search","vector_store_ids":["vs_1"],"vector_store_ids":[]}]`},
		{"蒙版祖先重复", `"tools":[{"type":"image_generation","input_image_mask":{"file_id":"file_1"},"input_image_mask":{"file_id":"file_2"}}]`},
		{"蒙版文件重复", `"tools":[{"type":"image_generation","input_image_mask":{"file_id":"file_1","file_id":"file_2"}}]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ScanRequest([]byte(`{"model":"m",`+tc.fields+`}`), 8192)
			if err == nil || err.Code != "ambiguous_request_metadata" {
				t.Fatalf("必要资源或祖先的歧义未拒绝：%v", err)
			}
		})
	}
}

func TestRequestResourceMetadataRejectsInvalidTypes(t *testing.T) {
	for _, value := range []string{`null`, `false`, `123`, `{}`, `[]`, `""`} {
		for _, fields := range []string{
			`"input":[{"type":"code_interpreter_call","container_id":` + value + `}]`,
			`"input":[{"type":"computer_call_output","output":{"type":"computer_screenshot","file_id":` + value + `}}]`,
			`"tools":[{"type":"file_search","vector_store_ids":[` + value + `]}]`,
			`"tools":[{"type":"code_interpreter","container":{"type":"auto","file_ids":[` + value + `]}}]`,
			`"tools":[{"type":"code_interpreter","file_ids":[` + value + `]}]`,
			`"tools":[{"type":"image_generation","input_image_mask":{"file_id":` + value + `}}]`,
		} {
			if _, err := ScanRequest([]byte(`{"model":"m",`+fields+`}`), 8192); err == nil || err.Code != "invalid_request_metadata" {
				t.Fatalf("资源引用的非法类型未拒绝：字段=%s，错误=%v", fields, err)
			}
		}
		if value != "null" {
			for _, typ := range []string{"input_file", "input_image"} {
				if _, err := ScanRequest([]byte(`{"model":"m","input":[{"type":"`+typ+`","file_id":`+value+`}]}`), 8192); err == nil || err.Code != "invalid_request_metadata" {
					t.Fatalf("可空文件引用的非法非空值未拒绝：类型=%s，值=%s，错误=%v", typ, value, err)
				}
			}
		}
	}
	for _, fields := range []string{
		`"input":[{"type":"code_interpreter_call"}]`,
		`"input":[{"type":"computer_call_output"}]`,
		`"input":[{"type":"computer_call_output","output":"data"}]`,
		`"input":[{"type":"computer_call_output","output":[]}]`,
		`"input":[{"role":"user","content":{"type":"input_file","file_id":"file_1"}}]`,
		`"input":[{"type":"function_call_output","output":{"type":"input_file","file_id":"file_1"}}]`,
		`"input":[{"type":"input_image","image_url":{"file_id":"file_1"}}]`,
		`"input":[{"type":"input_file","file_data":{"file_id":"file_1"}}]`,
		`"tools":[{"type":"file_search","vector_store_ids":"vs_1"}]`,
		`"tools":[{"type":"code_interpreter","file_ids":{"file_id":"file_1"}}]`,
		`"tools":[{"type":"code_interpreter","container":{"type":"auto","file_ids":null}}]`,
		`"tools":[{"type":"code_interpreter","container":""}]`,
		`"tools":[{"type":"image_generation","input_image_mask":null}]`,
		`"tools":[{"type":"image_generation","input_image_mask":{"image_url":null}}]`,
		`"input":[{"type":"computer_call_output","output":{"type":"computer_screenshot","image_url":null}}]`,
	} {
		if _, err := ScanRequest([]byte(`{"model":"m",`+fields+`}`), 8192); err == nil || err.Code != "invalid_request_metadata" {
			t.Fatalf("资源结构的非法类型未拒绝：字段=%s，错误=%v", fields, err)
		}
	}
}

func TestRequestUnknownResourceSelectorsAreRejected(t *testing.T) {
	for _, fields := range []string{
		`"tools":[{"type":"code_interpreter","container":{"id":"cntr_1"}}]`,
		`"tools":[{"type":"code_interpreter","container":{"type":"auto","id":"cntr_1"}}]`,
		`"tools":[{"type":"code_interpreter","container":{"type":"existing"}}]`,
		`"tools":[{"type":"code_interpreter","container":{"type":"auto","files":["file_1"]}}]`,
		`"tools":[{"type":"code_interpreter","files":[{"file_id":"file_1"}]}]`,
		`"input":[{"type":"input_file","file":{"id":"file_1"}}]`,
		`"input":[{"type":"computer_call_output","output":{"type":"future_screenshot","file_id":"file_1"}}]`,
		`"input":[{"type":"computer_call_output","output":{"type":"computer_screenshot","files":["file_1"]}}]`,
		`"tools":[{"type":"code_interpreter","environment":{"type":"container_reference","container_id":"cntr_1"}}]`,
	} {
		if _, err := ScanRequest([]byte(`{"model":"m",`+fields+`}`), 8192); err == nil || err.Code != "unsupported_request_mode" {
			t.Fatalf("未知资源选择未明确拒绝：字段=%s，错误=%v", fields, err)
		}
	}
}

func TestRequestUnknownInputExtensionPreservesOrdinaryFields(t *testing.T) {
	raw := []byte(`{"model":"m","input":[{"type":"vendor_payload","files":{"file_id":"business_1"},"environment":{"container_id":"business_2"}}],"tools":[{"type":"vendor_tool","environment":{"container_id":"business_3"}}]}`)
	facts, err := ScanRequest(raw, 8192)
	if err != nil {
		t.Fatal(err)
	}
	assertResourceFacts(t, facts, ReplayUnknown, nil, nil)
	changed := ReplaceModel(raw, facts, "upstream")
	if !bytes.HasSuffix(changed, raw[facts.ModelSpan.End:]) {
		t.Fatal("未知扩展的普通业务字段未保留原字节")
	}
}

func TestRequestResourceScanningRetainsBoundedState(t *testing.T) {
	for _, fields := range []string{
		`"input":[{"type":"code_interpreter_call","container_id":"` + strings.Repeat("x", 8192) + `"}]`,
		`"input":[{"type":"computer_call_output","output":{"type":"computer_screenshot","image_url":"data:image/png;base64,` + strings.Repeat("x", 8192) + `"}}]`,
		`"tools":[{"type":"code_interpreter","container":{"type":"auto","file_ids":[` + strings.TrimSuffix(strings.Repeat(`"file_1",`, 200), ",") + `]}}]`,
	} {
		if _, err := ScanRequest([]byte(`{"model":"m",`+fields+`}`), 1024); err != nil {
			t.Fatalf("借用引用正文或重复资源类型消耗了无界状态：%v", err)
		}
	}
	fields := `"input":[{"type":"computer_call_output","output":{"type":"` + strings.Repeat("x", 8192) + `"}}]`
	if _, err := ScanRequest([]byte(`{"model":"m",`+fields+`}`), 1024); err == nil || err.Code != "gateway_capacity_exceeded" {
		t.Fatalf("截图受控类型未遵守状态预算：%v", err)
	}
}
