package protocol

import (
	"bytes"
	"encoding/json"
	"strings"
)

type requestScan struct {
	facts              RequestFacts
	limit, used, depth int
	err                *GatewayError
}

func requestError(code, message string) *GatewayError {
	return &GatewayError{Status: 400, Code: code, Message: message}
}
func (s *requestScan) capacity() {
	s.err = &GatewayError{Status: 503, Code: "gateway_capacity_exceeded", Message: "必要请求元数据超出可用缓冲额度。"}
}
func (s *requestScan) charge(n int) bool {
	if n < 0 || n > s.limit-s.used {
		s.capacity()
		return false
	}
	s.used += n
	return true
}

// controlText 在解码前预占完整字符串区间；调用方按持有作用域归还临时额度。
func (s *requestScan) controlText(v jsonView, held *int) string {
	if s.err != nil {
		return ""
	}
	if v.kind() != '"' {
		s.err = requestError("invalid_request_metadata", "必要控制字段必须为字符串。")
		return ""
	}
	n := v.span.End - v.span.Start
	if n > s.limit-s.used {
		s.capacity()
		return ""
	}
	// 含转义时解码器还会临时创建解码字节数组，同时计入字符串与临时数组。
	if bytes.IndexByte(v.bytes(), '\\') >= 0 {
		if n > (s.limit-s.used-4)/2 {
			s.capacity()
			return ""
		}
		n = 2*n + 4
	}
	if !s.charge(n) {
		return ""
	}
	*held += n
	value, _ := v.text()
	return value
}
func (s *requestScan) tighten(replay ReplayConstraint, reason string) {
	if s.facts.Replay == ReplayForbidden {
		return
	}
	if replay == ReplayForbidden || s.facts.Replay == ReplayAllowed {
		s.facts.Replay = replay
		s.facts.ReplayReason = reason
	}
}
func (s *requestScan) add(dst *[]string, value string) {
	for _, v := range *dst {
		if v == value {
			return
		}
	}
	if s.charge(len(value) + 24) {
		*dst = append(*dst, value)
	}
}
func (s *requestScan) capability(value string) { s.add(&s.facts.Capabilities, value) }
func (s *requestScan) resource(value string)   { s.add(&s.facts.ResourceTypes, value) }
func keys(list string) map[string]bool {
	out := make(map[string]bool)
	for _, k := range strings.Fields(list) {
		out[k] = true
	}
	return out
}

var responseControls = keys("model input instructions stream stream_options background store previous_response_id conversation prompt tools tool_choice parallel_tool_calls reasoning text include temperature top_p max_output_tokens max_tool_calls truncation metadata user safety_identifier service_tier prompt_cache_key prompt_cache_retention prompt_cache_options context_management")
var necessaryTop = keys("model input stream stream_options background previous_response_id conversation prompt tools tool_choice max_output_tokens prompt_cache_options context_management")

// ScanRequest 校验必要元数据并保留模型字符串的原始字节区间。
func ScanRequest(body []byte, maxStateBytes int) (RequestFacts, *GatewayError) {
	s := requestScan{limit: maxStateBytes, facts: RequestFacts{Mode: "non_stream", Replay: ReplayAllowed}}
	v, valid := wholeJSON(body)
	if !valid || v.kind() != '{' {
		return RequestFacts{}, &GatewayError{Status: 415, Code: "unsupported_request_format", Message: "请求正文必须为合法的 UTF-8 JSON 对象。"}
	}
	if !s.charge(128) {
		return RequestFacts{}, s.err
	}
	var previous, conversation bool
	s.object(v, necessaryTop, func(k string, f jsonView) {
		if !responseControls[k] {
			s.tighten(ReplayUnknown, "unknown_request_control")
			return
		}
		switch k {
		case "model":
			var held int
			model := s.controlText(f, &held)
			s.used -= held
			if s.err != nil {
				return
			}
			if model == "" {
				s.err = requestError("invalid_request_metadata", "model 必须为非空字符串。")
				return
			}
			if s.charge(len(model)) {
				s.facts.Model = model
				s.facts.ModelSpan = f.span
			}
		case "stream", "background":
			if f.null() {
				return
			}
			enabled, ok := f.boolean()
			if !ok {
				s.err = requestError("invalid_request_metadata", k+" 必须为布尔值或 null。")
				return
			}
			if k == "stream" && enabled {
				s.facts.Mode = "stream"
			}
			if k == "background" && enabled {
				s.err = requestError("unsupported_request_mode", "尚不支持后台 Responses 请求。")
			}
		case "max_output_tokens":
			if f.null() {
				return
			}
			n, ok := f.integer()
			if !ok || n < 0 {
				s.err = requestError("invalid_request_metadata", "max_output_tokens 必须为非负可表示整数或 null。")
				return
			}
			if n == 0 {
				s.err = requestError("unsupported_request_mode", "尚不支持零输出预热请求。")
			}
		case "previous_response_id":
			previous = s.reference(f, "openai.response")
		case "conversation":
			if f.null() {
				return
			}
			if f.kind() == '"' {
				conversation = s.reference(f, "openai.conversation")
			} else if f.kind() == '{' {
				s.object(f, keys("id"), func(k string, val jsonView) {
					if k == "id" {
						conversation = s.reference(val, "openai.conversation")
					} else {
						s.err = requestError("unsupported_request_mode", "无法解释 conversation 控制对象。")
					}
				})
				if !conversation && s.err == nil {
					s.err = requestError("unsupported_request_mode", "conversation 必须明确引用现有对象。")
				}
			} else {
				s.err = requestError("invalid_request_metadata", "conversation 必须为字符串、对象或 null。")
			}
			if conversation {
				s.tighten(ReplayForbidden, "conversation_append")
			}
		case "prompt":
			if !f.null() {
				s.tighten(ReplayUnknown, "unexpanded_prompt")
				s.object(f, keys("id version variables"), func(k string, val jsonView) {
					if k == "id" {
						s.reference(val, "openai.prompt")
					}
					if k != "id" && k != "version" && k != "variables" {
						s.tighten(ReplayUnknown, "unknown_prompt_control")
					}
				})
			}
		case "input":
			s.input(f)
		case "tools":
			s.tools(f)
		case "tool_choice":
			s.toolChoice(f)
		case "stream_options":
			s.controls(f, keys("include_obfuscation"), nil)
		case "prompt_cache_options":
			s.controls(f, keys("mode ttl prewarm comparison_response_id"), func(k string, val jsonView) {
				if k == "comparison_response_id" {
					s.reference(val, "openai.response")
				}
				if k == "prewarm" {
					n, ok := val.boolean()
					if !ok {
						s.err = requestError("invalid_request_metadata", "prewarm 必须为布尔值。")
					} else if n {
						s.err = requestError("unsupported_request_mode", "尚不支持缓存预热。")
					}
				}
			})
		case "context_management":
			if !f.array(func(item jsonView) bool {
				var held int
				defer func() { s.used -= held }()
				typ := ""
				s.controls(item, keys("type compact_threshold"), func(k string, val jsonView) {
					if k == "type" {
						typ = s.controlText(val, &held)
					}
				})
				if typ != "compaction" {
					s.tighten(ReplayUnknown, "unknown_context_control")
				}
				return s.err == nil
			}) && s.err == nil {
				s.tighten(ReplayUnknown, "unknown_context_control")
			}
		}
	})
	if s.err == nil && s.facts.Model == "" {
		s.err = requestError("invalid_request_metadata", "请求缺少非空 model。")
	}
	if s.err == nil && previous && conversation {
		s.err = requestError("ambiguous_request_metadata", "previous_response_id 与 conversation 不能同时非空。")
	}
	return s.facts, s.err
}
func (s *requestScan) object(v jsonView, necessary map[string]bool, visit func(string, jsonView)) {
	if s.err != nil {
		return
	}
	s.depth++
	defer func() { s.depth-- }()
	if s.depth > 128 {
		s.capacity()
		return
	}
	seen := make(map[string]bool)
	held := 0
	defer func() { s.used -= held }()
	ok := v.objectWithKeyLimit(func() int { return s.limit - s.used }, func(k string, val jsonView) bool {
		if necessary[k] {
			if seen[k] {
				s.err = requestError("ambiguous_request_metadata", "必要元数据字段重复："+k+"。")
				return false
			}
			n := len(k) + 48
			if !s.charge(n) {
				return false
			}
			held += n
			seen[k] = true
		}
		visit(k, val)
		return s.err == nil
	})
	if !ok && s.err == nil {
		if v.kind() != '{' {
			s.err = requestError("invalid_request_metadata", "必要控制结构必须为对象。")
		} else {
			s.capacity()
		}
	}
}
func (s *requestScan) controls(v jsonView, allowed map[string]bool, visit func(string, jsonView)) {
	if v.null() {
		return
	}
	s.object(v, allowed, func(k string, f jsonView) {
		if !allowed[k] {
			s.tighten(ReplayUnknown, "unknown_nested_control")
		}
		if visit != nil {
			visit(k, f)
		}
	})
}
func (s *requestScan) reference(v jsonView, typ string) bool {
	if v.null() {
		return false
	}
	if v.kind() != '"' {
		s.err = requestError("invalid_request_metadata", "资源引用必须为字符串或 null。")
		return false
	}
	if v.span.End-v.span.Start == 2 {
		return false
	}
	s.resource(typ)
	return true
}
func (s *requestScan) input(v jsonView) {
	if v.kind() == '"' || v.null() {
		return
	}
	if !v.array(func(item jsonView) bool { s.inputItem(item); return s.err == nil }) && s.err == nil {
		s.err = requestError("invalid_request_metadata", "input 必须为字符串或数组。")
	}
}
func (s *requestScan) inputItem(v jsonView) {
	if v.kind() == '"' {
		return
	}
	typ := ""
	var held int
	defer func() { s.used -= held }()
	s.object(v, keys("type"), func(k string, val jsonView) {
		if k == "type" {
			typ = s.controlText(val, &held)
		}
	})
	if s.err != nil {
		return
	}
	known := keys("type role content id status")
	switch typ {
	case "", "message":
		s.object(v, known, func(k string, val jsonView) {
			if k == "content" {
				if val.kind() == '"' {
					return
				}
				if !val.array(func(part jsonView) bool { s.inputItem(part); return s.err == nil }) && s.err == nil {
					s.err = requestError("invalid_request_metadata", "message.content 必须为字符串或内容数组。")
				}
			}
		})
	case "input_text", "output_text", "refusal":
	case "input_image":
		s.capability("image_input")
		s.object(v, keys("type file_id image_url detail"), func(k string, val jsonView) {
			if k == "file_id" {
				s.reference(val, "openai.file")
			}
		})
	case "input_file":
		s.capability("file_input")
		s.object(v, keys("type file_id file_data file_url filename"), func(k string, val jsonView) {
			if k == "file_id" {
				s.reference(val, "openai.file")
			}
		})
	case "input_audio":
		s.capability("audio_input")
	case "input_video":
		s.capability("video_input")
	case "item_reference":
		s.object(v, keys("type id"), func(k string, val jsonView) {
			if k == "id" {
				s.reference(val, "openai.item")
			}
		})
	case "function_call", "custom_tool_call":
		s.capability("tool_calling")
	case "function_call_output", "custom_tool_call_output":
		s.capability("tool_calling")
		s.object(v, keys("type output"), func(k string, val jsonView) {
			if k == "output" && val.kind() == '[' {
				val.array(func(part jsonView) bool { s.inputItem(part); return s.err == nil })
			}
		})
	case "reasoning", "compaction":
		s.object(v, keys("type encrypted_content"), func(k string, val jsonView) {
			if k == "encrypted_content" {
				s.reference(val, "openai.opaque")
			}
		})
	default:
		s.tighten(ReplayUnknown, "unknown_input_type")
	}
}
func (s *requestScan) tools(v jsonView) {
	if v.null() {
		return
	}
	if !v.array(func(tool jsonView) bool {
		var held int
		defer func() { s.used -= held }()
		typ := ""
		s.object(tool, keys("type container vector_store_ids files file_ids"), func(k string, val jsonView) {
			if k == "type" {
				typ = s.controlText(val, &held)
			}
			if k == "container" {
				s.container(val)
			}
			if k == "vector_store_ids" {
				if !val.array(func(ref jsonView) bool { s.reference(ref, "openai.vector_store"); return s.err == nil }) && s.err == nil {
					s.err = requestError("invalid_request_metadata", "vector_store_ids 必须为引用数组。")
				}
			}
			if k == "file_ids" {
				val.array(func(ref jsonView) bool { s.reference(ref, "openai.file"); return s.err == nil })
			}
		})
		if s.err != nil {
			return false
		}
		switch typ {
		case "function", "custom":
			s.capability("tool_calling")
			allowed := keys("type name description parameters strict format")
			s.controls(tool, allowed, nil)
		case "web_search", "web_search_preview", "web_search_preview_2025_03_11", "file_search", "code_interpreter", "computer_use_preview", "image_generation", "mcp":
			s.capability("server_tools")
			s.tighten(ReplayForbidden, "server_tool_execution")
			if typ == "image_generation" {
				s.capability("image_output")
			}
		default:
			s.tighten(ReplayUnknown, "unknown_tool_type")
		}
		return s.err == nil
	}) && s.err == nil {
		s.err = requestError("invalid_request_metadata", "tools 必须为数组或 null。")
	}
}
func (s *requestScan) container(v jsonView) {
	s.tighten(ReplayForbidden, "mutable_execution_environment")
	if v.kind() == '"' {
		s.reference(v, "openai.container")
		return
	}
	if v.kind() != '{' {
		s.err = requestError("unsupported_request_mode", "无法解释 container 控制。")
		return
	}
	var held int
	defer func() { s.used -= held }()
	typ := ""
	found := false
	s.object(v, keys("type id file_ids memory_limit"), func(k string, val jsonView) {
		switch k {
		case "id":
			found = s.reference(val, "openai.container")
		case "file_ids":
			if !val.array(func(ref jsonView) bool { s.reference(ref, "openai.file"); return s.err == nil }) && s.err == nil {
				s.err = requestError("invalid_request_metadata", "container.file_ids 必须为引用数组。")
			}
		case "type":
			typ = s.controlText(val, &held)
		case "memory_limit":
		default:
			s.err = requestError("unsupported_request_mode", "无法解释 container 选择对象。")
		}
	})
	if typ != "auto" && !found && s.err == nil {
		s.err = requestError("unsupported_request_mode", "container 未提供明确的创建方式或引用。")
	}
}
func (s *requestScan) toolChoice(v jsonView) {
	if v.null() {
		return
	}
	var held int
	defer func() { s.used -= held }()
	if v.kind() == '"' {
		value := s.controlText(v, &held)
		if value != "auto" && value != "none" && value != "required" {
			s.tighten(ReplayUnknown, "unknown_tool_choice")
		}
		return
	}
	typ := ""
	s.object(v, keys("type name mode tools"), func(k string, val jsonView) {
		if k == "type" {
			typ = s.controlText(val, &held)
		}
	})
	if s.err != nil {
		return
	}
	switch typ {
	case "function", "custom":
		s.controls(v, keys("type name"), nil)
	case "allowed_tools":
		s.controls(v, keys("type mode tools"), func(k string, val jsonView) {
			if k == "tools" {
				if !val.array(func(choice jsonView) bool { s.toolChoice(choice); return s.err == nil }) && s.err == nil {
					s.tighten(ReplayUnknown, "unknown_tool_choice")
				}
			}
			if k == "mode" {
				var modeHeld int
				mode := s.controlText(val, &modeHeld)
				defer func() { s.used -= modeHeld }()
				if mode != "auto" && mode != "required" {
					s.tighten(ReplayUnknown, "unknown_tool_choice")
				}
			}
		})
	default:
		s.tighten(ReplayUnknown, "unknown_tool_choice")
	}
}

// ReplaceModel 仅替换已扫描的模型字符串，模型相同时保留原始切片。
func ReplaceModel(body []byte, facts RequestFacts, upstream string) []byte {
	if upstream == facts.Model {
		return body
	}
	span := facts.ModelSpan
	if span.Start < 0 || span.End > len(body) || span.Start >= span.End {
		return body
	}
	replacement, _ := json.Marshal(upstream)
	result := make([]byte, 0, len(body)-(span.End-span.Start)+len(replacement))
	result = append(result, body[:span.Start]...)
	result = append(result, replacement...)
	return append(result, body[span.End:]...)
}
