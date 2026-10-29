package protocol

import (
	"bytes"
	"io"
	"mime"
	"strings"
	"time"
)

type itemState struct {
	id, typ string
	name    bool
}

// ResponsesObserver 顺序观察单次响应，保留有界事实并与原字节复制分离。
type ResponsesObserver struct {
	opts                                ObserverOptions
	facts                               ResponseFacts
	buffer                              []byte
	bufferHeld, stateHeld               int64
	errorHeld, usageHeld                int64
	items                               map[int64]itemState
	rawBytes                            int64
	available, ended, closed, skipEvent bool
	lineHasBytes, previousCR            bool
	lineHead                            [32]byte
	lineHeadBytes                       int
	lineHeadOverflow                    bool
	frameData, frameHeartbeat           bool
	lastTime                            time.Time
	refusal, handoff                    bool
	terminalStatus                      string
}

func NewResponsesObserver(opts ObserverOptions) *ResponsesObserver {
	o := &ResponsesObserver{opts: opts, items: make(map[int64]itemState), facts: ResponseFacts{Terminal: "absent", Result: "pending", Output: "unknown", Coverage: "complete", TerminalCoverage: "complete", OutputCoverage: "complete", UsageCoverage: "complete", ErrorCoverage: "complete", Replay: opts.Replay, Usage: Usage{Status: "missing"}}}
	if o.facts.Replay == "" {
		o.facts.Replay = ReplayUnknown
	}
	media, _, err := mime.ParseMediaType(opts.ContentType)
	identity := opts.ContentEncoding == "" || strings.EqualFold(opts.ContentEncoding, "identity")
	jsonType := media == "application/json" || strings.HasPrefix(media, "application/") && strings.HasSuffix(media, "+json")
	if opts.StatusCode >= 400 {
		o.available = err == nil && identity && jsonType
	} else {
		o.available = err == nil && identity && (opts.Mode == "stream" && media == "text/event-stream" || opts.Mode == "non_stream" && jsonType)
	}
	if !o.available {
		reason := "unsupported_response_format"
		if !identity {
			reason = "unsupported_response_encoding"
		}
		o.unavailable(reason)
	}
	return o
}
func (o *ResponsesObserver) reserve(n int64) bool {
	return n >= 0 && (o.opts.Budget == nil || o.opts.Budget.Reserve(n))
}
func (o *ResponsesObserver) release(n int64) {
	if n > 0 && o.opts.Budget != nil {
		o.opts.Budget.Release(n)
	}
}
func (o *ResponsesObserver) grow(p *[]byte, held *int64, addition []byte, limit int) bool {
	if limit <= 0 || len(addition) > limit-len(*p) {
		return false
	}
	need := len(*p) + len(addition)
	if need > cap(*p) {
		next := cap(*p) * 2
		if next < need {
			next = need
		}
		if next > limit {
			next = limit
		}
		// 扩容期间旧数组仍被持有，先预占完整新数组，再归还旧容量。
		if !o.reserve(int64(next)) {
			return false
		}
		replacement := make([]byte, len(*p), next)
		copy(replacement, *p)
		*p = replacement
		o.release(*held)
		*held = int64(next)
	}
	*p = append(*p, addition...)
	return true
}
func (o *ResponsesObserver) clearBuffer() { o.release(o.bufferHeld); o.bufferHeld = 0; o.buffer = nil }
func (o *ResponsesObserver) unavailable(reason string) {
	o.facts.Coverage = "unavailable"
	o.facts.TerminalCoverage = "unavailable"
	o.facts.OutputCoverage = "unavailable"
	o.facts.UsageCoverage = "unavailable"
	o.facts.ErrorCoverage = "unavailable"
	if o.facts.Terminal == "absent" {
		o.facts.Terminal = "unavailable"
	}
	o.reason(reason)
}
func (o *ResponsesObserver) partial(reason string) {
	if o.facts.Coverage != "unavailable" {
		o.facts.Coverage = "partial"
	}
	if o.facts.OutputCoverage == "complete" {
		o.facts.OutputCoverage = "partial"
	}
	if o.facts.TerminalCoverage == "complete" {
		o.facts.TerminalCoverage = "partial"
	}
	o.reason(reason)
}
func (o *ResponsesObserver) reason(reason string) {
	if o.facts.Reason == "" {
		o.facts.Reason = reason
	}
}
func (o *ResponsesObserver) tighten(replay ReplayConstraint, reason string) {
	if o.facts.Replay == ReplayForbidden {
		return
	}
	if replay == ReplayForbidden || o.facts.Replay == ReplayAllowed {
		o.facts.Replay = replay
		o.facts.ReplayReason = reason
	}
}
func (o *ResponsesObserver) content(kind string, at time.Time) {
	o.facts.Output = "nonempty"
	if o.facts.FirstContent.IsZero() {
		o.facts.FirstContent = at
		o.facts.ContentKind = kind
	}
}

// Feed 接收已读取字节及读取时间，观察降级不会阻止调用者继续复制。
func (o *ResponsesObserver) Feed(raw []byte, at time.Time) {
	if o.ended || o.closed {
		return
	}
	o.rawBytes += int64(len(raw))
	o.lastTime = at
	if !o.available {
		return
	}
	if o.opts.Mode != "stream" || o.opts.StatusCode >= 400 {
		limit := o.opts.MaxJSONBytes
		if o.opts.StatusCode >= 400 && o.opts.MaxErrorBytes < limit {
			limit = o.opts.MaxErrorBytes
		}
		if !o.grow(&o.buffer, &o.bufferHeld, raw, limit) {
			o.available = false
			o.unavailable("json_observation_limit")
			o.clearBuffer()
		}
		return
	}
	for _, b := range raw {
		// CRLF 和单独 CR 均作为行结束；帧字节本身由 relay 原样转发。
		if o.previousCR {
			o.previousCR = false
			if b == '\n' {
				continue
			}
		}
		if b == '\r' || b == '\n' {
			if o.lineHasBytes {
				o.frameLine()
			}
			if !o.skipEvent && !o.grow(&o.buffer, &o.bufferHeld, []byte{'\n'}, o.opts.MaxEventBytes) {
				o.skipEvent = true
				o.partial("sse_event_observation_limit")
				o.clearBuffer()
			}
			if !o.lineHasBytes {
				// 帧是否可派发与 JSON 载荷观察分离，超长载荷也不丢失首事件时间。
				if o.frameData && !o.frameHeartbeat && o.facts.FirstEvent.IsZero() {
					o.facts.FirstEvent = at
				}
				if !o.skipEvent {
					o.dispatch(at)
				}
				o.clearBuffer()
				o.skipEvent = false
				o.frameData = false
				o.frameHeartbeat = false
			}
			o.lineHasBytes = false
			o.lineHeadBytes = 0
			o.lineHeadOverflow = false
			if b == '\r' {
				o.previousCR = true
			}
			continue
		}
		o.lineHasBytes = true
		if o.lineHeadBytes < len(o.lineHead) {
			o.lineHead[o.lineHeadBytes] = b
			o.lineHeadBytes++
		} else {
			o.lineHeadOverflow = true
		}
		if !o.skipEvent && !o.grow(&o.buffer, &o.bufferHeld, []byte{b}, o.opts.MaxEventBytes) {
			o.skipEvent = true
			o.partial("sse_event_observation_limit")
			o.clearBuffer()
		}
	}
}

// frameLine 仅保留有限的字段名前缀，data 载荷从不进入分帧状态。
func (o *ResponsesObserver) frameLine() {
	line := o.lineHead[:o.lineHeadBytes]
	if len(line) == 0 || line[0] == ':' {
		return
	}
	key, value, colon := bytes.Cut(line, []byte{':'})
	if !colon && o.lineHeadOverflow {
		return
	}
	if len(value) > 0 && value[0] == ' ' {
		value = value[1:]
	}
	switch string(key) {
	case "data":
		o.frameData = true
	case "event":
		o.frameHeartbeat = !o.lineHeadOverflow && (string(value) == "ping" || string(value) == "heartbeat")
	}
}
func (o *ResponsesObserver) dispatch(at time.Time) {
	var data []byte
	var held int64
	defer func() { o.release(held) }()
	name := ""
	hasData := false
	for start := 0; start < len(o.buffer); {
		end := bytes.IndexByte(o.buffer[start:], '\n')
		if end < 0 {
			break
		}
		end += start
		line := o.buffer[start:end]
		start = end + 1
		if len(line) == 0 || line[0] == ':' {
			continue
		}
		key, value, ok := bytes.Cut(line, []byte{':'})
		if !ok {
			value = nil
		}
		if len(value) > 0 && value[0] == ' ' {
			value = value[1:]
		}
		switch string(key) {
		case "event":
			if len(value) > 128 {
				o.partial("event_name_unavailable")
				return
			}
			name = string(value)
		case "data":
			if hasData && !o.grow(&data, &held, []byte{'\n'}, o.opts.MaxEventBytes) {
				o.partial("sse_payload_capacity")
				return
			}
			if !o.grow(&data, &held, value, o.opts.MaxEventBytes) {
				o.partial("sse_payload_capacity")
				return
			}
			hasData = true
		}
	}
	if !hasData || name == "ping" || name == "heartbeat" {
		return
	}
	if o.facts.FirstEvent.IsZero() {
		o.facts.FirstEvent = at
	}
	v, ok := wholeJSON(data)
	if !ok || v.kind() != '{' {
		o.partial("invalid_sse_json")
		return
	}
	o.event(v, name, at)
}
func (o *ResponsesObserver) validObject(v jsonView) bool {
	if v.kind() != '{' {
		o.partial("invalid_response_structure")
		return false
	}
	seen := make(map[string]bool)
	var held int64
	defer func() { o.release(held) }()
	valid := true
	ok := v.object(o.opts.MaxStateBytes, func(k string, val jsonView) bool {
		n := int64(len(k) + 48)
		if n > int64(o.opts.MaxStateBytes)-o.stateHeld-held || !o.reserve(n) {
			valid = false
			o.partial("observation_state_limit")
			return false
		}
		held += n
		if seen[k] {
			valid = false
			o.partial("ambiguous_response_metadata")
			return false
		}
		seen[k] = true
		return true
	})
	if !ok && valid {
		o.partial("observation_state_limit")
		return false
	}
	return valid
}
func (o *ResponsesObserver) event(v jsonView, name string, at time.Time) {
	if !o.validObject(v) {
		return
	}
	typ := v.stringField("type")
	if typ == "" {
		typ = name
	}
	if name != "" && typ != name {
		o.partial("event_type_conflict")
		return
	}
	switch typ {
	case "response.completed", "response.failed", "response.incomplete":
		response, ok := v.get("response")
		if !ok {
			o.partial("missing_terminal_response")
			return
		}
		expected := strings.TrimPrefix(typ, "response.")
		if response.stringField("status") != expected {
			o.facts.Terminal = "invalid"
			o.partial("terminal_status_conflict")
			return
		}
		o.response(response, at, true)
	case "error":
		o.error(v)
		o.facts.Result = "failed"
	case "response.created", "response.in_progress", "response.queued":
		if response, ok := v.get("response"); ok {
			o.execution(response)
			if usage, ok := response.get("usage"); ok && !usage.null() {
				o.usage(usage)
				o.facts.Usage.Status = "partial"
				o.facts.UsageCoverage = "partial"
			}
		}
	case "response.output_item.added", "response.output_item.done":
		item, ok := v.get("item")
		if !ok {
			o.partial("missing_output_item")
			return
		}
		index, valid := indexField(v, "output_index")
		if !valid {
			o.partial("invalid_output_index")
			return
		}
		if !o.track(index, item) {
			return
		}
		o.outputItem(item, at, typ == "response.output_item.done")
	case "response.output_text.delta", "response.output_text.done", "response.reasoning_summary_text.delta", "response.reasoning_summary_text.done", "response.reasoning_text.delta", "response.reasoning_text.done":
		if !o.contentIndex(v, false) {
			return
		}
		field := "delta"
		if strings.HasSuffix(typ, ".done") {
			field = "text"
		}
		if value, ok := v.get(field); ok && nonemptyString(value) {
			kind := "text"
			if strings.Contains(typ, "reasoning") {
				kind = "reasoning"
			}
			o.content(kind, at)
		} else if !ok || value.kind() != '"' {
			o.partial("invalid_content_delta")
		}
	case "response.refusal.delta", "response.refusal.done":
		if !o.contentIndex(v, false) {
			return
		}
		field := "delta"
		if strings.HasSuffix(typ, ".done") {
			field = "refusal"
		}
		if value, ok := v.get(field); ok && nonemptyString(value) {
			o.refusal = true
			if o.facts.FirstContent.IsZero() {
				o.facts.FirstContent = at
				o.facts.ContentKind = "refusal"
			}
		} else if !ok || value.kind() != '"' {
			o.partial("invalid_refusal_delta")
		}
	case "response.function_call_arguments.delta", "response.function_call_arguments.done", "response.custom_tool_call_input.delta", "response.custom_tool_call_input.done":
		if !o.contentIndex(v, true) {
			return
		}
		field := "delta"
		if strings.HasSuffix(typ, ".done") {
			field = "arguments"
			if strings.Contains(typ, "custom_tool") {
				field = "input"
			}
		}
		if value, ok := v.get(field); ok && nonemptyString(value) {
			o.content("tool_call", at)
		} else if !ok || value.kind() != '"' {
			o.partial("invalid_tool_delta")
		}
	case "response.content_part.added", "response.content_part.done", "response.reasoning_summary_part.added", "response.reasoning_summary_part.done":
		if !o.contentIndex(v, false) {
			return
		}
		if part, ok := v.get("part"); ok && strings.HasSuffix(typ, ".done") {
			o.part(part, at)
		}
	case "response.reasoning_summary.delta", "response.reasoning_summary.done":
		o.partial("unverified_reasoning_event")
	case "response.web_search_call.in_progress", "response.web_search_call.searching", "response.web_search_call.completed", "response.file_search_call.in_progress", "response.file_search_call.searching", "response.file_search_call.completed", "response.code_interpreter_call.in_progress", "response.code_interpreter_call.interpreting", "response.code_interpreter_call.completed", "response.image_generation_call.in_progress", "response.image_generation_call.generating", "response.image_generation_call.completed", "response.mcp_call.in_progress", "response.mcp_call.completed", "response.mcp_call.failed":
		o.tighten(ReplayForbidden, "observed_server_tool_execution")
	default:
		o.partial("unknown_sse_event")
		o.tighten(ReplayUnknown, "unknown_execution_event")
	}
}
func indexField(v jsonView, key string) (int64, bool) {
	value, ok := v.get(key)
	if !ok {
		return 0, false
	}
	n, valid := value.integer()
	return n, valid && n >= 0
}
func nonemptyString(v jsonView) bool { return v.kind() == '"' && v.span.End-v.span.Start > 2 }
func (o *ResponsesObserver) track(index int64, v jsonView) bool {
	if !o.validObject(v) {
		return false
	}
	id := v.stringField("id")
	typ := v.stringField("type")
	if len(id)+len(typ) > o.opts.MaxStateBytes {
		o.partial("observation_state_limit")
		return false
	}
	if old, ok := o.items[index]; ok {
		if old.id != "" && id != "" && old.id != id || old.typ != "" && typ != old.typ {
			o.partial("conflicting_output_item")
			return false
		}
		return true
	}
	n := int64(96 + len(id) + len(typ))
	if n > int64(o.opts.MaxStateBytes)-o.stateHeld || !o.reserve(n) {
		o.partial("observation_state_limit")
		return false
	}
	o.stateHeld += n
	o.items[index] = itemState{id: id, typ: typ, name: v.stringField("name") != ""}
	return true
}
func (o *ResponsesObserver) contentIndex(v jsonView, tool bool) bool {
	index, ok := indexField(v, "output_index")
	if !ok {
		o.partial("invalid_output_index")
		return false
	}
	id := v.stringField("item_id")
	if id == "" {
		o.partial("missing_item_id")
		return false
	}
	if !tool {
		if _, ok := indexField(v, "content_index"); !ok {
			if _, summary := indexField(v, "summary_index"); !summary {
				o.partial("invalid_content_index")
				return false
			}
		}
	}
	if old, ok := o.items[index]; ok {
		if old.id != "" && old.id != id {
			o.partial("conflicting_output_item")
			return false
		}
		if tool && old.typ != "function_call" && old.typ != "custom_tool_call" {
			o.partial("tool_item_type_conflict")
			return false
		}
		return true
	}
	// 文本增量可以先于完整条目出现；工具增量必须已经关联到工具起始。
	if tool {
		o.partial("unassociated_tool_delta")
		return false
	}
	n := int64(96 + len(id))
	if n > int64(o.opts.MaxStateBytes)-o.stateHeld || !o.reserve(n) {
		o.partial("observation_state_limit")
		return false
	}
	o.stateHeld += n
	o.items[index] = itemState{id: id}
	return true
}
func (o *ResponsesObserver) execution(v jsonView) {
	if !o.validObject(v) {
		o.tighten(ReplayUnknown, "ambiguous_response_execution_control")
		return
	}
	if conv, ok := v.get("conversation"); ok && !conv.null() {
		if conv.kind() == '{' && !o.validObject(conv) {
			o.tighten(ReplayUnknown, "ambiguous_response_conversation_control")
		} else if nonemptyString(conv) || conv.kind() == '{' && conv.stringField("id") != "" {
			o.tighten(ReplayForbidden, "observed_conversation_append")
		} else if conv.kind() != '"' {
			o.tighten(ReplayUnknown, "unknown_response_conversation_control")
		}
	}
	if background, ok := v.get("background"); ok && !background.null() {
		enabled, valid := background.boolean()
		if !valid || enabled {
			o.tighten(ReplayUnknown, "unexpected_response_background_control")
		}
	}
	if tools, ok := v.get("tools"); ok {
		if !tools.null() && !tools.array(func(tool jsonView) bool {
			if !o.validObject(tool) {
				o.tighten(ReplayUnknown, "ambiguous_response_tool_control")
				return true
			}
			typ := tool.stringField("type")
			switch typ {
			case "function", "custom":
				allowed := keys("type name description parameters strict format")
				tool.object(o.opts.MaxStateBytes, func(k string, _ jsonView) bool {
					if !allowed[k] {
						o.tighten(ReplayUnknown, "unknown_response_tool_control")
					}
					return true
				})
			case "web_search", "web_search_preview", "file_search", "code_interpreter", "computer_use_preview", "image_generation", "mcp":
				o.tighten(ReplayForbidden, "observed_server_tool_execution")
			default:
				o.tighten(ReplayUnknown, "unknown_response_tool")
			}
			return true
		}) {
			o.tighten(ReplayUnknown, "unknown_response_tools_control")
		}
	}
	if choice, ok := v.get("tool_choice"); ok {
		o.executionChoice(choice, 0)
	}
}
func (o *ResponsesObserver) executionChoice(v jsonView, depth int) {
	if v.null() {
		return
	}
	if depth >= 32 {
		o.tighten(ReplayUnknown, "response_tool_choice_state_limit")
		return
	}
	if v.kind() == '"' {
		value := ""
		if v.span.End-v.span.Start <= 64 {
			value, _ = v.text()
		}
		if value != "auto" && value != "none" && value != "required" {
			o.tighten(ReplayUnknown, "unknown_response_tool_choice")
		}
		return
	}
	if !o.validObject(v) {
		o.tighten(ReplayUnknown, "ambiguous_response_tool_choice")
		return
	}
	typ := v.stringField("type")
	allowed := keys("type name")
	switch typ {
	case "function", "custom":
		if v.stringField("name") == "" {
			o.tighten(ReplayUnknown, "unknown_response_tool_choice")
		}
	case "allowed_tools":
		allowed = keys("type mode tools")
		mode := v.stringField("mode")
		if mode != "auto" && mode != "required" {
			o.tighten(ReplayUnknown, "unknown_response_tool_choice")
		}
		if tools, ok := v.get("tools"); !ok || !tools.array(func(choice jsonView) bool { o.executionChoice(choice, depth+1); return true }) {
			o.tighten(ReplayUnknown, "unknown_response_tool_choice")
		}
	default:
		o.tighten(ReplayUnknown, "unknown_response_tool_choice")
	}
	v.object(o.opts.MaxStateBytes, func(k string, _ jsonView) bool {
		if !allowed[k] {
			o.tighten(ReplayUnknown, "unknown_response_tool_choice")
		}
		return true
	})
}
func (o *ResponsesObserver) response(v jsonView, at time.Time, terminal bool) {
	if !o.validObject(v) {
		o.tighten(ReplayUnknown, "ambiguous_response_execution_control")
		return
	}
	o.execution(v)
	if v.stringField("object") != "response" {
		o.partial("invalid_response_object")
		return
	}
	status := v.stringField("status")
	if err, ok := v.get("error"); ok && !err.null() {
		o.error(err)
	}
	if usage, ok := v.get("usage"); ok && !usage.null() {
		o.usage(usage)
	}
	output, ok := v.get("output")
	if !ok || !output.array(func(item jsonView) bool { o.outputItem(item, at, true); return true }) {
		o.partial("missing_or_invalid_output")
	}
	if terminal {
		if o.terminalStatus != "" && o.terminalStatus != status {
			o.facts.Terminal = "invalid"
			o.partial("conflicting_terminal_status")
			return
		}
		o.terminalStatus = status
		o.facts.Terminal = "observed"
	}
	switch status {
	case "completed":
		if o.facts.Result != "failed" {
			o.facts.Result = "completed"
		}
	case "failed":
		o.facts.Result = "failed"
		o.facts.Error.Present = true
	case "incomplete":
		details, _ := v.get("incomplete_details")
		switch details.stringField("reason") {
		case "max_output_tokens":
			o.facts.Result = "limited"
		case "content_filter":
			o.facts.Result = "filtered"
		default:
			o.facts.Result = "unknown"
			o.partial("unknown_incomplete_reason")
		}
	case "cancelled":
		o.facts.Result = "cancelled"
	case "queued", "in_progress":
		o.facts.Result = "accepted"
		o.tighten(ReplayUnknown, "unexpected_background_result")
		o.reason("unexpected_background_result")
	default:
		o.facts.Result = "unknown"
		o.partial("unknown_response_status")
	}
	if o.facts.Result == "completed" {
		if o.refusal {
			o.facts.Result = "refusal"
		} else if o.handoff {
			o.facts.Result = "tool_handoff"
		}
	}
	if o.facts.Error.Present {
		o.facts.Result = "failed"
	}
}
func (o *ResponsesObserver) outputItem(v jsonView, at time.Time, complete bool) {
	if !o.validObject(v) {
		return
	}
	typ := v.stringField("type")
	allowed := map[string]bool(nil)
	switch typ {
	case "message":
		allowed = keys("type id role status content")
	case "function_call":
		allowed = keys("type id call_id name arguments status")
	case "custom_tool_call":
		allowed = keys("type id call_id name input status")
	case "reasoning":
		allowed = keys("type id status summary content encrypted_content")
	case "compaction":
		allowed = keys("type id encrypted_content")
	}
	if allowed != nil {
		v.object(o.opts.MaxStateBytes, func(k string, _ jsonView) bool {
			if !allowed[k] {
				o.partial("unknown_output_field")
			}
			return true
		})
	}
	switch typ {
	case "message":
		if content, ok := v.get("content"); ok {
			if !content.array(func(part jsonView) bool { o.part(part, at); return true }) {
				o.partial("invalid_message_content")
			}
		} else if complete {
			o.partial("missing_message_content")
		}
	case "function_call", "custom_tool_call":
		field := "arguments"
		if typ == "custom_tool_call" {
			field = "input"
		}
		value, ok := v.get(field)
		if complete && v.stringField("name") != "" && v.stringField("call_id") != "" && ok && value.kind() == '"' {
			o.content("tool_call", at)
			o.handoff = true
		} else if complete {
			o.partial("incomplete_tool_call")
		}
	case "reasoning":
		if encrypted, ok := v.get("encrypted_content"); ok && nonemptyString(encrypted) {
			o.content("opaque_reasoning", at)
		}
		for _, key := range []string{"summary", "content"} {
			if content, ok := v.get(key); ok {
				if !content.array(func(part jsonView) bool { o.part(part, at); return true }) {
					o.partial("invalid_reasoning_content")
				}
			}
		}
	case "compaction":
		if encrypted, ok := v.get("encrypted_content"); ok && nonemptyString(encrypted) {
			o.content("opaque_compaction", at)
		} else if complete {
			o.partial("unknown_compaction_payload")
		}
	case "web_search_call", "file_search_call", "code_interpreter_call", "computer_call", "image_generation_call", "mcp_call":
		o.tighten(ReplayForbidden, "observed_server_tool_execution")
		previousOutput := o.facts.Output
		if complete && v.stringField("status") == "completed" {
			switch typ {
			case "image_generation_call":
				if payload, ok := v.get("result"); ok && nonemptyString(payload) {
					o.content("image", at)
				}
			case "code_interpreter_call":
				if outputs, ok := v.get("outputs"); ok {
					outputs.array(func(part jsonView) bool {
						if part.stringField("type") == "logs" {
							if logs, ok := part.get("logs"); ok && nonemptyString(logs) {
								o.content("server_tool", at)
							}
						} else {
							o.partial("unknown_server_tool_output")
						}
						return true
					})
				}
			case "mcp_call":
				if result, ok := v.get("output"); ok && nonemptyString(result) {
					o.content("server_tool", at)
				}
			}
		}
		if complete && previousOutput == o.facts.Output && o.facts.Output != "nonempty" {
			o.partial("unverified_server_tool_payload")
		}
	default:
		o.partial("unknown_output_type")
		o.tighten(ReplayUnknown, "unknown_output_execution")
	}
}
func (o *ResponsesObserver) part(v jsonView, at time.Time) {
	if !o.validObject(v) {
		return
	}
	typ := v.stringField("type")
	allowed := keys("type text annotations logprobs")
	if typ == "refusal" {
		allowed = keys("type refusal")
	}
	v.object(o.opts.MaxStateBytes, func(k string, _ jsonView) bool {
		if !allowed[k] {
			o.partial("unknown_content_field")
		}
		return true
	})
	switch typ {
	case "output_text", "text":
		if text, ok := v.get("text"); ok && nonemptyString(text) {
			o.content("text", at)
		} else if !ok || text.kind() != '"' {
			o.partial("invalid_text_output")
		}
	case "summary_text", "reasoning_text":
		if text, ok := v.get("text"); ok && nonemptyString(text) {
			o.content("reasoning", at)
		} else if !ok || text.kind() != '"' {
			o.partial("invalid_reasoning_output")
		}
	case "refusal":
		if text, ok := v.get("refusal"); ok && nonemptyString(text) {
			o.refusal = true
			if o.facts.FirstContent.IsZero() {
				o.facts.FirstContent = at
				o.facts.ContentKind = "refusal"
			}
		} else if !ok || text.kind() != '"' {
			o.partial("invalid_refusal_output")
		}
	default:
		o.partial("unknown_content_type")
	}
}
func (o *ResponsesObserver) error(v jsonView) {
	o.facts.Error.Present = true
	o.facts.Result = "failed"
	if !o.validObject(v) {
		o.facts.ErrorCoverage = "partial"
		return
	}
	if nested, ok := v.get("error"); ok && !nested.null() {
		v = nested
		if !o.validObject(v) {
			o.facts.ErrorCoverage = "partial"
			return
		}
	}
	codeRaw, codeExists := v.get("code")
	typeRaw, typeExists := v.get("type")
	n := int64(0)
	for _, field := range []struct {
		v      jsonView
		exists bool
	}{{codeRaw, codeExists}, {typeRaw, typeExists}} {
		if field.exists && !field.v.null() {
			if field.v.kind() != '"' {
				o.facts.ErrorCoverage = "partial"
				return
			}
			n += int64(field.v.span.End - field.v.span.Start)
		}
	}
	if n > int64(o.opts.MaxStateBytes)-o.stateHeld || !o.reserve(n) {
		o.facts.ErrorCoverage = "unavailable"
		o.reason("error_state_limit")
		return
	}
	code, _ := codeRaw.text()
	typ, _ := typeRaw.text()
	o.release(o.errorHeld)
	o.stateHeld -= o.errorHeld
	o.errorHeld = n
	o.stateHeld += n
	o.facts.Error.Code = code
	o.facts.Error.Type = typ
	o.facts.Error.Complete = true
}
func (o *ResponsesObserver) usage(v jsonView) {
	// 用量缺失、歧义或格式错误只降低用量维度，不能污染已完整验证的输出。
	terminalCoverage, outputCoverage := o.facts.TerminalCoverage, o.facts.OutputCoverage
	defer func() { o.facts.TerminalCoverage = terminalCoverage; o.facts.OutputCoverage = outputCoverage }()
	if !o.validObject(v) {
		o.facts.Usage.Status = "partial"
		o.facts.Usage.Anomaly = "ambiguous_or_invalid_usage"
		o.facts.UsageCoverage = "partial"
		return
	}
	if o.usageHeld == 0 {
		const n int64 = 40
		if n > int64(o.opts.MaxStateBytes)-o.stateHeld || !o.reserve(n) {
			o.facts.Usage.Status = "partial"
			o.facts.UsageCoverage = "unavailable"
			o.reason("usage_state_limit")
			return
		}
		o.usageHeld = n
		o.stateHeld += n
	}
	u := Usage{Status: "reported"}
	for _, field := range []struct {
		key string
		dst **int64
	}{{"input_tokens", &u.Input}, {"output_tokens", &u.Output}, {"total_tokens", &u.Total}} {
		if raw, ok := v.get(field.key); ok {
			n, valid := raw.integer()
			if !valid || n < 0 {
				u.Anomaly = "invalid_usage_integer"
				u.Status = "partial"
			} else {
				*field.dst = &n
			}
		} else {
			u.Status = "partial"
		}
	}
	for _, field := range []struct {
		parent, key string
		dst         **int64
	}{{"input_tokens_details", "cached_tokens", &u.CachedInput}, {"output_tokens_details", "reasoning_tokens", &u.ReasoningOutput}} {
		if details, ok := v.get(field.parent); ok && !details.null() {
			if !o.validObject(details) {
				u.Anomaly = "invalid_usage_details"
				u.Status = "partial"
				continue
			}
			if raw, ok := details.get(field.key); ok {
				n, valid := raw.integer()
				if !valid || n < 0 {
					u.Anomaly = "invalid_usage_integer"
					u.Status = "partial"
				} else {
					*field.dst = &n
				}
			}
		}
	}
	for _, pair := range [][2]*int64{{o.facts.Usage.Input, u.Input}, {o.facts.Usage.Output, u.Output}, {o.facts.Usage.Total, u.Total}, {o.facts.Usage.CachedInput, u.CachedInput}, {o.facts.Usage.ReasoningOutput, u.ReasoningOutput}} {
		if pair[0] != nil && pair[1] != nil && *pair[1] < *pair[0] {
			u.Anomaly = "usage_counter_decreased"
			u.Status = "partial"
		}
	}
	o.facts.Usage = u
	if u.Status == "reported" {
		o.facts.UsageCoverage = "complete"
	}
	if u.Status == "partial" {
		o.facts.UsageCoverage = "partial"
		if o.facts.Coverage == "complete" {
			o.facts.Coverage = "partial"
		}
	}
}

// End 的 nil 表示完整 HTTP EOF；实际错误不能被早到的协议终态清除。
func (o *ResponsesObserver) End(err error) {
	if o.ended || o.closed {
		return
	}
	o.ended = true
	o.facts.EOF = err == nil || err == io.EOF
	if o.available && (o.opts.Mode != "stream" || o.opts.StatusCode >= 400) && o.rawBytes > 0 {
		v, ok := wholeJSON(o.buffer)
		if !ok || v.kind() != '{' {
			o.partial("invalid_response_json")
		} else if o.opts.StatusCode >= 400 {
			if _, ok := v.get("error"); ok {
				o.error(v)
			}
		} else {
			o.response(v, o.lastTime, true)
		}
	}
	if o.opts.Mode == "stream" && (len(o.buffer) > 0 || o.skipEvent || o.lineHasBytes) {
		o.partial("truncated_sse_event")
	}
	if !o.facts.EOF {
		o.partial("response_io_incomplete")
	}
	if o.facts.Result == "pending" {
		o.facts.Result = "unknown"
	}
	legal := o.facts.Result == "completed" || o.facts.Result == "tool_handoff" || o.facts.Result == "refusal" || o.facts.Result == "filtered" || o.facts.Result == "limited" || o.facts.Result == "paused" || o.facts.Result == "continuation_required"
	o.facts.ProtocolComplete = o.opts.StatusCode >= 200 && o.opts.StatusCode < 300 && o.facts.EOF && o.facts.Terminal == "observed" && o.facts.TerminalCoverage == "complete" && o.facts.OutputCoverage == "complete" && legal && !o.facts.Error.Present
	if o.facts.Output != "nonempty" {
		if o.facts.EOF && o.rawBytes == 0 && o.opts.StatusCode >= 200 && o.opts.StatusCode < 300 {
			o.facts.Output = "empty"
			o.facts.EmptyReason = "empty_transport_body"
			o.facts.OutputCoverage = "complete"
		} else if o.facts.ProtocolComplete {
			if o.facts.Result == "completed" {
				o.facts.Output = "empty"
				o.facts.EmptyReason = "empty_output"
			} else {
				o.facts.Output = "not_applicable"
			}
		}
	}
	if o.facts.Usage.Status == "missing" {
		o.facts.UsageCoverage = "partial"
		if o.facts.Coverage == "complete" {
			o.facts.Coverage = "partial"
		}
	}
	if o.facts.TerminalCoverage == "complete" && o.facts.OutputCoverage == "complete" && o.facts.UsageCoverage == "complete" && o.facts.ErrorCoverage == "complete" {
		o.facts.Coverage = "complete"
	}
	o.clearBuffer()
}

// Facts 返回小型独立快照，用量指针不与观察器共享。
func (o *ResponsesObserver) Facts() ResponseFacts {
	f := o.facts
	for _, ptr := range []**int64{&f.Usage.Input, &f.Usage.Output, &f.Usage.Total, &f.Usage.CachedInput, &f.Usage.ReasoningOutput} {
		if *ptr != nil {
			v := **ptr
			*ptr = &v
		}
	}
	return f
}

// StopObserving 停止语义观察并归还正文及索引预算，不伪造真实 I/O 结束。
// 已确认的输出、错误与用量事实继续保留，后续 Feed 仍累计实际字节。
func (o *ResponsesObserver) StopObserving(reason string) {
	if o.closed || !o.available {
		return
	}
	o.available = false
	o.unavailable(reason)
	o.clearBuffer()
	// 小型已确认事实仍在使用，保留其预算直到 Close；条目索引可以立即释放。
	retained := o.errorHeld + o.usageHeld
	o.release(o.stateHeld - retained)
	o.stateHeld = retained
	o.items = nil
	o.skipEvent = false
	o.lineHasBytes = false
	o.previousCR = false
	o.lineHeadBytes = 0
	o.lineHeadOverflow = false
	o.frameData = false
	o.frameHeartbeat = false
}

// Close 幂等归还所有仍占用的观察预算。
func (o *ResponsesObserver) Close() {
	if o.closed {
		return
	}
	o.closed = true
	o.clearBuffer()
	o.release(o.stateHeld)
	o.stateHeld = 0
	o.items = nil
}
