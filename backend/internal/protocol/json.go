package protocol

import (
	"bytes"
	"encoding/json"
	"strconv"
	"unicode/utf8"
)

// jsonView 只借用原文区间，不建立对话或响应对象树。
type jsonView struct {
	raw  []byte
	span Span
}

func wholeJSON(raw []byte) (jsonView, bool) {
	if !utf8.Valid(raw) || !json.Valid(raw) {
		return jsonView{}, false
	}
	i := space(raw, 0)
	return jsonView{raw, Span{i, valueEnd(raw, i)}}, true
}
func space(raw []byte, i int) int {
	for i < len(raw) && (raw[i] == ' ' || raw[i] == '\r' || raw[i] == '\n' || raw[i] == '\t') {
		i++
	}
	return i
}
func stringEnd(raw []byte, i int) int {
	i++
	for i < len(raw) {
		if raw[i] == '\\' {
			i += 2
			continue
		}
		if raw[i] == '"' {
			return i + 1
		}
		i++
	}
	return len(raw)
}

// valueEnd 的输入已经通过 JSON 语法校验，扫描不使用递归栈。
func valueEnd(raw []byte, i int) int {
	if raw[i] == '"' {
		return stringEnd(raw, i)
	}
	if raw[i] != '{' && raw[i] != '[' {
		for i < len(raw) && raw[i] != ',' && raw[i] != '}' && raw[i] != ']' && raw[i] != ' ' && raw[i] != '\r' && raw[i] != '\n' && raw[i] != '\t' {
			i++
		}
		return i
	}
	depth := 0
	for i < len(raw) {
		switch raw[i] {
		case '"':
			i = stringEnd(raw, i)
			continue
		case '{', '[':
			depth++
		case '}', ']':
			depth--
			if depth == 0 {
				return i + 1
			}
		}
		i++
	}
	return i
}
func (v jsonView) kind() byte {
	if v.span.Start >= v.span.End {
		return 0
	}
	return v.raw[v.span.Start]
}
func (v jsonView) bytes() []byte { return v.raw[v.span.Start:v.span.End] }
func (v jsonView) text() (string, bool) {
	if v.kind() != '"' {
		return "", false
	}
	var s string
	err := json.Unmarshal(v.bytes(), &s)
	return s, err == nil
}
func (v jsonView) null() bool { return string(v.bytes()) == "null" }
func (v jsonView) integer() (int64, bool) {
	// int64 最长为带负号的 20 字节，先拒绝更长值以免为错误诊断复制巨大输入。
	if v.span.End-v.span.Start > 20 {
		return 0, false
	}
	n, e := strconv.ParseInt(string(v.bytes()), 10, 64)
	return n, e == nil
}
func (v jsonView) boolean() (bool, bool) {
	switch string(v.bytes()) {
	case "true":
		return true, true
	case "false":
		return false, true
	}
	return false, false
}
func (v jsonView) array(visit func(jsonView) bool) bool {
	if v.kind() != '[' {
		return false
	}
	for i := space(v.raw, v.span.Start+1); i < v.span.End-1; {
		end := valueEnd(v.raw, i)
		if !visit(jsonView{v.raw, Span{i, end}}) {
			return false
		}
		i = space(v.raw, end)
		if v.raw[i] == ',' {
			i = space(v.raw, i+1)
		}
	}
	return true
}

// object 顺序访问字段；键长度在解码前受限，调用者自行检查必要字段重复。
func (v jsonView) object(maxKey int, visit func(string, jsonView) bool) bool {
	return v.objectWithKeyLimit(func() int { return maxKey }, visit)
}

// objectWithKeyLimit 在每个键解码前读取当前额度，避免沿用已经消耗的初始额度。
func (v jsonView) objectWithKeyLimit(maxKey func() int, visit func(string, jsonView) bool) bool {
	if v.kind() != '{' {
		return false
	}
	for i := space(v.raw, v.span.Start+1); i < v.span.End-1; {
		end := stringEnd(v.raw, i)
		if end-i > maxKey() {
			return false
		}
		if bytes.IndexByte(v.raw[i:end], '\\') >= 0 && end-i > (maxKey()-4)/2 {
			return false
		}
		key, _ := jsonView{v.raw, Span{i, end}}.text()
		i = space(v.raw, end)
		i = space(v.raw, i+1)
		end = valueEnd(v.raw, i)
		if !visit(key, jsonView{v.raw, Span{i, end}}) {
			return false
		}
		i = space(v.raw, end)
		if v.raw[i] == ',' {
			i = space(v.raw, i+1)
		}
	}
	return true
}
func (v jsonView) get(key string) (jsonView, bool) {
	var found jsonView
	ok := false
	v.object(4096, func(k string, val jsonView) bool {
		if k == key {
			found = val
			ok = true
			return false
		}
		return true
	})
	return found, ok
}
func (v jsonView) stringField(key string) string {
	f, ok := v.get(key)
	if !ok || f.span.End-f.span.Start > 4096 {
		return ""
	}
	s, _ := f.text()
	return s
}
