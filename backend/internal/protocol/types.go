// Package protocol 提取必要请求事实并有界观察 Responses 响应，不拥有网络或响应提交。
package protocol

import "time"

// GatewayError 描述网关在响应提交前可以返回的稳定错误。
type GatewayError struct {
	Status  int
	Code    string
	Message string
}

func (e *GatewayError) Error() string { return e.Message }

type ReplayConstraint string

const (
	ReplayAllowed   ReplayConstraint = "after_send_allowed"
	ReplayForbidden ReplayConstraint = "after_send_forbidden"
	ReplayUnknown   ReplayConstraint = "unknown"
)

// Span 使用原始 UTF-8 JSON 字节的左闭右开区间。
type Span struct{ Start, End int }

// RequestFacts 仅保存执行需要的有界元数据，不保存完整对话树。
type RequestFacts struct {
	Model         string
	ModelSpan     Span
	Mode          string
	Capabilities  []string
	ResourceTypes []string
	Replay        ReplayConstraint
	ReplayReason  string
}

// BufferBudget 对实际保留的观察字节进行实例级计量。
type BufferBudget interface {
	Reserve(int64) bool
	Release(int64)
}

type ObserverOptions struct {
	Mode, ContentType, ContentEncoding string
	StatusCode                         int
	Replay                             ReplayConstraint
	MaxJSONBytes, MaxEventBytes        int
	MaxStateBytes, MaxErrorBytes       int
	Budget                             BufferBudget
}

// Usage 用指针区分未报告字段与真实的零值。
type Usage struct {
	Status                       string
	Input, Output, Total         *int64
	CachedInput, ReasoningOutput *int64
	Anomaly                      string
}

// ErrorFacts 只保存结构化机器字段；不把上游消息或正文带入常规记录。
type ErrorFacts struct {
	Present, Complete bool
	Code, Type        string
}

// ResponseFacts 将协议终局、输出、覆盖度和实际传输收尾分别表示。
type ResponseFacts struct {
	Terminal, Result, Output, Coverage string
	TerminalCoverage, OutputCoverage   string
	UsageCoverage, ErrorCoverage       string
	Reason, EmptyReason, ContentKind   string
	FirstEvent, FirstContent           time.Time
	Replay                             ReplayConstraint
	ReplayReason                       string
	Usage                              Usage
	Error                              ErrorFacts
	ProtocolComplete, EOF              bool
}
