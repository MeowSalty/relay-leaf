// Package runtime 管理有界运行占用和独立限制，不发送请求或接触秘密。
package runtime

import "time"

// Selection 使用稳定配置身份描述一次拟执行的调用。
type Selection struct {
	TargetID, ProviderID, CredentialID, CredentialVersion string
	QuotaScopes                                           []string
}

// Rejection 表示没有发生上游调用的准入拒绝。
type Rejection struct {
	Status int
	Code   string
}

func (e *Rejection) Error() string { return "运行许可不可用：" + e.Code }

type BufferKind uint8

const (
	RequestBuffer BufferKind = iota
	ResponseBuffer
)

// Restriction 保存已经确认的独立限制。Scope 可为 credential、target、quota 或 permission。
// 无期限限制必须显式设置 Permanent；Until 从收到证据的时间计算。
type Restriction struct {
	Kind, Scope, QuotaScopeID, Reason string
	ReceivedAt, Until                 time.Time
	Permanent                         bool
}

type RestrictionState struct {
	Key, Kind, Reason  string
	Until              time.Time
	Permanent, Probing bool
	Generation         uint64
}

type Stats struct {
	Inbound, ActiveAttempts, StateEntries int
	Attempts                              uint64
	BodyBytes, ResponseBytes              int64
	Clients, Targets, Quotas              map[string]int
	Restrictions                          []RestrictionState
}
