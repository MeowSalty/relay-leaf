package relay

import (
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"relayleaf/internal/protocol"
	"relayleaf/internal/routing"
	runstate "relayleaf/internal/runtime"
)

// retryAfter 只识别标准的单值头；期限始终从接收时刻计算，不在结算时重新起算。
func retryAfter(header http.Header, received time.Time) (time.Time, bool) {
	values := header.Values("Retry-After")
	if len(values) != 1 {
		return time.Time{}, false
	}
	value := strings.TrimSpace(values[0])
	if value == "" {
		return time.Time{}, false
	}
	digits := true
	for _, c := range value {
		if c < '0' || c > '9' {
			digits = false
			break
		}
	}
	if digits {
		seconds, err := strconv.ParseUint(value, 10, 64)
		if err != nil || seconds > uint64(math.MaxInt64/int64(time.Second)) {
			return received.Add(time.Duration(math.MaxInt64)), true
		}
		return received.Add(time.Duration(seconds) * time.Second), true
	}
	deadline, err := http.ParseTime(value)
	if err != nil {
		return time.Time{}, false
	}
	if !deadline.After(received) {
		return received, true
	}
	return received.Add(deadline.Sub(received)), true
}

func (h *Handler) applyHeaderRestriction(lease *runstate.AttemptLease, candidate routing.Candidate, response *http.Response, received time.Time) {
	until, valid := retryAfter(response.Header, received)
	if response.StatusCode == http.StatusTooManyRequests {
		if !valid {
			until = received.Add(candidate.Provider.Health.QuotaCooldown)
		}
		h.state.ApplyRestriction(lease, runstate.Restriction{Kind: "quota_cooldown", Scope: "credential", Reason: "upstream_rate_limit_scope_unknown", ReceivedAt: received, Until: until})
	} else if response.StatusCode >= 400 && valid {
		h.state.ApplyRestriction(lease, runstate.Restriction{Kind: "service_cooldown", Scope: "target", Reason: "upstream_retry_after", ReceivedAt: received, Until: until})
	}
}

func (h *Handler) applyObservedRestriction(lease *runstate.AttemptLease, candidate routing.Candidate, response *http.Response, facts protocol.ResponseFacts, received, headersAt time.Time) {
	if !facts.Error.Present || !facts.Error.Complete {
		return
	}
	code, typ := facts.Error.Code, facts.Error.Type
	exhausted := typ == "insufficient_quota"
	switch code {
	case "insufficient_quota", "credit_balance_exhausted", "organization_spend_limit_exceeded", "project_spend_limit_exceeded", "organization_usage_limit_exceeded":
		exhausted = true
	}
	if exhausted {
		scopeID := quotaEvidence(candidate, code)
		scope := "credential"
		if scopeID != "" {
			scope = "quota"
		}
		// Retry-After 只证明等待要求，不能证明余额或消费额度届时恢复。
		// 当前没有已验证的额度重置字段，因此明确耗尽必须等待外部复核。
		h.state.ApplyRestriction(lease, runstate.Restriction{Kind: "quota_exhausted", Scope: scope, QuotaScopeID: scopeID, Reason: "upstream_quota_exhausted", ReceivedAt: received, Permanent: true})
		return
	}
	if code == "slow_down" && typ == "rate_limit_error" {
		until, valid := retryAfter(response.Header, headersAt)
		if !valid {
			until = received.Add(candidate.Provider.Health.QuotaCooldown)
		}
		h.state.ApplyRestriction(lease, runstate.Restriction{Kind: "quota_cooldown", Scope: "credential", Reason: "upstream_rate_limit_scope_unknown", ReceivedAt: received, Until: until})
	}
}

// 只有文档化错误边界和精确的配置归属同时匹配，才从凭据扩大到共享配额域。
func quotaEvidence(c routing.Candidate, code string) string {
	dimension := ""
	switch code {
	case "organization_spend_limit_exceeded", "organization_usage_limit_exceeded", "credit_balance_exhausted":
		dimension = "organization"
	case "project_spend_limit_exceeded":
		dimension = "project"
	}
	value := c.Credential.Attributes[dimension]
	if dimension == "" || value == "" {
		return ""
	}
	match := ""
	for _, fullID := range c.QuotaScopes {
		id := strings.TrimPrefix(fullID, c.Provider.ID+"/")
		scope, ok := c.Provider.QuotaScopes[id]
		if !ok || len(scope.Owner) != 1 || scope.Owner[dimension] != value {
			continue
		}
		if match != "" {
			return ""
		}
		match = fullID
	}
	return match
}
