package config

import (
	"maps"
	"relayleaf/internal/catalog"
	"time"
)

func defaultExecution() catalog.Execution {
	e := catalog.Execution{
		MaxRetries: 1, MaxRetriesPerProvider: 1, RetryInterval: 200 * time.Millisecond, RetryMaxInterval: 2 * time.Second, RetryWaitTimeout: 30 * time.Second,
		ConnectTimeout: 10 * time.Second, TLSHandshakeTimeout: 10 * time.Second, ResponseHeaderTimeout: 120 * time.Second, StreamFirstEventTimeout: 120 * time.Second, StreamFirstContentTimeout: 180 * time.Second, NonStreamResponseTimeout: 300 * time.Second, StreamIdleTimeout: 90 * time.Second, DownstreamWriteIdleTimeout: 30 * time.Second, RequestBodyReadTimeout: 60 * time.Second,
		EmptyResponseEnabled: true, EmptyResponseMaxBufferBytes: 1 << 20, EmptyResponseMaxWait: 120 * time.Second, MaxErrorBodyBytes: 65536, MaxErrorBodyWait: 2 * time.Second, Sources: map[string]string{},
	}
	for _, name := range []string{"max_retries", "max_retries_per_provider", "retry_interval", "retry_max_interval", "retry_wait_timeout", "connect_timeout", "tls_handshake_timeout", "response_header_timeout", "stream_first_event_timeout", "stream_first_content_timeout.enabled", "stream_first_content_timeout.timeout", "non_stream_response_timeout", "stream_idle_timeout", "downstream_write_idle_timeout", "request_body_read_timeout", "empty_response.enabled", "empty_response.max_buffer_bytes", "empty_response.max_wait", "conditional_retry.enabled", "conditional_retry.rules", "conditional_retry.max_error_body_bytes", "conditional_retry.max_error_body_wait"} {
		e.Sources[name] = "builtin"
	}
	return e
}
func (c *compiler) duration(s, path string, zero bool) (time.Duration, error) {
	d, err := time.ParseDuration(s)
	if err != nil || d < 0 || (!zero && d == 0) {
		return 0, c.err(path, "时长必须为正数且在可表示范围内")
	}
	return d, nil
}
func (c *compiler) execution(base catalog.Execution, d *executionDTO, path string, level int) (catalog.Execution, error) {
	base.Sources = maps.Clone(base.Sources)
	if d == nil {
		return base, nil
	}
	for _, entry := range []struct {
		p    *int
		dst  *int
		name string
	}{{d.MaxRetries, &base.MaxRetries, "max_retries"}, {d.MaxRetriesPerProvider, &base.MaxRetriesPerProvider, "max_retries_per_provider"}} {
		if entry.p != nil {
			if *entry.p < 0 || *entry.p > 2147483647 {
				return base, c.err(path+"."+entry.name, "需要非负的 32 位整数")
			}
			*entry.dst = *entry.p
			base.Sources[entry.name] = path
		}
	}
	for _, entry := range []struct {
		p    *string
		dst  *time.Duration
		name string
		zero bool
	}{
		{d.RetryInterval, &base.RetryInterval, "retry_interval", true}, {d.RetryMaxInterval, &base.RetryMaxInterval, "retry_max_interval", false}, {d.RetryWaitTimeout, &base.RetryWaitTimeout, "retry_wait_timeout", false}, {d.ConnectTimeout, &base.ConnectTimeout, "connect_timeout", false}, {d.TLSHandshakeTimeout, &base.TLSHandshakeTimeout, "tls_handshake_timeout", false}, {d.ResponseHeaderTimeout, &base.ResponseHeaderTimeout, "response_header_timeout", false}, {d.StreamFirstEventTimeout, &base.StreamFirstEventTimeout, "stream_first_event_timeout", false}, {d.NonStreamResponseTimeout, &base.NonStreamResponseTimeout, "non_stream_response_timeout", false}, {d.StreamIdleTimeout, &base.StreamIdleTimeout, "stream_idle_timeout", false}, {d.DownstreamWriteIdleTimeout, &base.DownstreamWriteIdleTimeout, "downstream_write_idle_timeout", false}, {d.RequestBodyReadTimeout, &base.RequestBodyReadTimeout, "request_body_read_timeout", false},
	} {
		if entry.p != nil {
			v, err := c.duration(*entry.p, path+"."+entry.name, entry.zero)
			if err != nil {
				return base, err
			}
			*entry.dst = v
			base.Sources[entry.name] = path
		}
	}
	if d.StreamFirstContent != nil {
		x := d.StreamFirstContent
		if x.Enabled != nil {
			base.StreamFirstContentEnabled = *x.Enabled
			base.Sources["stream_first_content_timeout.enabled"] = path
		}
		if x.Timeout != nil {
			v, err := c.duration(*x.Timeout, path+".stream_first_content_timeout.timeout", false)
			if err != nil {
				return base, err
			}
			base.StreamFirstContentTimeout = v
			base.Sources["stream_first_content_timeout.timeout"] = path
		}
	}
	if d.EmptyResponse != nil {
		x := d.EmptyResponse
		if x.Enabled != nil {
			base.EmptyResponseEnabled = *x.Enabled
			base.Sources["empty_response.enabled"] = path
		}
		if x.MaxBufferBytes != nil {
			if *x.MaxBufferBytes <= 0 {
				return base, c.err(path+".empty_response.max_buffer_bytes", "字节容量必须为正数")
			}
			base.EmptyResponseMaxBufferBytes = *x.MaxBufferBytes
			base.Sources["empty_response.max_buffer_bytes"] = path
		}
		if x.MaxWait != nil {
			v, err := c.duration(*x.MaxWait, path+".empty_response.max_wait", false)
			if err != nil {
				return base, err
			}
			base.EmptyResponseMaxWait = v
			base.Sources["empty_response.max_wait"] = path
		}
	}
	if d.ConditionalRetry != nil {
		x := d.ConditionalRetry
		if x.Enabled != nil {
			base.ConditionalRetryEnabled = *x.Enabled
			base.Sources["conditional_retry.enabled"] = path
		}
		if x.MaxErrorBodyBytes != nil {
			if *x.MaxErrorBodyBytes <= 0 {
				return base, c.err(path+".conditional_retry.max_error_body_bytes", "字节容量必须为正数")
			}
			base.MaxErrorBodyBytes = *x.MaxErrorBodyBytes
			base.Sources["conditional_retry.max_error_body_bytes"] = path
		}
		if x.MaxErrorBodyWait != nil {
			v, err := c.duration(*x.MaxErrorBodyWait, path+".conditional_retry.max_error_body_wait", false)
			if err != nil {
				return base, err
			}
			base.MaxErrorBodyWait = v
			base.Sources["conditional_retry.max_error_body_wait"] = path
		}
		ids := map[string]bool{}
		for i, r := range x.Rules {
			p := indexPath(path+".conditional_retry.rules", i)
			if !safePathPart(r.ID) || ids[r.ID] {
				return base, c.err(p+".id", "规则标识符无效或重复")
			}
			ids[r.ID] = true
			if r.Action != "retry" && r.Action != "no_retry" {
				return base, c.err(p+".action", "重试动作无效")
			}
			if err := c.match(r.Match, p+".match", false); err != nil {
				return base, err
			}
		}
		if len(x.Rules) > 64 {
			return base, c.err(path+".conditional_retry.rules", "最多允许 64 条规则")
		}
		if x.Rules != nil {
			base.RetryRules = make([]catalog.RetryRule, 0, len(x.Rules))
			for _, r := range x.Rules {
				base.RetryRules = append(base.RetryRules, catalog.RetryRule{ID: r.ID, Action: r.Action, Match: domainMatch(r.Match)})
			}
			base.Sources["conditional_retry.rules"] = path
		}
	}
	if base.RetryMaxInterval < base.RetryInterval {
		return base, c.err(path+".retry_max_interval", "继承后的值必须不小于 retry_interval")
	}
	if base.ConditionalRetryEnabled {
		return base, c.err(path+".conditional_retry.enabled", "不支持：尚未实现条件重放")
	}
	return base, nil
}
func (c *compiler) match(m matchDTO, path string, health bool) error {
	if len(m.StatusCodes)+len(m.ErrorCodes)+len(m.ErrorTypes)+len(m.MessageContains)+len(m.MessageEquals)+len(m.FailureKinds) == 0 {
		return c.err(path, "规则要求非空的匹配条件")
	}
	for _, v := range m.StatusCodes {
		if v < 400 || v > 599 {
			return c.err(path+".status_codes", "HTTP 状态码无效")
		}
	}
	seenStatus := map[int]bool{}
	for _, v := range m.StatusCodes {
		if seenStatus[v] {
			return c.err(path+".status_codes", "状态码重复")
		}
		seenStatus[v] = true
	}
	for _, xs := range [][]string{m.Protocols, m.Operations, m.ErrorCodes, m.ErrorTypes, m.MessageContains, m.MessageEquals, m.FailureKinds} {
		if err := c.unique(xs, path, false); err != nil {
			return err
		}
		for _, v := range xs {
			if !text(v, 512) {
				return c.err(path, "匹配值不能为空且不得包含控制字符")
			}
		}
	}
	if m.RetryAfter != "" && m.RetryAfter != "valid" {
		return c.err(path+".retry_after", "Retry-After 条件无效")
	}
	if !health && len(m.FailureKinds) > 0 {
		return c.err(path+".failure_kinds", "字段仅适用于健康规则")
	}
	if len(m.Operations) > 0 {
		return c.err(path+".operations", "未定义操作过滤条件")
	}
	for _, p := range m.Protocols {
		if p != "openai.responses" && p != "openai.chat" && p != "anthropic.messages" && p != "gemini.generate_content" {
			return c.err(path+".protocols", "未知协议过滤条件")
		}
	}
	for _, k := range m.FailureKinds {
		if k != "communication_error" && k != "upstream_timeout" && k != "service_error" && k != "stream_error" && k != "empty_response" {
			return c.err(path+".failure_kinds", "未知故障类型")
		}
	}
	return nil
}
func (c *compiler) health(d *healthDTO) (catalog.Health, error) {
	h := catalog.Health{CircuitBreakerEnabled: true, QuotaCooldown: 30 * time.Second, ConsecutiveFailureThreshold: 3, FailureRateMinSamples: 20, MaxSamples: 1000, UnknownConcurrency: 1, HalfOpenConcurrency: 1, HalfOpenSuccessThreshold: 2, FailureRateThreshold: 0.5, FailureWindow: 30 * time.Second, Cooldown: 30 * time.Second, MaxCooldown: 5 * time.Minute, HealthyTTL: 5 * time.Minute}
	h.NoAvailableTargetAction = "fail_fast"
	h.ProbeInterval = 2 * time.Second
	if d == nil {
		return h, nil
	}
	path := "$.health_defaults"
	if d.CircuitBreaker != nil {
		if d.CircuitBreaker.ExemptUntil != nil {
			return h, c.err(path+".circuit_breaker.exempt_until", "字段仅适用于提供方层级")
		}
		if err := c.breaker(d.CircuitBreaker, path+".circuit_breaker"); err != nil {
			return h, err
		}
		h.CircuitBreakerEnabled = enabled(d.CircuitBreaker.Enabled)
		h.Rules = c.domainHealthRules(d.CircuitBreaker.Rules)
		if x := d.CircuitBreaker.NoAvailableTarget; x != nil {
			if x.Action != "" {
				h.NoAvailableTargetAction = x.Action
			}
			if x.ProbeInterval != "" {
				h.ProbeInterval, _ = time.ParseDuration(x.ProbeInterval)
			}
		}
	}
	if d.ActiveProbe {
		return h, c.err(path+".active_probe", "不支持：尚未实现主动探测")
	}
	ints := []struct {
		p    *int
		name string
	}{{d.ConsecutiveFailureThreshold, "consecutive_failure_threshold"}, {d.FailureRateMinSamples, "failure_rate_min_samples"}, {d.MaxSamples, "max_samples"}, {d.UnknownConcurrency, "unknown_concurrency"}, {d.HalfOpenConcurrency, "half_open_concurrency"}, {d.HalfOpenSuccessThreshold, "half_open_success_threshold"}}
	for _, v := range ints {
		if v.p != nil && (*v.p <= 0 || *v.p > 2147483647) {
			return h, c.err(path+"."+v.name, "需要正的 32 位整数")
		}
	}
	maxSamples, minSamples, consecutive := 1000, 20, 3
	if d.MaxSamples != nil {
		maxSamples = *d.MaxSamples
	}
	if d.FailureRateMinSamples != nil {
		minSamples = *d.FailureRateMinSamples
	}
	if d.ConsecutiveFailureThreshold != nil {
		consecutive = *d.ConsecutiveFailureThreshold
	}
	if minSamples > maxSamples || consecutive > maxSamples {
		return h, c.err(path+".max_samples", "样本容量必须覆盖两个阈值")
	}
	h.MaxSamples = maxSamples
	h.FailureRateMinSamples = minSamples
	h.ConsecutiveFailureThreshold = consecutive
	if d.UnknownConcurrency != nil {
		h.UnknownConcurrency = *d.UnknownConcurrency
	}
	if d.HalfOpenConcurrency != nil {
		h.HalfOpenConcurrency = *d.HalfOpenConcurrency
	}
	if d.HalfOpenSuccessThreshold != nil {
		h.HalfOpenSuccessThreshold = *d.HalfOpenSuccessThreshold
	}
	if d.FailureRateThreshold != nil && !(*d.FailureRateThreshold > 0 && *d.FailureRateThreshold <= 1) {
		return h, c.err(path+".failure_rate_threshold", "比例必须在 (0, 1] 范围内")
	}
	if d.FailureRateThreshold != nil {
		h.FailureRateThreshold = *d.FailureRateThreshold
	}
	cooldown, maxCooldown := 30*time.Second, 5*time.Minute
	for _, v := range []struct {
		p    *string
		name string
	}{{d.FailureWindow, "failure_window"}, {d.Cooldown, "cooldown"}, {d.MaxCooldown, "max_cooldown"}, {d.QuotaCooldown, "quota_cooldown"}, {d.HealthyTTL, "healthy_ttl"}} {
		if v.p != nil {
			dur, err := c.duration(*v.p, path+"."+v.name, false)
			if err != nil {
				return h, err
			}
			switch v.name {
			case "cooldown":
				cooldown = dur
				h.Cooldown = dur
			case "max_cooldown":
				maxCooldown = dur
				h.MaxCooldown = dur
			case "quota_cooldown":
				h.QuotaCooldown = dur
			case "failure_window":
				h.FailureWindow = dur
			case "healthy_ttl":
				h.HealthyTTL = dur
			}
		}
	}
	if cooldown > maxCooldown {
		return h, c.err(path+".max_cooldown", "必须不小于 cooldown")
	}
	if err := c.ruleCapacity(h.Rules, h.MaxSamples, path+".circuit_breaker.rules"); err != nil {
		return h, err
	}
	return h, nil
}
func (c *compiler) breaker(d *breakerDTO, path string) error {
	if d.ExemptUntil != nil {
		if _, err := c.timestamp(*d.ExemptUntil, path+".exempt_until"); err != nil {
			return err
		}
	}
	if d.NoAvailableTarget != nil {
		x := d.NoAvailableTarget
		action := x.Action
		if _, present := c.nodes[path+".no_available_target.action"]; !present {
			action = "fail_fast"
		}
		if action != "fail_fast" && action != "probe" {
			return c.err(path+".no_available_target.action", "无可用候选时的动作无效")
		}
		interval := x.ProbeInterval
		if _, present := c.nodes[path+".no_available_target.probe_interval"]; !present {
			interval = "2s"
		}
		if _, err := c.duration(interval, path+".no_available_target.probe_interval", false); err != nil {
			return err
		}
		if action == "probe" && enabled(d.Enabled) {
			return c.err(path+".no_available_target.action", "不支持：尚未实现提前探测")
		}
	}
	ids := map[string]bool{}
	for i, r := range d.Rules {
		p := indexPath(path+".rules", i)
		if !safePathPart(r.ID) || ids[r.ID] {
			return c.err(p+".id", "规则标识符无效或重复")
		}
		ids[r.ID] = true
		if err := c.match(r.Match, p+".match", true); err != nil {
			return err
		}
		switch r.Action.Type {
		case "observe_only":
			if r.Trigger != nil {
				return c.err(p+".trigger", "observe_only 不允许设置 trigger")
			}
			if r.Action.Duration != "" || r.Action.Until != "" {
				return c.err(p+".action", "observe_only 不允许设置 duration 或 until")
			}
		case "open_for", "open_until":
			if r.Trigger == nil {
				return c.err(p+".trigger", "熔断动作要求设置 trigger")
			}
			if r.Action.Type == "open_for" {
				if r.Action.Until != "" {
					return c.err(p+".action.until", "until 仅适用于 open_until")
				}
				if _, err := c.duration(r.Action.Duration, p+".action.duration", false); err != nil {
					return err
				}
			} else {
				if r.Action.Duration != "" {
					return c.err(p+".action.duration", "duration 仅适用于 open_for")
				}
				if _, err := c.timestamp(r.Action.Until, p+".action.until"); err != nil {
					return err
				}
			}
		default:
			return c.err(p+".action.type", "健康动作无效")
		}
		if r.Trigger != nil {
			x := r.Trigger
			if x.Type != "immediate" && x.Type != "consecutive_failures" && x.Type != "failure_rate" {
				return c.err(p+".trigger.type", "触发器类型无效")
			}
			if x.Type != "immediate" {
				if _, err := c.duration(x.Window, p+".trigger.window", false); err != nil {
					return err
				}
			}
			if x.Type == "immediate" && (x.Window != "" || x.Count != 0 || x.MinSamples != 0 || x.Rate != 0) {
				return c.err(p+".trigger", "immediate 不允许设置统计参数")
			}
			if x.Type == "consecutive_failures" && x.Count <= 0 {
				return c.err(p+".trigger.count", "计数必须为正数")
			}
			if x.Type == "consecutive_failures" && (x.MinSamples != 0 || x.Rate != 0) {
				return c.err(p+".trigger", "失败率参数要求使用 failure_rate 触发器")
			}
			if x.Type == "failure_rate" && (x.MinSamples <= 0 || !(x.Rate > 0 && x.Rate <= 1)) {
				return c.err(p+".trigger", "失败率阈值无效")
			}
			if x.Type == "failure_rate" && x.Count != 0 {
				return c.err(p+".trigger.count", "count 要求使用 consecutive_failures 触发器")
			}
		}
	}
	if len(d.Rules) > 64 {
		return c.err(path+".rules", "最多允许 64 条规则")
	}
	return nil
}
func (c *compiler) ruleCapacity(rules []catalog.HealthRule, capacity int, path string) error {
	for i, r := range rules {
		if r.Count > capacity || r.MinSamples > capacity {
			return c.err(indexPath(path, i)+".trigger", "规则样本阈值超过 max_samples")
		}
	}
	return nil
}
func domainMatch(m matchDTO) catalog.Match {
	return catalog.Match{Protocols: m.Protocols, ErrorCodes: m.ErrorCodes, ErrorTypes: m.ErrorTypes, MessageContains: m.MessageContains, MessageEquals: m.MessageEquals, FailureKinds: m.FailureKinds, StatusCodes: m.StatusCodes, RetryAfter: m.RetryAfter}
}
func (c *compiler) domainHealthRules(rules []healthRuleDTO) []catalog.HealthRule {
	result := make([]catalog.HealthRule, 0, len(rules))
	for _, r := range rules {
		h := catalog.HealthRule{ID: r.ID, Action: r.Action.Type, Match: domainMatch(r.Match)}
		if r.Action.Duration != "" {
			h.Duration, _ = time.ParseDuration(r.Action.Duration)
		}
		if r.Action.Until != "" {
			h.Until, _ = time.Parse(time.RFC3339, r.Action.Until)
		}
		if r.Trigger != nil {
			h.Trigger = r.Trigger.Type
			h.Count = r.Trigger.Count
			h.MinSamples = r.Trigger.MinSamples
			h.Ratio = r.Trigger.Rate
			h.Window, _ = time.ParseDuration(r.Trigger.Window)
		}
		result = append(result, h)
	}
	return result
}
