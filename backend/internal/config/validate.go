package config

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"relayleaf/internal/catalog"
	"slices"
	"strings"
)

func operation(protocol, op string) error {
	allowed := map[string][]string{"openai.responses": {"create", "compact"}, "openai.chat": {"create"}, "anthropic.messages": {"create", "count_tokens"}, "gemini.generate_content": {"generate_content", "stream_generate_content"}}
	if !slices.Contains(allowed[protocol], op) {
		return fmt.Errorf("未知操作")
	}
	return nil
}
func (c *compiler) accessGroupRefs(ids []string, groups map[string]catalog.AccessGroup, path string) error {
	if err := c.unique(ids, path, true); err != nil {
		return err
	}
	for _, id := range ids {
		if _, ok := groups[id]; !ok {
			return c.err(path, "访问组引用了未知组")
		}
	}
	return nil
}
func (c *compiler) modes(modes []string, path string) error {
	if len(modes) == 0 {
		return c.err(path, "请求模式不能为空")
	}
	if err := c.unique(modes, path, false); err != nil {
		return err
	}
	for _, m := range modes {
		if m != "non_stream" && m != "stream" {
			return c.err(path, "未知请求模式")
		}
	}
	return nil
}

var headerName = regexp.MustCompile("^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")

func reservedIdentity(s string) bool {
	switch strings.ToLower(s) {
	case "openai-organization", "openai-project", "x-goog-user-project":
		return true
	}
	return false
}
func reservedHeader(s string) bool {
	switch strings.ToLower(s) {
	case "authorization", "x-api-key", "api-key", "x-goog-api-key", "cookie", "host", "content-length", "content-type", "content-encoding", "transfer-encoding", "connection", "upgrade", "trailer", "te", "keep-alive", "proxy-connection", "proxy-authorization", "proxy-authenticate":
		return true
	}
	return reservedIdentity(s)
}
func (c *compiler) endpoint(e endpointDTO, path string) error {
	if err := operation(e.Protocol, e.Operation); err != nil {
		return c.err(path+".operation", "未知协议操作")
	}
	if err := c.modes(e.RequestModes, path+".request_modes"); err != nil {
		return err
	}
	if err := c.upstreamPath(e.PathTemplate, e.Protocol, e.Operation, path+".path_template"); err != nil {
		return err
	}
	if e.Operation == "compact" || e.Operation == "count_tokens" || e.Operation == "generate_content" {
		if len(e.RequestModes) != 1 || e.RequestModes[0] != "non_stream" {
			return c.err(path+".request_modes", "操作仅支持 non_stream")
		}
	}
	if e.Operation == "stream_generate_content" && (len(e.RequestModes) != 1 || e.RequestModes[0] != "stream") {
		return c.err(path+".request_modes", "操作仅支持 stream")
	}
	switch e.Auth.Type {
	case "bearer", "none":
		if e.Auth.Name != "" {
			return c.err(path+".auth.name", "name 仅适用于 header 或 query 认证")
		}
	case "header":
		isAuthCarrier := slices.Contains([]string{"authorization", "x-api-key", "api-key", "x-goog-api-key"}, strings.ToLower(e.Auth.Name))
		if !headerName.MatchString(e.Auth.Name) || reservedHeader(e.Auth.Name) && !isAuthCarrier {
			return c.err(path+".auth.name", "身份请求头无效或属于保留名称")
		}
	case "query":
		if !safePathPart(e.Auth.Name) || reservedIdentity(e.Auth.Name) {
			return c.err(path+".auth.name", "身份查询参数名无效或属于保留名称")
		}
		return c.err(path+".auth.type", "不支持：尚未实现上游 query 认证")
	default:
		return c.err(path+".auth.type", "未知认证类型")
	}
	seen := map[string]bool{}
	for k, v := range e.Headers {
		canonical := http.CanonicalHeaderKey(k)
		if !headerName.MatchString(k) || reservedHeader(k) || strings.EqualFold(k, e.Auth.Name) || seen[canonical] || !text(v, 8192) {
			return c.err(path+".headers", "固定请求头无效、重复或属于保留名称")
		}
		seen[canonical] = true
	}
	for k, v := range e.Query {
		if !safePathPart(k) || reservedIdentity(k) || strings.EqualFold(k, e.Auth.Name) || k == "key" || !text(v, 8192) {
			return c.err(path+".query", "查询参数无效或属于保留名称")
		}
	}
	if len(e.Query) > 0 && e.Protocol != "gemini.generate_content" {
		return c.err(path+".query", "不支持：此操作尚未实现固定查询参数")
	}
	if e.Protocol == "anthropic.messages" {
		found := false
		for k, v := range e.Headers {
			if strings.EqualFold(k, "anthropic-version") && v == "2023-06-01" {
				found = true
			}
		}
		if !found {
			return c.err(path+".headers", "Anthropic 要求显式设置受支持的 anthropic-version 请求头")
		}
	}
	if e.Operation == "stream_generate_content" && e.Query["alt"] != "sse" {
		return c.err(path+".query", "Gemini 流式请求要求设置 alt: sse")
	}
	return nil
}

// 上游部署路径独立于客户端的操作入口。
func (c *compiler) upstreamPath(template, protocol, operation, path string) error {
	if !strings.HasPrefix(template, "/") || strings.HasPrefix(template, "//") || strings.ContainsAny(template, "?#") || !text(template, 8192) {
		return c.err(path, "需要不含 origin、查询参数、片段或控制字符的上游绝对路径")
	}
	u, err := url.ParseRequestURI(template)
	if err != nil || u.IsAbs() || u.Host != "" || u.RawQuery != "" || u.Fragment != "" || u.ForceQuery || strings.IndexFunc(u.Path, func(r rune) bool { return r < 32 || r == 127 }) >= 0 {
		return c.err(path, "上游路径或 URL 转义无效")
	}
	if protocol != "gemini.generate_content" {
		if strings.ContainsAny(template, "{}") {
			return c.err(path, "此协议未定义模型路径占位符")
		}
		return nil
	}
	if strings.Count(template, "{model}") != 1 || strings.ContainsAny(strings.ReplaceAll(template, "{model}", ""), "{}") {
		return c.err(path, "Gemini 要求恰好一个模型路径占位符")
	}
	suffix := ":generateContent"
	if operation == "stream_generate_content" {
		suffix = ":streamGenerateContent"
	}
	if !strings.HasSuffix(template, "/{model}"+suffix) {
		return c.err(path, "模型占位符必须占据操作路径的最后一段")
	}
	return nil
}

var attributes = []string{"account", "organization", "project", "quota_project", "region", "deployment", "resource", "boundary"}

func (c *compiler) identity(a, s map[string]string, path string) error {
	for k, v := range a {
		if !slices.Contains(attributes, k) || !text(v, 512) {
			return c.err(path+".attributes", "属性未知或身份值无效")
		}
	}
	selectors := map[string]string{"openai_organization": "organization", "openai_project": "project", "google_user_project": "quota_project"}
	for k, v := range s {
		attr, ok := selectors[k]
		if !ok || !text(v, 512) || a[attr] != v {
			return c.err(path+".selectors", "选择器必须与其声明的身份属性匹配")
		}
	}
	return nil
}

var resourceTypes = []string{"openai.response", "openai.conversation", "openai.item", "openai.prompt", "openai.file", "openai.vector_store", "openai.container", "openai.audio", "openai.opaque", "anthropic.file", "anthropic.container", "anthropic.opaque", "gemini.cached_content", "gemini.file", "gemini.cloud_object", "gemini.opaque"}

func (c *compiler) owner(o map[string]string, path string, resource bool) error {
	if len(o) == 0 {
		return c.err(path, "所有者身份不能为空")
	}
	if err := c.identity(o, nil, path); err != nil {
		return err
	}
	if resource {
		if _, ok := o["quota_project"]; ok {
			return c.err(path, "quota_project 不能作为资源所有者")
		}
	}
	return nil
}
func (c *compiler) scopes(p providerDTO, path string, targets map[string]catalog.Target) error {
	quotaSignatures := map[string]bool{}
	for _, sid := range sortedKeys(p.ResourceScopes) {
		s := p.ResourceScopes[sid]
		sp := path + ".resource_scopes." + sid
		if err := c.owner(s.Owner, sp+".owner", true); err != nil {
			return err
		}
		if len(s.ResourceTypes) == 0 {
			return c.err(sp+".resource_types", "资源类型不能为空")
		}
		if err := c.unique(s.ResourceTypes, sp+".resource_types", false); err != nil {
			return err
		}
		for _, t := range s.ResourceTypes {
			if !slices.Contains(resourceTypes, t) {
				return c.err(sp+".resource_types", "未知资源类型")
			}
		}
		sharing := s.Sharing
		if sharing == "" {
			sharing = "private"
		}
		if sharing != "private" && sharing != "verified_shared" {
			return c.err(sp+".sharing", "未知共享策略")
		}
		members := map[string]string{}
		for cid, cred := range p.Credentials {
			if cred.ResourceScope != sid {
				continue
			}
			for attr, value := range s.Owner {
				if cred.Identity.Attributes[attr] != value {
					return c.err(sp+".owner", "作用域所有者与凭据身份冲突")
				}
			}
			version := cred.SecretVersion
			if version == "" {
				version = "none"
			}
			members[cid] = version
		}
		if sharing == "private" && len(members) > 1 {
			return c.err(sp+".sharing", "private 资源作用域最多只能有一个凭据")
		}
		if sharing == "private" && s.Verification != nil {
			return c.err(sp+".verification", "验证信息要求使用 verified_shared 策略")
		}
		if sharing == "verified_shared" {
			v := s.Verification
			if v == nil {
				return c.err(sp+".verification", "verified_shared 要求提供验证信息")
			}
			if _, err := c.timestamp(v.CheckedAt, sp+".verification.checked_at"); err != nil {
				return err
			}
			if !text(v.Evidence, 4096) {
				return c.err(sp+".verification.evidence", "验证依据不能为空")
			}
			if len(v.CredentialVersions) != len(members) {
				return c.err(sp+".verification.credential_versions", "验证信息必须完整且准确地覆盖作用域成员")
			}
			for cid, ver := range members {
				if v.CredentialVersions[cid] != ver {
					return c.err(sp+".verification.credential_versions", "验证信息与成员密钥版本不匹配")
				}
			}
		}
	}
	for _, sid := range sortedKeys(p.QuotaScopes) {
		s := p.QuotaScopes[sid]
		sp := path + ".quota_scopes." + sid
		if err := c.owner(s.Owner, sp+".owner", false); err != nil {
			return err
		}
		if err := c.unique(s.Targets, sp+".targets", false); err != nil {
			return err
		}
		if s.Targets != nil && len(s.Targets) == 0 {
			return c.err(sp+".targets", "显式设置的 targets 不能为空；省略则使用提供方的全部目标")
		}
		for _, tid := range s.Targets {
			t, ok := targets[tid]
			if !ok || path != "$.providers."+t.ProviderID {
				return c.err(sp+".targets", "配额目标必须存在于同一提供方中")
			}
		}
		effective := quotaTargets(strings.TrimPrefix(path, "$.providers."), s, targets)
		ownerJSON, _ := json.Marshal(s.Owner)
		targetsJSON, _ := json.Marshal(effective)
		signature := string(ownerJSON) + string(targetsJSON)
		if quotaSignatures[signature] {
			return c.err(sp, "配额所有者与有效目标边界重复")
		}
		quotaSignatures[signature] = true
		if _, err := c.localLimits(s.Limits, sp+".limits"); err != nil {
			return err
		}
		for cid, cred := range p.Credentials {
			if !slices.Contains(cred.QuotaScopes, sid) {
				continue
			}
			for attr, value := range s.Owner {
				if cred.Identity.Attributes[attr] != value {
					return c.err(path+".credentials."+cid+".quota_scopes", "配额所有者与凭据身份冲突")
				}
			}
		}
		for _, t := range targets {
			if path != "$.providers."+t.ProviderID || !slices.Contains(t.QuotaScopes, sid) {
				continue
			}
			if len(s.Targets) > 0 && !slices.Contains(s.Targets, t.ID) {
				return c.err(sp+".targets", "绑定的配额作用域未覆盖目标")
			}
			for _, cid := range t.Credentials {
				cred := p.Credentials[cid]
				for attr, value := range s.Owner {
					if cred.Identity.Attributes[attr] != value {
						return c.err(sp+".owner", "绑定的配额所有者与凭据身份冲突")
					}
				}
			}
		}
	}
	return nil
}
func quotaTargets(provider string, s quotaScopeDTO, targets map[string]catalog.Target) []string {
	ids := slices.Clone(s.Targets)
	if s.Targets == nil {
		for id, t := range targets {
			if t.ProviderID == provider {
				ids = append(ids, id)
			}
		}
	}
	slices.Sort(ids)
	return ids
}
func (c *compiler) routeOptions(r routeDTO, path string) error {
	if r.ResourcePolicy.Mode != "stateless_pool" && r.ResourcePolicy.Mode != "fixed_scope" {
		return c.err(path+".resource_policy.mode", "未知资源策略")
	}
	if r.ResourcePolicy.Mode == "stateless_pool" && r.ResourcePolicy.Scope != "" {
		return c.err(path+".resource_policy.scope", "stateless_pool 不允许设置 scope")
	}
	if r.ResourcePolicy.Mode == "fixed_scope" {
		parts := strings.Split(r.ResourcePolicy.Scope, "/")
		if len(parts) != 2 || !safePathPart(parts[0]) || !safePathPart(parts[1]) {
			return c.err(path+".resource_policy.scope", "需要 provider/scope 格式")
		}
	}
	if r.Selection != nil {
		s := r.Selection
		if s.Strategy != "" && s.Strategy != "weighted" && s.Strategy != "latency_weighted" {
			return c.err(path+".selection.strategy", "未知选择策略")
		}
		if s.Latency != nil {
			x := s.Latency
			for _, v := range []struct {
				p    *int
				name string
			}{{x.MaxSamples, "max_samples"}, {x.MinSamples, "min_samples"}, {x.DegradeSamples, "degrade_samples"}, {x.RecoverySamples, "recovery_samples"}} {
				if v.p != nil && (*v.p <= 0 || *v.p > 2147483647) {
					return c.err(path+".selection.latency."+v.name, "需要正的 32 位整数")
				}
			}
			if x.Window != nil {
				if _, err := c.duration(*x.Window, path+".selection.latency.window", false); err != nil {
					return err
				}
			}
			if x.SlowWeightFactor != nil && !(*x.SlowWeightFactor > 0 && *x.SlowWeightFactor < 1) {
				return c.err(path+".selection.latency.slow_weight_factor", "比例必须在 (0, 1) 范围内")
			}
			if x.MaxSamples != nil && x.MinSamples != nil && *x.MaxSamples < *x.MinSamples {
				return c.err(path+".selection.latency.max_samples", "必须不小于 min_samples")
			}
			for _, v := range []struct {
				x    *thresholdDTO
				name string
			}{{x.Stream, "stream"}, {x.NonStream, "non_stream"}} {
				if v.x == nil {
					continue
				}
				if v.x.Slow == nil || v.x.Recovery == nil {
					return c.err(path+".selection.latency."+v.name, "必须同时设置两个性能阈值")
				}
				slow, err := c.duration(*v.x.Slow, path+".selection.latency."+v.name+".slow_threshold", false)
				if err != nil {
					return err
				}
				recovery, err := c.duration(*v.x.Recovery, path+".selection.latency."+v.name+".recovery_threshold", false)
				if err != nil {
					return err
				}
				if recovery >= slow {
					return c.err(path+".selection.latency."+v.name, "恢复阈值必须低于慢速阈值")
				}
			}
		}
		if s.Strategy == "latency_weighted" {
			return c.err(path+".selection.strategy", "不支持：尚未实现性能选择")
		}
	}
	if r.Affinity != nil {
		a := r.Affinity
		if a.Header != "" && (!headerName.MatchString(a.Header) || reservedHeader(a.Header) || strings.EqualFold(a.Header, "traceparent") || strings.EqualFold(a.Header, "user-agent")) {
			return c.err(path+".session_affinity.header", "亲和性请求头无效或属于保留名称")
		}
		if a.TTL != nil {
			if _, err := c.duration(*a.TTL, path+".session_affinity.ttl", false); err != nil {
				return err
			}
		}
		if a.Enabled {
			if a.TTL == nil {
				return c.err(path+".session_affinity.ttl", "启用亲和性时必须设置 TTL")
			}
			return c.err(path+".session_affinity.enabled", "不支持：尚未实现会话亲和性")
		}
	}
	return nil
}
func (c *compiler) observability(o *observabilityDTO) error {
	p := "$.observability"
	for _, v := range []struct {
		n    int
		name string
	}{{o.RecentRequestCapacity, "recent_request_capacity"}, {o.MaxObservedJSONBytes, "max_observed_json_bytes"}, {o.MaxObservedSSEEventBytes, "max_observed_sse_event_bytes"}, {o.MaxObservedStateBytes, "max_observed_state_bytes"}, {o.LogQueueCapacity, "log_queue_capacity"}} {
		if _, present := c.nodes[p+"."+v.name]; present && v.n <= 0 {
			return c.err(p+"."+v.name, "容量必须为正数")
		}
	}
	if o.CaptureBodies {
		return c.err(p+".capture_bodies", "不支持：尚未实现正文采集")
	}
	if o.JSONL != nil {
		x := o.JSONL
		if _, present := c.nodes[p+".jsonl.retention_days"]; present && x.RetentionDays <= 0 {
			return c.err(p+".jsonl.retention_days", "保留期限必须为正数")
		}
		if _, present := c.nodes[p+".jsonl.max_total_bytes"]; present && x.MaxTotalBytes <= 0 {
			return c.err(p+".jsonl.max_total_bytes", "字节容量必须为正数")
		}
		if x.Enabled {
			return c.err(p+".jsonl.enabled", "不支持：尚未实现 JSONL 日志")
		}
	}
	return nil
}
