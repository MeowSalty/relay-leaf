package config

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math"
	"net"
	"net/url"
	"regexp"
	"relayleaf/internal/catalog"
	"slices"
	"strings"
	"time"
)

func enabled(v *bool) bool             { return v == nil || *v }
func indexPath(p string, i int) string { return fmt.Sprintf("%s[%d]", p, i) }
func text(s string, max int) bool {
	return s != "" && len(s) <= max && strings.IndexFunc(s, func(r rune) bool { return r < 32 || r == 127 }) < 0
}
func (c *compiler) unique(xs []string, path string, ids bool) error {
	seen := map[string]bool{}
	for _, s := range xs {
		if seen[s] {
			return c.err(path, "列表成员重复")
		}
		seen[s] = true
		if ids && !safePathPart(s) {
			return c.err(path, "稳定标识符无效")
		}
	}
	return nil
}
func (c *compiler) timestamp(s, path string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, c.err(path, "需要包含时区的 RFC 3339 时间戳")
	}
	return t, nil
}
func (c *compiler) secretRef(s, path string) error {
	kind, value, ok := strings.Cut(s, ":")
	if !ok || value == "" || (kind != "env" && kind != "file") || strings.IndexFunc(value, func(r rune) bool { return r < 32 || r == 127 }) >= 0 {
		return c.err(path, "需要 env: 或 file: 密钥引用")
	}
	if kind == "env" && !regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`).MatchString(value) {
		return c.err(path, "环境变量名无效")
	}
	return nil
}
func secretPath(secret *string, path string) string {
	if secret != nil {
		return path + ".secret"
	}
	return path + ".secret_ref"
}
func (c *compiler) secretSource(secret, ref *string, path string, required bool) error {
	if secret != nil && ref != nil {
		return c.err(path+".secret", "secret 与 secret_ref 只能设置一个")
	}
	if secret != nil {
		return c.validateSecret(*secret, path+".secret")
	}
	if ref != nil {
		return c.secretRef(*ref, path+".secret_ref")
	}
	if required {
		return c.err(path, "必须设置 secret 或 secret_ref")
	}
	return nil
}
func (c *compiler) clientSecretSource(secret, ref *string, path string) error {
	if err := c.secretSource(secret, ref, path, true); err != nil {
		return err
	}
	if secret != nil {
		return c.validateClientMaterial(*secret, path+".secret")
	}
	return nil
}
func (c *compiler) localLimits(d *localLimitsDTO, path string) (catalog.LocalLimits, error) {
	l := catalog.LocalLimits{}
	if d == nil {
		return l, nil
	}
	if d.MaxInflight != nil {
		if *d.MaxInflight <= 0 || *d.MaxInflight > 2147483647 {
			return l, c.err(path+".max_inflight", "需要正的 32 位整数")
		}
		l.MaxInflight = *d.MaxInflight
	}
	if d.RequestRate != nil {
		x := d.RequestRate
		if x.Requests <= 0 || x.Requests > 2147483647 || x.Burst <= 0 || x.Burst > 2147483647 {
			return l, c.err(path+".request_rate", "requests 和 burst 必须为正的 32 位整数")
		}
		v, err := c.duration(x.Interval, path+".request_rate.interval", false)
		if err != nil {
			return l, err
		}
		if v < time.Millisecond || v > 24*time.Hour {
			return l, c.err(path+".request_rate.interval", "interval 必须在 1ms 到 24h 之间")
		}
		return l, c.err(path+".request_rate", "不支持：尚未实现请求速率限制")
	}
	return l, nil
}
func (c *compiler) limits(d limitsDTO) (catalog.Limits, error) {
	p := "$.limits"
	l := catalog.Limits{MaxInflight: d.MaxInflight, QueueCapacity: d.QueueCapacity, MaxRequestBodyBytes: d.MaxRequestBodyBytes, MaxDecompressedBodyBytes: d.MaxDecompressedBodyBytes, MaxTotalBodyBufferBytes: d.MaxTotalBodyBufferBytes, MaxTotalResponseBufferBytes: d.MaxTotalResponseBufferBytes, MaxRuntimeStateEntries: d.MaxRuntimeStateEntries, MaxSessionBindings: d.MaxSessionBindings, MaxSessionBindingsPerClient: d.MaxSessionBindingsPerClient, MaxSessionIDBytes: d.MaxSessionIDBytes, QueueWaitTimeout: 2 * time.Second}
	for _, v := range []struct {
		n    int
		name string
	}{{d.MaxInflight, "max_inflight"}, {d.MaxRuntimeStateEntries, "max_runtime_state_entries"}, {d.MaxSessionBindings, "max_session_bindings"}, {d.MaxSessionBindingsPerClient, "max_session_bindings_per_client"}, {d.MaxSessionIDBytes, "max_session_id_bytes"}} {
		if v.n <= 0 || v.n > 2147483647 {
			return l, c.err(p+"."+v.name, "容量必须为正的 32 位整数")
		}
	}
	for _, v := range []struct {
		n    int64
		name string
	}{{d.MaxRequestBodyBytes, "max_request_body_bytes"}, {d.MaxDecompressedBodyBytes, "max_decompressed_body_bytes"}, {d.MaxTotalBodyBufferBytes, "max_total_body_buffer_bytes"}, {d.MaxTotalResponseBufferBytes, "max_total_response_buffer_bytes"}} {
		if v.n <= 0 {
			return l, c.err(p+"."+v.name, "字节容量必须为正数")
		}
	}
	if d.QueueCapacity < 0 || d.QueueCapacity > 2147483647 || d.QueueCapacity > d.MaxInflight {
		return l, c.err(p+".queue_capacity", "队列容量必须非负且不超过 max_inflight")
	}
	if d.QueueWaitTimeout != nil {
		v, err := c.duration(*d.QueueWaitTimeout, p+".queue_wait_timeout", false)
		if err != nil {
			return l, err
		}
		l.QueueWaitTimeout = v
	}
	if d.MaxSessionBindingsPerClient > d.MaxSessionBindings {
		return l, c.err(p+".max_session_bindings_per_client", "不得超过会话总容量")
	}
	if d.QueueCapacity > 0 {
		return l, c.err(p+".queue_capacity", "不支持：尚未实现准入队列")
	}
	return l, nil
}
func (c *compiler) compile(ctx context.Context, d document) (*catalog.Snapshot, error) {
	if d.Version != 1 {
		return nil, c.err("$.version", "不支持的配置版本")
	}
	if d.Mode != "local" {
		return nil, c.err("$.mode", "不支持：仅实现了 local 预设")
	}
	if d.Source != "file" {
		return nil, c.err("$.source", "不支持：仅实现了 file 配置源")
	}
	host, port, err := net.SplitHostPort(d.Listeners.Gateway)
	if err != nil || port == "" {
		return nil, c.err("$.listeners.gateway", "需要显式设置回环地址，格式为 host:port")
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return nil, c.err("$.listeners.gateway", "local 预设要求使用回环 IP")
	}
	if n, err := parsePort(port); err != nil || n == 0 {
		return nil, c.err("$.listeners.gateway", "端口必须在 1 到 65535 之间")
	}
	if d.Listeners.Admin != "" {
		return nil, c.err("$.listeners.admin", "不支持：尚未实现管理监听器")
	}
	if d.UI.Enabled {
		return nil, c.err("$.ui.enabled", "不支持：尚未实现 UI")
	}
	if d.ClientAuth.Mode != "api_key" {
		return nil, c.err("$.client_auth.mode", "需要 api_key 客户端认证")
	}
	if d.ClientAuth.AllowGeminiQueryKey {
		return nil, c.err("$.client_auth.allow_gemini_query_key", "不支持：尚未实现 Gemini query 认证")
	}
	if d.ClientAuth.Keys == nil {
		return nil, c.err("$.client_auth.keys", "必须提供客户端密钥映射")
	}
	data := catalog.Data{Gateway: d.Listeners.Gateway, Providers: map[string]catalog.Provider{}, Targets: map[string]catalog.Target{}, Routes: map[string]catalog.Route{}, ClientKeys: map[string]catalog.ClientKey{}, SecretsResolved: c.options.ResolveSecrets}
	data.Observability = catalog.Observability{RecentRequestCapacity: 1000, MaxObservedJSONBytes: 1048576, MaxObservedSSEEventBytes: 262144, MaxObservedStateBytes: 262144, LogQueueCapacity: 2048}
	data.Execution, err = c.execution(defaultExecution(), d.Execution, "$.execution_defaults", 0)
	if err != nil {
		return nil, err
	}
	data.Health, err = c.health(d.Health)
	if err != nil {
		return nil, err
	}
	data.Limits, err = c.limits(d.Limits)
	if err != nil {
		return nil, err
	}
	if d.Observability != nil {
		if err := c.observability(d.Observability); err != nil {
			return nil, err
		}
		o := d.Observability
		if o.RecentRequestCapacity > 0 {
			data.Observability.RecentRequestCapacity = o.RecentRequestCapacity
		}
		if o.MaxObservedJSONBytes > 0 {
			data.Observability.MaxObservedJSONBytes = o.MaxObservedJSONBytes
		}
		if o.MaxObservedSSEEventBytes > 0 {
			data.Observability.MaxObservedSSEEventBytes = o.MaxObservedSSEEventBytes
		}
		if o.MaxObservedStateBytes > 0 {
			data.Observability.MaxObservedStateBytes = o.MaxObservedStateBytes
		}
		if o.LogQueueCapacity > 0 {
			data.Observability.LogQueueCapacity = o.LogQueueCapacity
		}
	}
	required := map[string]bool{}
	for _, pid := range sortedKeys(d.Providers) {
		p := d.Providers[pid]
		path := "$.providers." + pid
		u, err := url.Parse(p.Origin)
		if err != nil || u.Hostname() == "" || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.ForceQuery || strings.ContainsAny(p.Origin, "?#") || u.Path != "" && u.Path != "/" {
			return nil, c.err(path+".origin", "需要不含凭据、查询参数、片段或路径的 HTTP(S) origin")
		}
		if strings.HasSuffix(u.Host, ":") {
			return nil, c.err(path+".origin", "origin 中显式设置的端口必须在 1 到 65535 之间")
		}
		if port := u.Port(); port != "" {
			if n, err := parsePort(port); err != nil || n == 0 {
				return nil, c.err(path+".origin", "origin 中显式设置的端口必须在 1 到 65535 之间")
			}
		}
		provider := catalog.Provider{ID: pid, Name: p.Name, Origin: strings.TrimSuffix(p.Origin, "/"), Enabled: enabled(p.Enabled), Endpoints: map[string]catalog.Endpoint{}, Credentials: map[string]catalog.Credential{}, Health: data.Health}
		provider.ResourceScopes = map[string]catalog.ResourceScope{}
		provider.QuotaScopes = map[string]catalog.QuotaScope{}
		provider.Execution, err = c.execution(data.Execution, p.Execution, path+".execution", 1)
		if err != nil {
			return nil, err
		}
		if p.Health != nil && p.Health.CircuitBreaker != nil {
			b := p.Health.CircuitBreaker
			if b.NoAvailableTarget != nil {
				return nil, c.err(path+".health.circuit_breaker.no_available_target", "字段仅适用于全局配置")
			}
			if err := c.breaker(b, path+".health.circuit_breaker"); err != nil {
				return nil, err
			}
			provider.Health.CircuitBreakerEnabled = data.Health.CircuitBreakerEnabled && enabled(b.Enabled)
			if b.Rules != nil {
				provider.Health.Rules = c.domainHealthRules(b.Rules)
			}
			if b.ExemptUntil != nil {
				provider.Health.ExemptUntil, _ = c.timestamp(*b.ExemptUntil, path+".health.circuit_breaker.exempt_until")
			}
			if err := c.ruleCapacity(provider.Health.Rules, provider.Health.MaxSamples, path+".health.circuit_breaker.rules"); err != nil {
				return nil, err
			}
		}
		for _, eid := range sortedKeys(p.Endpoints) {
			e := p.Endpoints[eid]
			ep := path + ".endpoints." + eid
			if err := c.endpoint(e, ep); err != nil {
				return nil, err
			}
			provider.Endpoints[eid] = catalog.Endpoint{ID: eid, Protocol: e.Protocol, Operation: e.Operation, PathTemplate: e.PathTemplate, RequestModes: e.RequestModes, AuthType: e.Auth.Type, AuthName: e.Auth.Name, Headers: e.Headers, Query: e.Query}
		}
		for _, cid := range sortedKeys(p.Credentials) {
			v := p.Credentials[cid]
			cp := path + ".credentials." + cid
			if err := c.secretSource(v.Secret, v.SecretRef, cp, false); err != nil {
				return nil, err
			}
			if v.Secret != nil || v.SecretRef != nil {
				if !safePathPart(v.SecretVersion) || v.SecretVersion == "none" {
					return nil, c.err(cp+".secret_version", "密钥要求有效且非保留的版本")
				}
			} else if v.SecretVersion != "" {
				return nil, c.err(cp+".secret_version", "没有密钥内容时不允许设置密钥版本")
			}
			if err := c.identity(v.Identity.Attributes, v.Identity.Selectors, cp+".identity"); err != nil {
				return nil, err
			}
			if err := c.unique(v.QuotaScopes, cp+".quota_scopes", true); err != nil {
				return nil, err
			}
			for _, sid := range v.QuotaScopes {
				if _, ok := p.QuotaScopes[sid]; !ok {
					return nil, c.err(cp+".quota_scopes", "未知配额作用域")
				}
			}
			if v.ResourceScope != "" {
				if _, ok := p.ResourceScopes[v.ResourceScope]; !ok {
					return nil, c.err(cp+".resource_scope", "未知资源作用域")
				}
			}
			version := v.SecretVersion
			if version == "" {
				version = "none"
			}
			cred := catalog.Credential{ID: cid, Name: v.Name, Enabled: enabled(v.Enabled), Version: version, Attributes: v.Identity.Attributes, Selectors: v.Identity.Selectors, ResourceScope: v.ResourceScope, QuotaScopes: v.QuotaScopes}
			cred.Execution, err = c.execution(provider.Execution, v.Execution, cp+".execution", 2)
			if err != nil {
				return nil, err
			}
			provider.Credentials[cid] = cred
		}
		for _, mid := range sortedKeys(p.Models) {
			m := p.Models[mid]
			mp := path + ".models." + mid
			if !text(m.UpstreamName, 512) {
				return nil, c.err(mp+".upstream_name", "上游模型名不能为空")
			}
			for _, eid := range sortedKeys(m.Bindings) {
				b := m.Bindings[eid]
				bp := mp + ".bindings." + eid
				e, ok := provider.Endpoints[eid]
				if !ok {
					return nil, c.err(bp, "未知端点")
				}
				if len(b.Credentials) == 0 {
					return nil, c.err(bp+".credentials", "绑定要求非空的凭据集合")
				}
				if err := c.unique(b.Credentials, bp+".credentials", true); err != nil {
					return nil, err
				}
				if err := c.modes(b.RequestModes, bp+".request_modes"); err != nil {
					return nil, err
				}
				for _, mode := range b.RequestModes {
					if !slices.Contains(e.RequestModes, mode) {
						return nil, c.err(bp+".request_modes", "端点不支持绑定的请求模式")
					}
				}
				if err := c.unique(b.Capabilities, bp+".capabilities", false); err != nil {
					return nil, err
				}
				for _, cap := range b.Capabilities {
					if !slices.Contains([]string{"image_input", "tool_calling", "file_input", "audio_input", "video_input", "server_tools", "image_output", "audio_output"}, cap) {
						return nil, c.err(bp+".capabilities", "未知能力")
					}
					if cap != "image_input" && cap != "tool_calling" {
						return nil, c.err(bp+".capabilities", "不支持：尚未实现声明的能力")
					}
				}
				if err := c.unique(b.QuotaScopes, bp+".quota_scopes", true); err != nil {
					return nil, err
				}
				for _, sid := range b.QuotaScopes {
					if _, ok := p.QuotaScopes[sid]; !ok {
						return nil, c.err(bp+".quota_scopes", "未知配额作用域")
					}
				}
				limits, err := c.localLimits(b.Limits, bp+".limits")
				if err != nil {
					return nil, err
				}
				active := provider.Enabled && enabled(b.Enabled)
				available := false
				for _, cid := range b.Credentials {
					cred, ok := provider.Credentials[cid]
					if !ok {
						return nil, c.err(bp+".credentials", "提供方中不存在该凭据")
					}
					source := p.Credentials[cid]
					if source.Secret != nil && e.AuthType == "bearer" && !clientToken.MatchString(*source.Secret) {
						return nil, c.err(path+".credentials."+cid+".secret", "上游 Bearer 密钥的 token 语法无效")
					}
					if active && cred.Enabled {
						available = true
						if e.AuthType != "none" {
							if source.Secret == nil && source.SecretRef == nil {
								return nil, c.err(path+".credentials."+cid, "启用的认证要求设置 secret 或 secret_ref")
							}
							required[pid+"/"+cid] = true
						}
						if cred.Execution.MaxRetries > 0 {
							return nil, c.err("$.execution_defaults.max_retries", "不支持：尚未实现自动重试")
						}
						if provider.Health.CircuitBreakerEnabled {
							return nil, c.err("$.health_defaults.circuit_breaker.enabled", "不支持：尚未实现自动熔断")
						}
					}
				}
				if active && !available {
					return nil, c.err(bp+".credentials", "启用的绑定需要启用的凭据")
				}
				if active && (e.Protocol != "openai.responses" || e.Operation != "create") {
					return nil, c.err(path+".endpoints."+eid+".operation", "不支持：仅实现了 Responses create")
				}
				tid := pid + "/" + mid + "/" + eid
				data.Targets[tid] = catalog.Target{ID: tid, ProviderID: pid, ModelID: mid, EndpointID: eid, UpstreamName: m.UpstreamName, Enabled: active, Credentials: b.Credentials, RequestModes: b.RequestModes, Capabilities: b.Capabilities, QuotaScopes: b.QuotaScopes, Limits: limits}
			}
		}
		if err := c.scopes(p, path, data.Targets); err != nil {
			return nil, err
		}
		for sid, s := range p.ResourceScopes {
			scope := catalog.ResourceScope{ID: sid, Name: s.Name, Sharing: s.Sharing, Owner: s.Owner, ResourceTypes: s.ResourceTypes}
			if scope.Sharing == "" {
				scope.Sharing = "private"
			}
			if s.Verification != nil {
				scope.CheckedAt, _ = c.timestamp(s.Verification.CheckedAt, path+".resource_scopes."+sid+".verification.checked_at")
				scope.Evidence = s.Verification.Evidence
				scope.CredentialVersions = s.Verification.CredentialVersions
			}
			provider.ResourceScopes[sid] = scope
		}
		for sid, s := range p.QuotaScopes {
			limits, _ := c.localLimits(s.Limits, path+".quota_scopes."+sid+".limits")
			provider.QuotaScopes[sid] = catalog.QuotaScope{ID: sid, Name: s.Name, Owner: s.Owner, Targets: quotaTargets(pid, s, data.Targets), Limits: limits}
		}
		data.Providers[pid] = provider
	}
	routeKeys := map[string]bool{}
	for _, rid := range sortedKeys(d.Routes) {
		r := d.Routes[rid]
		path := "$.routes." + rid
		if !text(r.PublicModel, 512) {
			return nil, c.err(path+".public_model", "公开模型名不能为空")
		}
		if err := operation(r.Protocol, r.Operation); err != nil {
			return nil, c.err(path+".operation", "未知协议操作")
		}
		key := r.Protocol + "/" + r.Operation + "/" + r.PublicModel
		if routeKeys[key] {
			return nil, c.err(path, "协议、操作与公开模型组合冲突")
		}
		routeKeys[key] = true
		if len(r.Targets) == 0 {
			return nil, c.err(path+".targets", "路由必须设置目标")
		}
		if err := c.routeOptions(r, path); err != nil {
			return nil, err
		}
		route := catalog.Route{ID: rid, Name: r.Name, Enabled: enabled(r.Enabled), Protocol: r.Protocol, Operation: r.Operation, PublicModel: r.PublicModel, ResourcePolicy: r.ResourcePolicy.Mode, ResourceScope: r.ResourcePolicy.Scope}
		activeCount := 0
		seen := map[string]bool{}
		for i, rt := range r.Targets {
			rp := indexPath(path+".targets", i)
			t, ok := data.Targets[rt.Target]
			if !ok {
				return nil, c.err(rp+".target", "未知目标")
			}
			if seen[rt.Target] {
				return nil, c.err(rp+".target", "目标重复")
			}
			seen[rt.Target] = true
			e := data.Providers[t.ProviderID].Endpoints[t.EndpointID]
			if e.Protocol != r.Protocol || e.Operation != r.Operation {
				return nil, c.err(rp+".target", "目标协议或操作不匹配")
			}
			if rt.Priority < 0 || rt.Priority > 2147483647 || !(rt.Weight > 0) || math.IsInf(rt.Weight, 0) || math.IsNaN(rt.Weight) {
				return nil, c.err(rp, "优先级必须非负，权重必须为有限正数")
			}
			if t.Enabled {
				activeCount++
			}
			route.Targets = append(route.Targets, catalog.RouteTarget{Target: rt.Target, Priority: rt.Priority, Weight: rt.Weight})
		}
		if route.Enabled && activeCount > 1 {
			return nil, c.err(path+".targets", "不支持：尚未实现多个启用的目标")
		}
		if route.Enabled && activeCount == 0 {
			return nil, c.err(path+".targets", "启用的路由要求存在启用的目标")
		}
		data.Routes[rid] = route
	}
	if len(data.Targets)+len(data.ClientKeys) > data.Limits.MaxRuntimeStateEntries {
		return nil, c.err("$.limits.max_runtime_state_entries", "候选配置超出运行时身份容量")
	}
	// 所有结构校验均在读取外部密钥内容之前完成。
	digests := map[[32]byte]string{}
	for _, kid := range sortedKeys(d.ClientAuth.Keys) {
		k := d.ClientAuth.Keys[kid]
		path := "$.client_auth.keys." + kid
		if err := c.clientSecretSource(k.Secret, k.SecretRef, path); err != nil {
			return nil, err
		}
		if !safePathPart(k.SecretVersion) {
			return nil, c.err(path+".secret_version", "必须设置有效的密钥版本")
		}
		if k.AllowedRoutes == nil {
			return nil, c.err(path+".allowed_routes", "必须显式设置路由授权列表")
		}
		if err := c.unique(k.AllowedRoutes, path+".allowed_routes", true); err != nil {
			return nil, err
		}
		for _, rid := range k.AllowedRoutes {
			if _, ok := data.Routes[rid]; !ok {
				return nil, c.err(path+".allowed_routes", "路由授权引用了未知路由")
			}
		}
		limits, err := c.localLimits(k.Limits, path+".limits")
		if err != nil {
			return nil, err
		}
		ck := catalog.ClientKey{ID: kid, Name: k.Name, Version: k.SecretVersion, Enabled: enabled(k.Enabled), AllowedRoutes: k.AllowedRoutes, Limits: limits}
		if k.ExpiresAt != nil {
			ck.ExpiresAt, err = c.timestamp(*k.ExpiresAt, path+".expires_at")
			if err != nil {
				return nil, err
			}
		}
		data.ClientKeys[kid] = ck
		if k.Retiring != nil {
			r := k.Retiring
			if err := c.clientSecretSource(r.Secret, r.SecretRef, path+".retiring_secret"); err != nil {
				return nil, err
			}
			if !safePathPart(r.SecretVersion) || r.SecretVersion == k.SecretVersion {
				return nil, c.err(path+".retiring_secret.secret_version", "退役密钥版本必须有效且不同于当前版本")
			}
			if _, err := c.timestamp(r.ValidUntil, path+".retiring_secret.valid_until"); err != nil {
				return nil, err
			}
			ck.RetiringVersion = r.SecretVersion
			ck.RetiringValidUntil, _ = c.timestamp(r.ValidUntil, path+".retiring_secret.valid_until")
			data.ClientKeys[kid] = ck
		}
	}
	identities := len(data.Targets) + len(data.ClientKeys)
	for _, p := range data.Providers {
		identities += len(p.Credentials) + len(p.QuotaScopes) + len(p.ResourceScopes)
	}
	if identities > data.Limits.MaxRuntimeStateEntries {
		return nil, c.err("$.limits.max_runtime_state_entries", "候选配置超出运行时身份容量")
	}
	if c.options.ResolveSecrets {
		addVerification := func(kid string, secret, ref *string, path string, until time.Time) error {
			material, err := c.clientMaterial(ctx, secret, ref, path)
			if err != nil {
				return err
			}
			digest := sha256.Sum256([]byte(material.Reveal()))
			if _, ok := digests[digest]; ok {
				return c.err(secretPath(secret, path), "可接受的客户端密钥内容重复")
			}
			digests[digest] = kid
			data.Verifications = append(data.Verifications, catalog.Verification{ClientID: kid, Material: material, ValidUntil: until})
			return nil
		}
		for _, kid := range sortedKeys(d.ClientAuth.Keys) {
			k := d.ClientAuth.Keys[kid]
			ck := data.ClientKeys[kid]
			if !ck.Enabled || !ck.ExpiresAt.IsZero() && !c.now.Before(ck.ExpiresAt) {
				continue
			}
			path := "$.client_auth.keys." + kid
			if err := addVerification(kid, k.Secret, k.SecretRef, path, ck.ExpiresAt); err != nil {
				return nil, err
			}
			if k.Retiring != nil {
				until, _ := c.timestamp(k.Retiring.ValidUntil, path+".retiring_secret.valid_until")
				if c.now.Before(until) {
					if !ck.ExpiresAt.IsZero() && ck.ExpiresAt.Before(until) {
						until = ck.ExpiresAt
					}
					if err := addVerification(kid, k.Retiring.Secret, k.Retiring.SecretRef, path+".retiring_secret", until); err != nil {
						return nil, err
					}
				}
			}
		}
		for _, pid := range sortedKeys(data.Providers) {
			provider := data.Providers[pid]
			for _, cid := range sortedKeys(provider.Credentials) {
				if !required[pid+"/"+cid] {
					continue
				}
				cred := provider.Credentials[cid]
				source := d.Providers[pid].Credentials[cid]
				path := "$.providers." + pid + ".credentials." + cid
				material, err := c.resolve(ctx, source.Secret, source.SecretRef, path)
				if err != nil {
					return nil, err
				}
				cred.Secret = material
				for _, target := range data.Targets {
					if target.ProviderID == pid && target.Enabled && slices.Contains(target.Credentials, cid) && provider.Endpoints[target.EndpointID].AuthType == "bearer" && !clientToken.MatchString(material.Reveal()) {
						return nil, c.err(secretPath(source.Secret, path), "上游 Bearer 密钥的 token 语法无效")
					}
				}
				provider.Credentials[cid] = cred
			}
			data.Providers[pid] = provider
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return catalog.NewSnapshot(data), nil
}

var clientToken = regexp.MustCompile(`^[A-Za-z0-9._~+/\-]+=*$`)

func (c *compiler) clientMaterial(ctx context.Context, secret, ref *string, path string) (catalog.Secret, error) {
	s, err := c.resolve(ctx, secret, ref, path)
	if err != nil {
		return s, err
	}
	if err := c.validateClientMaterial(s.Reveal(), secretPath(secret, path)); err != nil {
		return catalog.Secret{}, err
	}
	return s, nil
}
func (c *compiler) validateClientMaterial(material, path string) error {
	if len(material) < 32 || len(material) > 512 || !clientToken.MatchString(material) {
		return c.err(path, "客户端密钥必须为 32–512 字节的 ASCII Bearer token")
	}
	return nil
}
func sortedKeys[V any](m map[string]V) []string {
	xs := make([]string, 0, len(m))
	for k := range m {
		xs = append(xs, k)
	}
	slices.Sort(xs)
	return xs
}
func parsePort(s string) (int, error) {
	n := 0
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("端口无效")
		}
		n = n*10 + int(r-'0')
		if n > 65535 {
			return 0, fmt.Errorf("端口无效")
		}
	}
	return n, nil
}
