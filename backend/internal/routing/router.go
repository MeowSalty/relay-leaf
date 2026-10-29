package routing

import (
	"math"
	"math/rand/v2"
	"net/http"
	"slices"
	"sort"
	"strings"
	"sync"

	"relayleaf/internal/catalog"
	"relayleaf/internal/protocol"
)

type rotationKey struct{ route, target string }

// Router 在不可变配置中筛选候选，独立轮换每个路由及目标的合法凭据。
type Router struct {
	providers map[string]catalog.Provider
	targets   map[string]catalog.Target
	routes    map[string]catalog.Route
	mu        sync.Mutex
	next      map[rotationKey]int
	random    func() float64
}

func NewRouter(snapshot *catalog.Snapshot) *Router {
	return &Router{providers: snapshot.Providers(), targets: snapshot.Targets(), routes: snapshot.Routes(), next: make(map[rotationKey]int), random: rand.Float64}
}

// Resolve 只做静态筛选，人工状态不参与授权或资源范围的判断。
func (r *Router) Resolve(key catalog.ClientKey, facts protocol.RequestFacts, _ *http.Request) (Selection, *protocol.GatewayError) {
	var route catalog.Route
	found := false
	for _, candidate := range r.routes {
		if candidate.Protocol == "openai.responses" && candidate.Operation == "create" && candidate.PublicModel == facts.Model {
			route, found = candidate, true
			break
		}
	}
	if !found {
		return Selection{}, gatewayError(404, "route_not_found", "公开模型没有配置路由")
	}
	if !slices.Contains(key.AllowedRoutes, route.ID) {
		return Selection{}, gatewayError(403, "route_forbidden", "客户端没有此路由的权限")
	}
	var targets []TargetCandidates
	for _, binding := range route.Targets {
		target, ok := r.targets[binding.Target]
		if !ok {
			continue
		}
		provider, ok := r.providers[target.ProviderID]
		if !ok {
			continue
		}
		endpoint, ok := provider.Endpoints[target.EndpointID]
		if !ok || endpoint.Protocol != route.Protocol || endpoint.Operation != route.Operation {
			continue
		}
		group := TargetCandidates{TargetID: target.ID, Priority: binding.Priority, Weight: binding.Weight}
		for _, id := range target.Credentials {
			credential, ok := provider.Credentials[id]
			if !ok || !groupsAllowed(key, credential.AccessGroups) {
				continue
			}
			group.Candidates = append(group.Candidates, Candidate{Provider: provider, Target: target, Endpoint: endpoint, Credential: credential, QuotaScopes: quotaScopes(provider, target, credential)})
		}
		if len(group.Candidates) > 0 {
			targets = append(targets, group)
		}
	}
	if len(targets) == 0 {
		return Selection{}, gatewayError(403, "upstream_forbidden", "客户端没有此路由上游凭据的访问组权限")
	}
	var err *protocol.GatewayError
	targets, err = r.resourceCandidates(route, facts, targets)
	if err != nil {
		return Selection{}, err
	}
	targets = filterCandidates(targets, func(c Candidate) bool {
		return slices.Contains(c.Target.RequestModes, facts.Mode) && slices.Contains(c.Endpoint.RequestModes, facts.Mode)
	})
	if len(targets) == 0 {
		return Selection{}, gatewayError(400, "unsupported_request_mode", "路由没有支持此请求模式的获准目标")
	}
	targets = filterCandidates(targets, func(c Candidate) bool { return containsAll(c.Target.Capabilities, facts.Capabilities) })
	if len(targets) == 0 {
		return Selection{}, gatewayError(400, "unsupported_capability", "路由没有支持必要能力的获准目标")
	}
	targets = filterCandidates(targets, func(c Candidate) bool {
		return route.Enabled && c.Provider.Enabled && c.Target.Enabled && c.Credential.Enabled && (c.Endpoint.AuthType == "none" || c.Credential.Secret.Reveal() != "")
	})
	if len(targets) == 0 {
		return Selection{}, gatewayError(503, "no_available_target", "路由没有可用的获准上游凭据")
	}
	return Selection{Route: route, Targets: targets}, nil
}

func groupsAllowed(key catalog.ClientKey, groups []string) bool {
	allowed := false
	for _, group := range groups {
		if slices.Contains(key.DeniedAccessGroups, group) {
			return false
		}
		allowed = allowed || slices.Contains(key.AllowedAccessGroups, group)
	}
	return allowed
}

func (r *Router) resourceCandidates(route catalog.Route, facts protocol.RequestFacts, targets []TargetCandidates) ([]TargetCandidates, *protocol.GatewayError) {
	if route.ResourcePolicy == "stateless_pool" {
		if len(facts.ResourceTypes) > 0 {
			return nil, gatewayError(409, "resource_owner_unknown", "当前路由不能确认硬资源引用的归属")
		}
		return targets, nil
	}
	providerID, scopeID, qualified := strings.Cut(route.ResourceScope, "/")
	provider, exists := r.providers[providerID]
	scope, declared := provider.ResourceScopes[scopeID]
	if route.ResourcePolicy != "fixed_scope" || !qualified || !exists || !declared {
		return nil, gatewayError(409, "resource_owner_unknown", "路由没有合法的明确资源作用域")
	}
	if !containsAll(scope.ResourceTypes, facts.ResourceTypes) {
		return nil, gatewayError(409, "resource_scope_conflict", "明确资源作用域未覆盖全部硬资源类型")
	}
	targets = filterCandidates(targets, func(c Candidate) bool {
		return c.Provider.ID == providerID && c.Credential.ResourceScope == scopeID
	})
	if len(targets) == 0 {
		return nil, gatewayError(503, "no_available_target", "获准上游与合法资源作用域没有交集")
	}
	if slices.Contains(facts.ResourceTypes, "openai.opaque") {
		model := targets[0].Candidates[0].Target.UpstreamName
		for _, target := range targets {
			for _, candidate := range target.Candidates {
				if candidate.Target.UpstreamName != model {
					return nil, gatewayError(409, "resource_scope_conflict", "不透明上下文的有效上游模型范围冲突")
				}
			}
		}
	}
	return targets, nil
}

func filterCandidates(targets []TargetCandidates, keep func(Candidate) bool) []TargetCandidates {
	result := make([]TargetCandidates, 0, len(targets))
	for _, target := range targets {
		candidates := make([]Candidate, 0, len(target.Candidates))
		for _, candidate := range target.Candidates {
			if keep(candidate) {
				candidates = append(candidates, candidate)
			}
		}
		if len(candidates) > 0 {
			target.Candidates = candidates
			result = append(result, target)
		}
	}
	return result
}

func quotaScopes(provider catalog.Provider, target catalog.Target, credential catalog.Credential) []string {
	var scopes []string
	seen := make(map[string]bool)
	for _, refs := range [][]string{credential.QuotaScopes, target.QuotaScopes} {
		for _, id := range refs {
			scope, ok := provider.QuotaScopes[id]
			if ok && !seen[id] && (len(scope.Targets) == 0 || slices.Contains(scope.Targets, target.ID)) {
				seen[id] = true
				scopes = append(scopes, provider.ID+"/"+id)
			}
		}
	}
	sort.Strings(scopes)
	return scopes
}

// Pick 使用本次运行可用性选择最小优先级目标，再轮换其可用凭据。
func (r *Router) Pick(selection Selection, available func(Candidate) bool) (Candidate, bool) {
	if available == nil {
		return Candidate{}, false
	}
	targets := filterCandidates(selection.Targets, available)
	if len(targets) == 0 {
		return Candidate{}, false
	}
	priority := targets[0].Priority
	for _, target := range targets {
		priority = min(priority, target.Priority)
	}
	var eligible []TargetCandidates
	maxWeight := 0.0
	for _, target := range targets {
		if target.Priority == priority && target.Weight > 0 && !math.IsInf(target.Weight, 0) && !math.IsNaN(target.Weight) {
			eligible = append(eligible, target)
			maxWeight = max(maxWeight, target.Weight)
		}
	}
	if len(eligible) == 0 {
		return Candidate{}, false
	}
	// 先缩放权重，避免有限正权重相加溢出。
	total := 0.0
	for _, target := range eligible {
		total += target.Weight / maxWeight
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	draw := r.random() * total
	chosen := eligible[len(eligible)-1]
	for _, target := range eligible {
		draw -= target.Weight / maxWeight
		if draw < 0 {
			chosen = target
			break
		}
	}
	key := rotationKey{selection.Route.ID, chosen.TargetID}
	start := r.next[key] % len(chosen.Candidates)
	r.next[key] = (start + 1) % len(chosen.Candidates)
	return chosen.Candidates[start], true
}

func containsAll(available, required []string) bool {
	for _, item := range required {
		if !slices.Contains(available, item) {
			return false
		}
	}
	return true
}
