package routing

import (
	"net/http"
	"slices"
	"sort"
	"sync"

	"relayleaf/internal/catalog"
	"relayleaf/internal/protocol"
)

// Router 在不可变配置中筛选候选，独立轮换每个路由的合法凭据。
type Router struct {
	providers map[string]catalog.Provider
	targets   map[string]catalog.Target
	routes    map[string]catalog.Route
	mu        sync.Mutex
	next      map[string]uint64
}

func NewRouter(snapshot *catalog.Snapshot) *Router {
	return &Router{providers: snapshot.Providers(), targets: snapshot.Targets(), routes: snapshot.Routes(), next: make(map[string]uint64)}
}

// Resolve 只在精确模型及客户端授权内选择，运行许可由执行管理器复核。
func (r *Router) Resolve(key catalog.ClientKey, facts protocol.RequestFacts, _ *http.Request) (Selection, *protocol.GatewayError) {
	var route catalog.Route
	found := false
	for _, candidate := range r.routes {
		if candidate.Enabled && candidate.Protocol == "openai.responses" && candidate.Operation == "create" && candidate.PublicModel == facts.Model {
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
	if route.ResourcePolicy != "stateless_pool" || len(facts.ResourceTypes) > 0 {
		return Selection{}, gatewayError(409, "resource_owner_unknown", "当前路由不能确认硬资源引用的归属")
	}
	var candidates []Candidate
	hasTarget, hasMode, hasCapability := false, false, false
	for _, binding := range route.Targets {
		target, ok := r.targets[binding.Target]
		if !ok || !target.Enabled {
			continue
		}
		provider, ok := r.providers[target.ProviderID]
		if !ok || !provider.Enabled {
			continue
		}
		endpoint, ok := provider.Endpoints[target.EndpointID]
		if !ok || endpoint.Protocol != route.Protocol || endpoint.Operation != route.Operation {
			continue
		}
		hasTarget = true
		if !slices.Contains(target.RequestModes, facts.Mode) || !slices.Contains(endpoint.RequestModes, facts.Mode) {
			continue
		}
		hasMode = true
		if !containsAll(target.Capabilities, facts.Capabilities) {
			continue
		}
		hasCapability = true
		for _, id := range target.Credentials {
			credential, ok := provider.Credentials[id]
			if !ok || !credential.Enabled || endpoint.AuthType != "none" && credential.Secret.Reveal() == "" {
				continue
			}
			var scopes []string
			seen := make(map[string]bool)
			for _, refs := range [][]string{credential.QuotaScopes, target.QuotaScopes} {
				for _, scopeID := range refs {
					scope, ok := provider.QuotaScopes[scopeID]
					if ok && !seen[scopeID] && (len(scope.Targets) == 0 || slices.Contains(scope.Targets, target.ID)) {
						seen[scopeID] = true
						scopes = append(scopes, provider.ID+"/"+scopeID)
					}
				}
			}
			sort.Strings(scopes)
			candidates = append(candidates, Candidate{Provider: provider, Target: target, Endpoint: endpoint, Credential: credential, QuotaScopes: scopes})
		}
	}
	if !hasTarget {
		return Selection{}, gatewayError(503, "no_available_target", "路由没有启用的上游目标")
	}
	if !hasMode {
		return Selection{}, gatewayError(400, "unsupported_request_mode", "路由没有支持此请求模式的目标")
	}
	if !hasCapability {
		return Selection{}, gatewayError(400, "unsupported_capability", "路由没有支持必要能力的目标")
	}
	if len(candidates) == 0 {
		return Selection{}, gatewayError(503, "no_available_target", "路由没有可用的上游凭据")
	}
	r.mu.Lock()
	start := int(r.next[route.ID] % uint64(len(candidates)))
	r.next[route.ID]++
	r.mu.Unlock()
	rotated := append(candidates[start:], candidates[:start]...)
	return Selection{Route: route, Candidates: rotated}, nil
}

func containsAll(available, required []string) bool {
	for _, item := range required {
		if !slices.Contains(available, item) {
			return false
		}
	}
	return true
}
