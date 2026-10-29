package routing

import (
	"slices"
	"sync"
	"testing"

	"relayleaf/internal/catalog"
	"relayleaf/internal/protocol"
)

func routingData() catalog.Data {
	return catalog.Data{
		Providers: map[string]catalog.Provider{"p": {ID: "p", Enabled: true, Origin: "https://upstream.example", Endpoints: map[string]catalog.Endpoint{"e": {ID: "e", Protocol: "openai.responses", Operation: "create", PathTemplate: "/v1/responses", AuthType: "bearer", RequestModes: []string{"non_stream", "stream"}}}, Credentials: map[string]catalog.Credential{
			"disabled": {ID: "disabled", Enabled: false}, "missing": {ID: "missing", Enabled: true},
			"a": {ID: "a", Enabled: true, Secret: catalog.NewSecret("upstream-a"), QuotaScopes: []string{"account", "other"}},
			"b": {ID: "b", Enabled: true, Secret: catalog.NewSecret("upstream-b"), QuotaScopes: []string{"account"}},
		}, QuotaScopes: map[string]catalog.QuotaScope{"account": {ID: "account"}, "target": {ID: "target", Targets: []string{"p/m/e"}}, "other": {ID: "other", Targets: []string{"p/other/e"}}}}},
		Targets: map[string]catalog.Target{"p/m/e": {ID: "p/m/e", Enabled: true, ProviderID: "p", EndpointID: "e", UpstreamName: "real-model", RequestModes: []string{"non_stream", "stream"}, Capabilities: []string{"tools"}, Credentials: []string{"disabled", "missing", "a", "b"}, QuotaScopes: []string{"account", "target"}}},
		Routes:  map[string]catalog.Route{"r": {ID: "r", Enabled: true, Protocol: "openai.responses", Operation: "create", PublicModel: "public-model", ResourcePolicy: "stateless_pool", Targets: []catalog.RouteTarget{{Target: "p/m/e"}}}},
	}
}

func TestResolveFiltersEachCredentialAndDeduplicatesApplicableScopes(t *testing.T) {
	router := NewRouter(catalog.NewSnapshot(routingData()))
	key := catalog.ClientKey{ID: "c", AllowedRoutes: []string{"r"}}
	facts := protocol.RequestFacts{Model: "public-model", Mode: "non_stream", Capabilities: []string{"tools"}}
	for i := range 4 {
		selection, err := router.Resolve(key, facts, nil)
		if err != nil || len(selection.Candidates) != 2 {
			t.Fatalf("候选过滤失败：%v %+v", err, selection)
		}
		want := []string{"a", "b"}[i%2]
		if selection.Candidates[0].Credential.ID != want {
			t.Fatalf("凭据未轮换：得到 %s，预期 %s", selection.Candidates[0].Credential.ID, want)
		}
		for _, candidate := range selection.Candidates {
			if !slices.Equal(candidate.QuotaScopes, []string{"p/account", "p/target"}) {
				t.Fatalf("配额作用域并集错误：%v", candidate.QuotaScopes)
			}
		}
	}
}

func TestResolveRejectsUnauthorizedUnsupportedAndHardReferences(t *testing.T) {
	for _, tc := range []struct {
		name   string
		modify func(*catalog.Data, *catalog.ClientKey, *protocol.RequestFacts)
		code   string
	}{
		{"模型精确匹配", func(_ *catalog.Data, _ *catalog.ClientKey, f *protocol.RequestFacts) { f.Model = "Public-model" }, "route_not_found"},
		{"模型不修剪", func(_ *catalog.Data, _ *catalog.ClientKey, f *protocol.RequestFacts) { f.Model += " " }, "route_not_found"},
		{"路由授权", func(_ *catalog.Data, k *catalog.ClientKey, _ *protocol.RequestFacts) { k.AllowedRoutes = nil }, "route_forbidden"},
		{"模式", func(_ *catalog.Data, _ *catalog.ClientKey, f *protocol.RequestFacts) { f.Mode = "background" }, "unsupported_request_mode"},
		{"能力", func(_ *catalog.Data, _ *catalog.ClientKey, f *protocol.RequestFacts) {
			f.Capabilities = []string{"image"}
		}, "unsupported_capability"},
		{"硬引用", func(_ *catalog.Data, _ *catalog.ClientKey, f *protocol.RequestFacts) {
			f.ResourceTypes = []string{"openai.response"}
		}, "resource_owner_unknown"},
		{"固定资源策略未开放", func(d *catalog.Data, _ *catalog.ClientKey, _ *protocol.RequestFacts) {
			r := d.Routes["r"]
			r.ResourcePolicy = "fixed_scope"
			d.Routes["r"] = r
		}, "resource_owner_unknown"},
		{"目标禁用", func(d *catalog.Data, _ *catalog.ClientKey, _ *protocol.RequestFacts) {
			target := d.Targets["p/m/e"]
			target.Enabled = false
			d.Targets[target.ID] = target
		}, "no_available_target"},
		{"凭据禁用", func(d *catalog.Data, _ *catalog.ClientKey, _ *protocol.RequestFacts) {
			p := d.Providers["p"]
			for id, c := range p.Credentials {
				c.Enabled = false
				p.Credentials[id] = c
			}
			d.Providers["p"] = p
		}, "no_available_target"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, key, facts := routingData(), catalog.ClientKey{AllowedRoutes: []string{"r"}}, protocol.RequestFacts{Model: "public-model", Mode: "non_stream"}
			tc.modify(&data, &key, &facts)
			_, err := NewRouter(catalog.NewSnapshot(data)).Resolve(key, facts, nil)
			if err == nil || err.Code != tc.code {
				t.Fatalf("路由拒绝码=%v，预期 %s", err, tc.code)
			}
		})
	}
}

func TestResolveConcurrentRotationIsBalanced(t *testing.T) {
	router := NewRouter(catalog.NewSnapshot(routingData()))
	var wg sync.WaitGroup
	results := make(chan string, 100)
	for range 100 {
		wg.Go(func() {
			selection, err := router.Resolve(catalog.ClientKey{AllowedRoutes: []string{"r"}}, protocol.RequestFacts{Model: "public-model", Mode: "stream"}, nil)
			if err != nil {
				t.Errorf("并发路由失败：%v", err)
				return
			}
			results <- selection.Candidates[0].Credential.ID
		})
	}
	wg.Wait()
	close(results)
	counts := make(map[string]int)
	for id := range results {
		counts[id]++
	}
	if counts["a"] != 50 || counts["b"] != 50 {
		t.Fatalf("并发凭据轮换不均衡：%v", counts)
	}
}

func TestResolveKeepsProviderAndRouteScopeIdentitiesSeparate(t *testing.T) {
	data := routingData()
	second := data.Providers["p"]
	second.ID = "q"
	data.Providers["q"] = second
	secondTarget := data.Targets["p/m/e"]
	secondTarget.ID, secondTarget.ProviderID = "q/m/e", "q"
	secondTarget.QuotaScopes = []string{"account"}
	data.Targets[secondTarget.ID] = secondTarget
	secondRoute := data.Routes["r"]
	secondRoute.ID, secondRoute.PublicModel = "s", "second-model"
	secondRoute.Targets = []catalog.RouteTarget{{Target: secondTarget.ID}}
	data.Routes[secondRoute.ID] = secondRoute
	router := NewRouter(catalog.NewSnapshot(data))
	for _, tc := range []struct{ model, provider, scope string }{{"public-model", "p", "p/account"}, {"second-model", "q", "q/account"}} {
		selection, err := router.Resolve(catalog.ClientKey{AllowedRoutes: []string{"r", "s"}}, protocol.RequestFacts{Model: tc.model, Mode: "non_stream"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, candidate := range selection.Candidates {
			if candidate.Provider.ID != tc.provider || !slices.Contains(candidate.QuotaScopes, tc.scope) {
				t.Fatalf("不同提供方的同名资源混合：%+v", candidate)
			}
		}
	}
}
