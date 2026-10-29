package routing

import (
	"math"
	"slices"
	"strconv"
	"testing"

	"relayleaf/internal/catalog"
	"relayleaf/internal/protocol"
)

func authorizedKey() catalog.ClientKey {
	return catalog.ClientKey{AllowedRoutes: []string{"r"}, AllowedAccessGroups: []string{"general"}}
}

func requestFacts() protocol.RequestFacts {
	return protocol.RequestFacts{Model: "public-model", Mode: "non_stream"}
}

func TestResolveAccessGroupsAndErrorOrder(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*catalog.Data, *catalog.ClientKey, *protocol.RequestFacts)
		status int
		code   string
		ids    []string
	}{
		{"允许组任一相交", func(d *catalog.Data, k *catalog.ClientKey, _ *protocol.RequestFacts) {
			k.AllowedAccessGroups = []string{"other", "general"}
		}, 0, "", []string{"a", "b"}},
		{"允许空集合", func(_ *catalog.Data, k *catalog.ClientKey, f *protocol.RequestFacts) {
			k.AllowedAccessGroups = nil
			f.ResourceTypes = []string{"openai.file"}
		}, 403, "upstream_forbidden", nil},
		{"拒绝覆盖允许", func(_ *catalog.Data, k *catalog.ClientKey, _ *protocol.RequestFacts) {
			k.DeniedAccessGroups = []string{"general"}
		}, 403, "upstream_forbidden", nil},
		{"凭据任一拒绝组即排除", func(d *catalog.Data, k *catalog.ClientKey, _ *protocol.RequestFacts) {
			p := d.Providers["p"]
			c := p.Credentials["a"]
			c.AccessGroups = []string{"general", "blocked"}
			p.Credentials["a"] = c
			d.Providers["p"] = p
			k.DeniedAccessGroups = []string{"blocked"}
		}, 0, "", []string{"b"}},
		{"漏标凭据排除", func(d *catalog.Data, _ *catalog.ClientKey, _ *protocol.RequestFacts) {
			p := d.Providers["p"]
			c := p.Credentials["a"]
			c.AccessGroups = nil
			p.Credentials["a"] = c
			d.Providers["p"] = p
		}, 0, "", []string{"b"}},
		{"路由授权优先", func(d *catalog.Data, k *catalog.ClientKey, f *protocol.RequestFacts) {
			r := d.Routes["r"]
			r.Enabled = false
			d.Routes["r"] = r
			k.AllowedRoutes = nil
			k.AllowedAccessGroups = nil
			f.ResourceTypes = []string{"openai.file"}
		}, 403, "route_forbidden", nil},
		{"禁用路由仍匹配", func(d *catalog.Data, _ *catalog.ClientKey, _ *protocol.RequestFacts) {
			r := d.Routes["r"]
			r.Enabled = false
			d.Routes["r"] = r
		}, 503, "no_available_target", nil},
		{"禁用提供方不改变组授权", func(d *catalog.Data, _ *catalog.ClientKey, _ *protocol.RequestFacts) {
			p := d.Providers["p"]
			p.Enabled = false
			d.Providers["p"] = p
		}, 503, "no_available_target", nil},
		{"只授权停用凭据", func(d *catalog.Data, k *catalog.ClientKey, _ *protocol.RequestFacts) {
			p := d.Providers["p"]
			c := p.Credentials["disabled"]
			c.AccessGroups = []string{"disabled-only"}
			p.Credentials["disabled"] = c
			d.Providers["p"] = p
			k.AllowedAccessGroups = []string{"disabled-only"}
		}, 503, "no_available_target", nil},
		{"只授权缺失秘密凭据", func(d *catalog.Data, k *catalog.ClientKey, _ *protocol.RequestFacts) {
			p := d.Providers["p"]
			c := p.Credentials["missing"]
			c.AccessGroups = []string{"missing-only"}
			p.Credentials["missing"] = c
			d.Providers["p"] = p
			k.AllowedAccessGroups = []string{"missing-only"}
		}, 503, "no_available_target", nil},
		{"资源优先于模式和人工", func(d *catalog.Data, _ *catalog.ClientKey, f *protocol.RequestFacts) {
			r := d.Routes["r"]
			r.Enabled = false
			d.Routes["r"] = r
			f.Mode = "background"
			f.Capabilities = []string{"unknown"}
			f.ResourceTypes = []string{"openai.file"}
		}, 409, "resource_owner_unknown", nil},
		{"模式优先于人工", func(d *catalog.Data, _ *catalog.ClientKey, f *protocol.RequestFacts) {
			target := d.Targets["p/m/e"]
			target.Enabled = false
			d.Targets[target.ID] = target
			f.Mode = "background"
			f.Capabilities = []string{"unknown"}
		}, 400, "unsupported_request_mode", nil},
		{"能力优先于人工", func(d *catalog.Data, _ *catalog.ClientKey, f *protocol.RequestFacts) {
			target := d.Targets["p/m/e"]
			target.Enabled = false
			d.Targets[target.ID] = target
			f.Capabilities = []string{"unknown"}
		}, 400, "unsupported_capability", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, key, facts := routingData(), authorizedKey(), requestFacts()
			tc.change(&data, &key, &facts)
			selection, err := NewRouter(catalog.NewSnapshot(data)).Resolve(key, facts, nil)
			if tc.code != "" {
				if err == nil || err.Code != tc.code || err.Status != tc.status {
					t.Fatalf("错误=%v，预期 %d/%s", err, tc.status, tc.code)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			var ids []string
			for _, target := range selection.Targets {
				for _, c := range target.Candidates {
					ids = append(ids, c.Credential.ID)
				}
			}
			if !slices.Equal(ids, tc.ids) {
				t.Fatalf("获准凭据=%v，预期 %v", ids, tc.ids)
			}
		})
	}
}

func fixedScopeData() catalog.Data {
	data := routingData()
	p := data.Providers["p"]
	p.ResourceScopes = map[string]catalog.ResourceScope{
		"shared": {ID: "shared", Sharing: "verified_shared", ResourceTypes: []string{"openai.file", "openai.response", "openai.opaque"}},
		"other":  {ID: "other", ResourceTypes: []string{"openai.file"}},
	}
	for id, c := range p.Credentials {
		c.ResourceScope = "shared"
		p.Credentials[id] = c
	}
	data.Providers["p"] = p
	r := data.Routes["r"]
	r.ResourcePolicy = "fixed_scope"
	r.ResourceScope = "p/shared"
	data.Routes["r"] = r
	return data
}

func addSecondTarget(data *catalog.Data) {
	target := data.Targets["p/m/e"]
	target.ID = "p/second/e"
	data.Targets[target.ID] = target
	route := data.Routes["r"]
	route.Targets = append(route.Targets, catalog.RouteTarget{Target: target.ID, Priority: 1, Weight: 3})
	data.Routes["r"] = route
}

func TestResolveFixedScopeCoverageAndIntersection(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*catalog.Data, *catalog.ClientKey, *protocol.RequestFacts)
		status int
		code   string
	}{
		{"覆盖多种引用", func(_ *catalog.Data, _ *catalog.ClientKey, f *protocol.RequestFacts) {
			f.ResourceTypes = []string{"openai.file", "openai.response"}
		}, 0, ""},
		{"没有引用仍限制成员", func(d *catalog.Data, _ *catalog.ClientKey, _ *protocol.RequestFacts) {
			p := d.Providers["p"]
			c := p.Credentials["b"]
			c.ResourceScope = "other"
			p.Credentials["b"] = c
			d.Providers["p"] = p
		}, 0, ""},
		{"非限定作用域", func(d *catalog.Data, _ *catalog.ClientKey, _ *protocol.RequestFacts) {
			r := d.Routes["r"]
			r.ResourceScope = "shared"
			d.Routes["r"] = r
		}, 409, "resource_owner_unknown"},
		{"未知作用域", func(d *catalog.Data, _ *catalog.ClientKey, _ *protocol.RequestFacts) {
			r := d.Routes["r"]
			r.ResourceScope = "p/missing"
			d.Routes["r"] = r
		}, 409, "resource_owner_unknown"},
		{"未知提供方", func(d *catalog.Data, _ *catalog.ClientKey, _ *protocol.RequestFacts) {
			r := d.Routes["r"]
			r.ResourceScope = "q/shared"
			d.Routes["r"] = r
		}, 409, "resource_owner_unknown"},
		{"必须覆盖全部类型", func(_ *catalog.Data, _ *catalog.ClientKey, f *protocol.RequestFacts) {
			f.ResourceTypes = []string{"openai.file", "openai.vector_store"}
		}, 409, "resource_scope_conflict"},
		{"获准与合法成员无交集", func(d *catalog.Data, k *catalog.ClientKey, f *protocol.RequestFacts) {
			p := d.Providers["p"]
			c := p.Credentials["a"]
			c.ResourceScope = "other"
			c.AccessGroups = []string{"outside"}
			p.Credentials["a"] = c
			d.Providers["p"] = p
			k.AllowedAccessGroups = []string{"outside"}
			f.Mode = "background"
		}, 503, "no_available_target"},
		{"组错误优先于范围错误", func(d *catalog.Data, k *catalog.ClientKey, _ *protocol.RequestFacts) {
			k.AllowedAccessGroups = nil
			r := d.Routes["r"]
			r.ResourceScope = "p/missing"
			d.Routes["r"] = r
		}, 403, "upstream_forbidden"},
		{"资源筛选后模式判断", func(d *catalog.Data, k *catalog.ClientKey, _ *protocol.RequestFacts) {
			addSecondTarget(d)
			p := d.Providers["p"]
			c := p.Credentials["b"]
			c.ResourceScope = "other"
			p.Credentials["b"] = c
			d.Providers["p"] = p
			first := d.Targets["p/m/e"]
			first.RequestModes = []string{"stream"}
			first.Credentials = []string{"a"}
			d.Targets[first.ID] = first
			second := d.Targets["p/second/e"]
			second.Credentials = []string{"b"}
			d.Targets[second.ID] = second
		}, 400, "unsupported_request_mode"},
		{"资源筛选后能力判断", func(d *catalog.Data, _ *catalog.ClientKey, f *protocol.RequestFacts) {
			addSecondTarget(d)
			p := d.Providers["p"]
			c := p.Credentials["b"]
			c.ResourceScope = "other"
			p.Credentials["b"] = c
			d.Providers["p"] = p
			first := d.Targets["p/m/e"]
			first.Capabilities = nil
			first.Credentials = []string{"a"}
			d.Targets[first.ID] = first
			second := d.Targets["p/second/e"]
			second.Credentials = []string{"b"}
			d.Targets[second.ID] = second
			f.Capabilities = []string{"tools"}
		}, 400, "unsupported_capability"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, key, facts := fixedScopeData(), authorizedKey(), requestFacts()
			tc.change(&data, &key, &facts)
			selection, err := NewRouter(catalog.NewSnapshot(data)).Resolve(key, facts, nil)
			if tc.code != "" {
				if err == nil || err.Code != tc.code || err.Status != tc.status {
					t.Fatalf("错误=%v，预期 %d/%s", err, tc.status, tc.code)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, target := range selection.Targets {
				for _, c := range target.Candidates {
					if c.Provider.ID != "p" || c.Credential.ResourceScope != "shared" {
						t.Fatalf("候选越过固定作用域：%+v", c.Key())
					}
				}
			}
		})
	}
}

func TestResolveOpaqueChecksAuthorizedScopeBeforeOtherFilters(t *testing.T) {
	for _, tc := range []struct {
		name     string
		change   func(*catalog.Data)
		conflict bool
	}{
		{"不同模型", func(*catalog.Data) {}, true},
		{"禁用目标不能消除冲突", func(d *catalog.Data) {
			target := d.Targets["p/second/e"]
			target.Enabled = false
			d.Targets[target.ID] = target
		}, true},
		{"不支持模式不能消除冲突", func(d *catalog.Data) {
			target := d.Targets["p/second/e"]
			target.RequestModes = []string{"stream"}
			d.Targets[target.ID] = target
		}, true},
		{"不支持能力不能消除冲突", func(d *catalog.Data) {
			target := d.Targets["p/second/e"]
			target.Capabilities = nil
			d.Targets[target.ID] = target
		}, true},
		{"禁用凭据不能消除冲突", func(d *catalog.Data) {
			p := d.Providers["p"]
			c := p.Credentials["b"]
			c.Enabled = false
			p.Credentials["b"] = c
			d.Providers["p"] = p
		}, true},
		{"未授权目标不进入冲突集合", func(d *catalog.Data) {
			p := d.Providers["p"]
			c := p.Credentials["b"]
			c.AccessGroups = []string{"private"}
			p.Credentials["b"] = c
			d.Providers["p"] = p
		}, false},
		{"作用域外目标不进入冲突集合", func(d *catalog.Data) {
			p := d.Providers["p"]
			c := p.Credentials["b"]
			c.ResourceScope = "other"
			p.Credentials["b"] = c
			d.Providers["p"] = p
		}, false},
		{"相同有效模型允许", func(d *catalog.Data) {
			target := d.Targets["p/second/e"]
			target.UpstreamName = "real-model"
			d.Targets[target.ID] = target
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := fixedScopeData()
			addSecondTarget(&data)
			first := data.Targets["p/m/e"]
			first.Credentials = []string{"a"}
			data.Targets[first.ID] = first
			second := data.Targets["p/second/e"]
			second.UpstreamName = "other-model"
			second.Credentials = []string{"b"}
			data.Targets[second.ID] = second
			tc.change(&data)
			facts := requestFacts()
			facts.ResourceTypes = []string{"openai.opaque"}
			facts.Capabilities = []string{"tools"}
			_, err := NewRouter(catalog.NewSnapshot(data)).Resolve(authorizedKey(), facts, nil)
			if tc.conflict {
				if err == nil || err.Status != 409 || err.Code != "resource_scope_conflict" {
					t.Fatalf("模型冲突未拒绝：%v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCandidateKeyNormalizesMaterialVersion(t *testing.T) {
	c := Candidate{Provider: catalog.Provider{ID: "p"}, Target: catalog.Target{ID: "t"}, Credential: catalog.Credential{ID: "c"}}
	if got := c.Key(); got != (CandidateKey{"p", "t", "c", "none"}) {
		t.Fatalf("材料身份错误：%+v", got)
	}
	c.Credential.Version = "v2"
	if c.Key().CredentialVersion != "v2" {
		t.Fatal("丢失凭据材料版本")
	}
}

func TestResolveIsStableAndGroupsTargets(t *testing.T) {
	data := routingData()
	addSecondTarget(&data)
	r := NewRouter(catalog.NewSnapshot(data))
	for range 4 {
		selection, err := r.Resolve(authorizedKey(), requestFacts(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(selection.Targets) != 2 || selection.Targets[1].TargetID != "p/second/e" || selection.Targets[1].Priority != 1 || selection.Targets[1].Weight != 3 {
			t.Fatalf("目标分组错误：%+v", selection.Targets)
		}
		for _, target := range selection.Targets {
			if len(target.Candidates) != 2 || target.Candidates[0].Credential.ID != "a" {
				t.Fatalf("静态筛选推进了轮转：%+v", target)
			}
		}
	}
}

func TestPickReappliesAvailabilityBeforePriority(t *testing.T) {
	data := routingData()
	addSecondTarget(&data)
	r := NewRouter(catalog.NewSnapshot(data))
	selection, err := r.Resolve(authorizedKey(), requestFacts(), nil)
	if err != nil {
		t.Fatal(err)
	}
	allowPrimary := true
	available := func(c Candidate) bool {
		return c.Target.ID != "p/m/e" || allowPrimary && c.Credential.ID == "b"
	}
	for _, want := range []string{"p/m/e", "p/second/e", "p/m/e"} {
		allowPrimary = want == "p/m/e"
		c, ok := r.Pick(selection, available)
		if !ok || c.Target.ID != want {
			t.Fatalf("动态优先级错误：%+v，预期 %s", c.Key(), want)
		}
		if want == "p/m/e" && c.Credential.ID != "b" {
			t.Fatalf("选到不可用凭据：%+v", c.Key())
		}
	}
	if _, ok := r.Pick(selection, nil); ok {
		t.Fatal("缺少可用性回调仍选取候选")
	}
	if _, ok := r.Pick(selection, func(Candidate) bool { return false }); ok {
		t.Fatal("全不可用仍选取候选")
	}
	if _, ok := r.Pick(Selection{}, func(Candidate) bool { return true }); ok {
		t.Fatal("空集合仍选取候选")
	}
}

func TestPickTargetWeightsDoNotCountCredentials(t *testing.T) {
	for _, count := range []int{1, 12} {
		data := routingData()
		addSecondTarget(&data)
		r := NewRouter(catalog.NewSnapshot(data))
		selection, err := r.Resolve(authorizedKey(), requestFacts(), nil)
		if err != nil {
			t.Fatal(err)
		}
		selection.Targets[1].Priority = 0
		base := selection.Targets[0].Candidates[0]
		selection.Targets[0].Candidates = make([]Candidate, count)
		for i := range count {
			selection.Targets[0].Candidates[i] = base
			selection.Targets[0].Candidates[i].Credential.ID = "key-" + strconv.Itoa(i)
		}
		draw := 0
		r.random = func() float64 { value := (float64(draw) + 0.5) / 1000; draw++; return value }
		counts := map[string]int{}
		for range 1000 {
			c, ok := r.Pick(selection, func(Candidate) bool { return true })
			if !ok {
				t.Fatal("没有候选")
			}
			counts[c.Target.ID]++
		}
		if counts["p/m/e"] != 250 || counts["p/second/e"] != 750 {
			t.Fatalf("%d 把凭据改变了目标权重：%v", count, counts)
		}
	}
}

func TestPickFiniteWeightsAvoidOverflow(t *testing.T) {
	data := routingData()
	addSecondTarget(&data)
	r := NewRouter(catalog.NewSnapshot(data))
	selection, err := r.Resolve(authorizedKey(), requestFacts(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := range selection.Targets {
		selection.Targets[i].Priority = 0
		selection.Targets[i].Weight = math.MaxFloat64
	}
	for _, tc := range []struct {
		draw   float64
		target string
	}{{0.25, "p/m/e"}, {0.75, "p/second/e"}} {
		r.random = func() float64 { return tc.draw }
		c, ok := r.Pick(selection, func(Candidate) bool { return true })
		if !ok || c.Target.ID != tc.target {
			t.Fatalf("大权重选择错误：%+v", c.Key())
		}
	}
	for _, weight := range []float64{0, -1, math.Inf(1), math.NaN()} {
		for i := range selection.Targets {
			selection.Targets[i].Weight = weight
		}
		if _, ok := r.Pick(selection, func(Candidate) bool { return true }); ok {
			t.Fatal("非法权重仍选取候选")
		}
	}
}

func TestPickRotationSeparatesRoutesAndTargets(t *testing.T) {
	data := routingData()
	addSecondTarget(&data)
	r := NewRouter(catalog.NewSnapshot(data))
	selection, err := r.Resolve(authorizedKey(), requestFacts(), nil)
	if err != nil {
		t.Fatal(err)
	}
	selection.Targets[1].Priority = 0
	r.random = func() float64 { return 0 }
	all := func(Candidate) bool { return true }
	first, _ := r.Pick(selection, all)
	secondRoute := selection
	secondRoute.Route.ID = "second"
	otherRoute, _ := r.Pick(secondRoute, all)
	otherTarget, _ := r.Pick(selection, func(c Candidate) bool { return c.Target.ID == "p/second/e" })
	second, _ := r.Pick(selection, all)
	if first.Credential.ID != "a" || otherRoute.Credential.ID != "a" || otherTarget.Credential.ID != "a" || second.Credential.ID != "b" {
		t.Fatalf("轮转游标互相干扰：%s %s %s %s", first.Credential.ID, otherRoute.Credential.ID, otherTarget.Credential.ID, second.Credential.ID)
	}
	if len(r.next) != 3 {
		t.Fatalf("轮转身份数量错误：%d", len(r.next))
	}
	for key, value := range r.next {
		if value < 0 || value >= 2 {
			t.Fatalf("游标超出凭据集合：%+v=%d", key, value)
		}
	}
}

func TestResolveGroupPermissionConstrainsSupportedModesAndCapabilities(t *testing.T) {
	data := routingData()
	addSecondTarget(&data)
	provider := data.Providers["p"]
	credential := provider.Credentials["b"]
	credential.AccessGroups = []string{"private"}
	provider.Credentials["b"] = credential
	data.Providers["p"] = provider
	first := data.Targets["p/m/e"]
	first.Credentials = []string{"a"}
	first.RequestModes = []string{"stream"}
	first.Capabilities = nil
	data.Targets[first.ID] = first
	second := data.Targets["p/second/e"]
	second.Credentials = []string{"b"}
	data.Targets[second.ID] = second
	router := NewRouter(catalog.NewSnapshot(data))
	facts := requestFacts()
	if _, err := router.Resolve(authorizedKey(), facts, nil); err == nil || err.Code != "unsupported_request_mode" {
		t.Fatalf("未授权目标影响了模式判定：%v", err)
	}
	facts.Mode = "stream"
	facts.Capabilities = []string{"tools"}
	if _, err := router.Resolve(authorizedKey(), facts, nil); err == nil || err.Code != "unsupported_capability" {
		t.Fatalf("未授权目标影响了能力判定：%v", err)
	}
}

func TestResolveAccessGroupsAcrossProvidersAndCredentials(t *testing.T) {
	data := routingData()
	provider := data.Providers["p"]
	provider.ID = "q"
	provider.Credentials = map[string]catalog.Credential{
		"q-key": {ID: "q-key", Enabled: true, Secret: catalog.NewSecret("q-secret"), AccessGroups: []string{"private"}},
	}
	data.Providers["q"] = provider
	target := data.Targets["p/m/e"]
	target.ID, target.ProviderID = "q/m/e", "q"
	target.Credentials = []string{"q-key"}
	data.Targets[target.ID] = target
	route := data.Routes["r"]
	route.Targets = append(route.Targets, catalog.RouteTarget{Target: target.ID, Weight: 1})
	data.Routes["r"] = route
	router := NewRouter(catalog.NewSnapshot(data))
	for _, tc := range []struct {
		groups []string
		want   []string
	}{
		{[]string{"general"}, []string{"p/m/e"}},
		{[]string{"private"}, []string{"q/m/e"}},
		{[]string{"general", "private"}, []string{"p/m/e", "q/m/e"}},
	} {
		key := authorizedKey()
		key.AllowedAccessGroups = tc.groups
		selection, err := router.Resolve(key, requestFacts(), nil)
		if err != nil {
			t.Fatal(err)
		}
		var ids []string
		for _, target := range selection.Targets {
			ids = append(ids, target.TargetID)
		}
		if !slices.Equal(ids, tc.want) {
			t.Fatalf("跨提供方权限错误：%v，预期 %v", ids, tc.want)
		}
	}
}

func TestResolveNoAuthenticationStillRequiresAccessGroup(t *testing.T) {
	data := routingData()
	provider := data.Providers["p"]
	endpoint := provider.Endpoints["e"]
	endpoint.AuthType = "none"
	provider.Endpoints["e"] = endpoint
	data.Providers["p"] = provider
	router := NewRouter(catalog.NewSnapshot(data))
	selection, err := router.Resolve(authorizedKey(), requestFacts(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(selection.Targets[0].Candidates) != 3 {
		t.Fatalf("无认证端点的静态凭据集合错误：%+v", selection.Targets)
	}
	key := authorizedKey()
	key.AllowedAccessGroups = nil
	if _, err := router.Resolve(key, requestFacts(), nil); err == nil || err.Code != "upstream_forbidden" {
		t.Fatalf("无认证端点绕过访问组：%v", err)
	}
}
