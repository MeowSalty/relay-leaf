package runtime

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"relayleaf/internal/catalog"
	"relayleaf/internal/testkit"
)

func managerData() catalog.Data {
	return catalog.Data{
		Limits: catalog.Limits{MaxInflight: 8, MaxRuntimeStateEntries: 100, MaxTotalBodyBufferBytes: 100, MaxTotalResponseBufferBytes: 200},
		ClientKeys: map[string]catalog.ClientKey{
			"client-a": {Enabled: true, Limits: catalog.LocalLimits{MaxInflight: 1}},
			"client-b": {Enabled: true},
		},
		Providers: map[string]catalog.Provider{
			"p": {Enabled: true, Credentials: map[string]catalog.Credential{
				"a": {Enabled: true, Version: "v1", Attributes: map[string]string{"project": "x"}, QuotaScopes: []string{"project"}},
				"b": {Enabled: true, Version: "v2", Attributes: map[string]string{"project": "x"}, QuotaScopes: []string{"project"}},
			}, QuotaScopes: map[string]catalog.QuotaScope{
				"project": {Owner: map[string]string{"project": "x"}, Limits: catalog.LocalLimits{MaxInflight: 4}},
				"model":   {Owner: map[string]string{"project": "x"}, Targets: []string{"p/model/create"}, Limits: catalog.LocalLimits{MaxInflight: 2}},
			}},
		},
		Targets: map[string]catalog.Target{
			"p/model/create": {ProviderID: "p", Enabled: true, Credentials: []string{"a", "b"}, QuotaScopes: []string{"model", "project"}, Limits: catalog.LocalLimits{MaxInflight: 4}},
			"p/other/create": {ProviderID: "p", Enabled: true, Credentials: []string{"a"}, Limits: catalog.LocalLimits{MaxInflight: 4}},
		},
		Routes: map[string]catalog.Route{"r": {Enabled: true, Targets: []catalog.RouteTarget{{Target: "p/model/create"}, {Target: "p/other/create"}}}},
	}
}

func newManager(t *testing.T, data catalog.Data) (*Manager, *testkit.Clock) {
	t.Helper()
	clock := testkit.NewClock(time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC))
	m, err := NewManager(catalog.NewSnapshot(data), clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	return m, clock
}

func choice() Selection {
	return Selection{TargetID: "p/model/create", ProviderID: "p", CredentialID: "a", CredentialVersion: "v1", QuotaScopes: []string{"p/model", "p/project"}}
}

func start(t *testing.T, m *Manager, selection Selection, id string) *AttemptLease {
	t.Helper()
	l, err := m.Start(context.Background(), selection, id)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func wantRejected(t *testing.T, err error, code string) {
	t.Helper()
	var rejection *Rejection
	if !errors.As(err, &rejection) || rejection.Code != code {
		t.Fatalf("期望拒绝 %s，实际错误为 %v", code, err)
	}
}

func TestInboundConcurrencyAndIdempotentRelease(t *testing.T) {
	d := managerData()
	d.Limits.MaxInflight = 2
	m, _ := newManager(t, d)
	gate := testkit.NewGate()
	var wg sync.WaitGroup
	accepted := make(chan *InboundLease, 32)
	for range 32 {
		wg.Go(func() {
			if err := gate.Wait(context.Background()); err != nil {
				t.Error(err)
				return
			}
			l, err := m.Admit(context.Background(), "client-a")
			if err == nil {
				accepted <- l
			} else {
				wantRejected(t, err, "client_concurrency_limited")
			}
		})
	}
	gate.Open()
	wg.Wait()
	close(accepted)
	if len(accepted) != 1 || m.Stats().Inbound != 1 {
		t.Fatalf("并发准入出现超额或漏计：%+v", m.Stats())
	}
	l := <-accepted
	b, err := m.Admit(context.Background(), "client-b")
	if err != nil {
		t.Fatal(err)
	}
	_, err = m.Admit(context.Background(), "client-b")
	wantRejected(t, err, "gateway_capacity_exceeded")
	for range 32 {
		wg.Go(l.Release)
		wg.Go(b.Release)
	}
	wg.Wait()
	if s := m.Stats(); s.Inbound != 0 || s.Clients["client-a"] != 0 || s.Clients["client-b"] != 0 {
		t.Fatalf("重复释放破坏入站计数：%+v", s)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = m.Admit(ctx, "client-a")
	if !errors.Is(err, context.Canceled) || m.Stats().Inbound != 0 {
		t.Fatalf("取消前准入没有完整回滚：%v", err)
	}
}

func TestAttemptAtomicCapacityAndScopeDeduplication(t *testing.T) {
	d := managerData()
	p := d.Providers["p"]
	q := p.QuotaScopes["project"]
	q.Limits.MaxInflight = 1
	p.QuotaScopes["project"] = q
	d.Providers["p"] = p
	m, _ := newManager(t, d)
	selection := choice()
	selection.QuotaScopes = []string{"p/project", "p/model", "p/project"}
	gate := testkit.NewGate()
	var wg sync.WaitGroup
	accepted := make(chan *AttemptLease, 32)
	for i := range 32 {
		wg.Go(func() {
			_ = gate.Wait(context.Background())
			l, err := m.Start(context.Background(), selection, fmt.Sprintf("attempt-%d", i))
			if err == nil {
				accepted <- l
			} else {
				wantRejected(t, err, "no_available_target")
			}
		})
	}
	gate.Open()
	wg.Wait()
	close(accepted)
	s := m.Stats()
	if len(accepted) != 1 || s.Attempts != 1 || s.ActiveAttempts != 1 || s.Quotas["p/project"] != 1 || s.Quotas["p/model"] != 1 || s.Targets[selection.TargetID] != 1 {
		t.Fatalf("多域占用不是全有或全无：%+v", s)
	}
	l := <-accepted
	for range 32 {
		wg.Go(l.Release)
		wg.Go(func() { l.Settle(false) })
	}
	wg.Wait()
	s = m.Stats()
	if s.ActiveAttempts != 0 || s.Targets[selection.TargetID] != 0 || s.Quotas["p/model"] != 0 || s.Quotas["p/project"] != 0 || s.Attempts != 1 {
		t.Fatalf("尝试重复释放或结算改变已发生次数：%+v", s)
	}
}

func TestAttemptRejectsForgedSelectionBeforeCounting(t *testing.T) {
	m, _ := newManager(t, managerData())
	mutations := []func(*Selection){
		func(s *Selection) { s.ProviderID = "foreign" },
		func(s *Selection) { s.CredentialID = "missing" },
		func(s *Selection) { s.CredentialVersion = "old" },
		func(s *Selection) { s.TargetID = "p/missing/create" },
		func(s *Selection) { s.QuotaScopes = []string{"p/project"} },
		func(s *Selection) { s.QuotaScopes = append(s.QuotaScopes, "foreign/project") },
	}
	for i, mutate := range mutations {
		s := choice()
		mutate(&s)
		_, err := m.Start(context.Background(), s, fmt.Sprintf("invalid-%d", i))
		wantRejected(t, err, "no_available_target")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := m.Start(ctx, choice(), "cancelled")
	if !errors.Is(err, context.Canceled) {
		t.Fatal("开始前取消没有保留取消来源")
	}
	if s := m.Stats(); s.Attempts != 0 || s.ActiveAttempts != 0 {
		t.Fatalf("拒绝或取消仍登记了真实尝试：%+v", s)
	}
	l := start(t, m, choice(), "same")
	_, err = m.Start(context.Background(), choice(), "same")
	wantRejected(t, err, "no_available_target")
	l.Release()
}

func TestCooldownRecoveryGenerationAndEvidenceDeduplication(t *testing.T) {
	m, clock := newManager(t, managerData())
	first := start(t, m, choice(), "first")
	old := start(t, m, choice(), "old-normal")
	e := Restriction{Scope: "credential", Kind: "quota_cooldown", ReceivedAt: clock.Now(), Until: clock.Now().Add(10 * time.Second), Reason: "scope_unknown"}
	m.ApplyRestriction(first, e)
	first.Release()
	clock.Advance(5 * time.Second)
	m.ApplyRestriction(first, e)
	old.Settle(true)
	old.Release()
	s := m.Stats()
	if len(s.Restrictions) != 1 || s.Restrictions[0].Generation != 1 || !s.Restrictions[0].Until.Equal(e.Until) {
		t.Fatalf("重复反馈或迟到正常成功改变限制：%+v", s)
	}
	_, err := m.Start(context.Background(), choice(), "cooling")
	wantRejected(t, err, "no_available_target")
	clock.Advance(5 * time.Second)
	probe := start(t, m, choice(), "probe-old")
	_, err = m.Start(context.Background(), choice(), "second-probe")
	wantRejected(t, err, "no_available_target")
	newEvidence := Restriction{Scope: "credential", Kind: "quota_cooldown", ReceivedAt: clock.Now(), Until: clock.Now().Add(10 * time.Second)}
	m.ApplyRestriction(old, newEvidence)
	probe.Settle(true)
	if r := m.Stats().Restrictions; len(r) != 1 || r[0].Generation != 2 || !r[0].Probing {
		t.Fatalf("旧代次恢复许可清除了新限制：%+v", r)
	}
	clock.Advance(10 * time.Second)
	_, err = m.Start(context.Background(), choice(), "while-old-probe-active")
	wantRejected(t, err, "no_available_target")
	probe.Release()
	fresh := start(t, m, choice(), "probe-current")
	fresh.Settle(true)
	fresh.Settle(false)
	fresh.Release()
	if len(m.Stats().Restrictions) != 0 {
		t.Fatal("当前代次的完整成功没有解除暂时限制")
	}
}

func TestQuotaRecoveryIsAtomicAcrossApplicableDomains(t *testing.T) {
	m, clock := newManager(t, managerData())
	first := start(t, m, choice(), "limits")
	for _, qid := range []string{"p/model", "p/project"} {
		m.ApplyRestriction(first, Restriction{Scope: "quota", Kind: "quota_cooldown", QuotaScopeID: qid, Until: clock.Now().Add(time.Second)})
	}
	first.Release()
	clock.Advance(time.Second)
	other := Selection{ProviderID: "p", TargetID: "p/other/create", CredentialID: "a", CredentialVersion: "v1", QuotaScopes: []string{"p/project"}}
	projectProbe := start(t, m, other, "project-probe")
	_, err := m.Start(context.Background(), choice(), "both-probes")
	wantRejected(t, err, "no_available_target")
	for _, r := range m.Stats().Restrictions {
		if r.Key == "quota:p/model" && r.Probing {
			t.Fatal("另一个域拒绝后仍保留了部分恢复许可")
		}
	}
	projectProbe.Settle(true)
	projectProbe.Release()
	modelProbe := start(t, m, choice(), "model-probe")
	modelProbe.Settle(true)
	modelProbe.Release()
	if len(m.Stats().Restrictions) != 0 {
		t.Fatal("适用配额域恢复没有独立解除")
	}
}

func TestPermanentAndPermissionRestrictionsStayWithinBinding(t *testing.T) {
	for _, scopeKind := range []struct{ scope, kind string }{{"credential", "authentication"}, {"credential", "quota_exhausted"}, {"permission", "permission"}, {"target", "capability"}} {
		t.Run(scopeKind.kind, func(t *testing.T) {
			m, clock := newManager(t, managerData())
			first := start(t, m, choice(), "restricted")
			m.ApplyRestriction(first, Restriction{Scope: scopeKind.scope, Kind: scopeKind.kind, Permanent: true})
			first.Settle(true)
			first.Release()
			clock.Advance(24 * time.Hour)
			_, err := m.Start(context.Background(), choice(), "later")
			wantRejected(t, err, "no_available_target")
			b := choice()
			b.CredentialID, b.CredentialVersion = "b", "v2"
			if scopeKind.scope != "target" {
				l := start(t, m, b, "other-material")
				l.Release()
			}
			if scopeKind.scope == "permission" || scopeKind.scope == "target" {
				other := Selection{ProviderID: "p", TargetID: "p/other/create", CredentialID: "a", CredentialVersion: "v1", QuotaScopes: []string{"p/project"}}
				l := start(t, m, other, "other-target")
				l.Release()
			}
		})
	}
}

func TestRestrictionCannotInventScopeOrKind(t *testing.T) {
	m, clock := newManager(t, managerData())
	l := start(t, m, choice(), "source")
	for _, e := range []Restriction{
		{Scope: "quota", Kind: "quota_cooldown", QuotaScopeID: "foreign/project", Permanent: true},
		{Scope: "quota", Kind: "quota_cooldown", QuotaScopeID: "project", Permanent: true},
		{Scope: "credential", Kind: "arbitrary", Permanent: true},
		{Scope: "target", Kind: "authentication", Permanent: true},
		{Scope: "provider", Kind: "service_cooldown", Permanent: true},
		{Scope: "credential", Kind: "quota_cooldown"},
	} {
		m.ApplyRestriction(l, e)
	}
	if len(m.Stats().Restrictions) != 0 {
		t.Fatal("无效反馈创建了限制状态")
	}
	m.ApplyRestriction(l, Restriction{Scope: "target", Kind: "service_cooldown", Until: clock.Now().Add(time.Second)})
	l.Release()
	if len(m.Stats().Restrictions) != 1 {
		t.Fatal("有效服务冷却没有生效")
	}
}

func TestBufferCapacityOverflowAndSnapshotIsolation(t *testing.T) {
	m, _ := newManager(t, managerData())
	if !m.ReserveBuffer(RequestBuffer, 60) || m.ReserveBuffer(RequestBuffer, 41) || m.ReserveBuffer(RequestBuffer, math.MaxInt64) || m.ReserveBuffer(ResponseBuffer, -1) || m.ReserveBuffer(BufferKind(99), 1) {
		t.Fatal("字节容量接受了超限、负值或未知类型")
	}
	if !m.ReserveBuffer(ResponseBuffer, 200) {
		t.Fatal("请求与响应预算被错误共享")
	}
	m.ReleaseBuffer(RequestBuffer, 61)
	m.ReleaseBuffer(RequestBuffer, -1)
	if m.Stats().BodyBytes != 60 {
		t.Fatal("错误释放破坏了字节占用")
	}
	m.ReleaseBuffer(RequestBuffer, 60)
	m.ReleaseBuffer(ResponseBuffer, 200)
	s := m.Stats()
	s.Clients["client-a"] = 999
	s.Targets["p/model/create"] = 999
	if fresh := m.Stats(); fresh.BodyBytes != 0 || fresh.ResponseBytes != 0 || fresh.Clients["client-a"] != 0 || fresh.Targets["p/model/create"] != 0 {
		t.Fatalf("字节未归还或Stats泄露可变状态：%+v", fresh)
	}
}

func TestStartupStateCapacityAndScopeOwnerValidation(t *testing.T) {
	d := managerData()
	m, _ := newManager(t, d)
	if m.Stats().StateEntries != 11 {
		t.Fatalf("启动身份计数未包含全部材料×Target槽：%d", m.Stats().StateEntries)
	}
	d.Limits.MaxRuntimeStateEntries = 10
	if _, err := NewManager(catalog.NewSnapshot(d), nil); err == nil {
		t.Fatal("身份槽超限仍允许启动")
	}
	d = managerData()
	p := d.Providers["p"]
	q := p.QuotaScopes["project"]
	q.Owner = map[string]string{"project": "foreign"}
	p.QuotaScopes["project"] = q
	d.Providers["p"] = p
	if _, err := NewManager(catalog.NewSnapshot(d), nil); err == nil {
		t.Fatal("归属不一致的共享配额仍允许启动")
	}
	if _, err := NewManager(nil, nil); err == nil {
		t.Fatal("空快照仍允许初始化")
	}
}

func TestInactiveAndUnboundCredentialsDoNotCreatePermissionSlots(t *testing.T) {
	d := managerData()
	p := d.Providers["p"]
	p.Credentials["unbound"] = catalog.Credential{Enabled: true, Version: "v3"}
	p.Credentials["disabled"] = catalog.Credential{Enabled: false, Version: "v4"}
	d.Providers["p"] = p
	target := d.Targets["p/model/create"]
	target.Credentials = append(target.Credentials, "disabled", "a")
	d.Targets["p/model/create"] = target
	m, _ := newManager(t, d)
	if m.Stats().StateEntries != 11 {
		t.Fatalf("未绑定、禁用或重复凭据产生额外状态：%d", m.Stats().StateEntries)
	}
}

func TestProviderNamespacesAndQuotaFeedbackStayIsolated(t *testing.T) {
	d := managerData()
	d.Providers["other"] = catalog.Provider{Enabled: true,
		Credentials: map[string]catalog.Credential{"a": {Enabled: true, Version: "v1", Attributes: map[string]string{"project": "x"}, QuotaScopes: []string{"project"}}},
		QuotaScopes: map[string]catalog.QuotaScope{"project": {Owner: map[string]string{"project": "x"}, Limits: catalog.LocalLimits{MaxInflight: 1}}},
	}
	d.Targets["other/model/create"] = catalog.Target{Enabled: true, ProviderID: "other", Credentials: []string{"a"}}
	d.Routes["other-route"] = catalog.Route{Enabled: true, Targets: []catalog.RouteTarget{{Target: "other/model/create"}}}
	m, clock := newManager(t, d)
	l := start(t, m, choice(), "provider-p")
	m.ApplyRestriction(l, Restriction{Scope: "quota", Kind: "quota_cooldown", QuotaScopeID: "p/project", Until: clock.Now().Add(time.Hour)})
	m.ApplyRestriction(l, Restriction{Scope: "quota", Kind: "quota_exhausted", QuotaScopeID: "other/project", Permanent: true})
	l.Release()
	_, err := m.Start(context.Background(), choice(), "limited-p")
	wantRejected(t, err, "no_available_target")
	other := Selection{ProviderID: "other", TargetID: "other/model/create", CredentialID: "a", CredentialVersion: "v1", QuotaScopes: []string{"other/project"}}
	unaffected := start(t, m, other, "provider-other")
	s := m.Stats()
	if s.Quotas["other/project"] != 1 || s.Quotas["p/project"] != 0 || len(s.Restrictions) != 1 {
		t.Fatalf("Provider 同名身份或外域反馈发生串域：%+v", s)
	}
	unaffected.Release()
}

func TestReleasedRecoveryCannotSettleAfterAnotherProbeStarts(t *testing.T) {
	m, clock := newManager(t, managerData())
	first := start(t, m, choice(), "first")
	m.ApplyRestriction(first, Restriction{Scope: "credential", Kind: "quota_cooldown", Until: clock.Now().Add(time.Second)})
	first.Release()
	clock.Advance(time.Second)
	old := start(t, m, choice(), "old")
	old.Release()
	current := start(t, m, choice(), "current")
	old.Settle(true)
	if r := m.Stats().Restrictions; len(r) != 1 || !r[0].Probing {
		t.Fatal("已释放的旧恢复许可清除了新试探中的限制")
	}
	current.Settle(false)
	current.Release()
	if len(m.Stats().Restrictions) != 1 {
		t.Fatal("中性或失败结算错误解除限制")
	}
}

func TestQuotaTargetScopeCannotCrossProviderAtStartup(t *testing.T) {
	d := managerData()
	d.Targets["foreign/model/create"] = catalog.Target{ProviderID: "foreign", Enabled: true}
	p := d.Providers["p"]
	q := p.QuotaScopes["model"]
	q.Targets = append(q.Targets, "foreign/model/create")
	p.QuotaScopes["model"] = q
	d.Providers["p"] = p
	if _, err := NewManager(catalog.NewSnapshot(d), nil); err == nil {
		t.Fatal("共享配额适用集合跨 Provider 仍允许启动")
	}
}

func TestConcurrentFeedbackAppliesOnceAndInboundOutlivesAttempt(t *testing.T) {
	m, clock := newManager(t, managerData())
	inbound, err := m.Admit(context.Background(), "client-a")
	if err != nil {
		t.Fatal(err)
	}
	l := start(t, m, choice(), "attempt")
	e := Restriction{Scope: "credential", Kind: "quota_cooldown", Until: clock.Now().Add(time.Second)}
	gate := testkit.NewGate()
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			_ = gate.Wait(context.Background())
			m.ApplyRestriction(l, e)
		})
	}
	gate.Open()
	wg.Wait()
	l.Release()
	s := m.Stats()
	if len(s.Restrictions) != 1 || s.Restrictions[0].Generation != 1 || s.ActiveAttempts != 0 || s.Inbound != 1 || s.StateEntries != 11 {
		t.Fatalf("重复反馈、上游收尾或限制状态改变入站排空语义：%+v", s)
	}
	_, err = m.Admit(context.Background(), "client-a")
	wantRejected(t, err, "client_concurrency_limited")
	inbound.Release()
	if m.Stats().Inbound != 0 {
		t.Fatal("下游完成后入站名额没有归还")
	}
}

func TestSharedPermanentQuotaCannotBeBypassedByAnotherCredential(t *testing.T) {
	m, clock := newManager(t, managerData())
	l := start(t, m, choice(), "source")
	m.ApplyRestriction(l, Restriction{Scope: "quota", Kind: "quota_exhausted", QuotaScopeID: "p/project", Permanent: true})
	l.Release()
	clock.Advance(365 * 24 * time.Hour)
	b := choice()
	b.CredentialID, b.CredentialVersion = "b", "v2"
	_, err := m.Start(context.Background(), b, "different-key")
	wantRejected(t, err, "no_available_target")
	other := Selection{ProviderID: "p", TargetID: "p/other/create", CredentialID: "a", CredentialVersion: "v1", QuotaScopes: []string{"p/project"}}
	_, err = m.Start(context.Background(), other, "different-target")
	wantRejected(t, err, "no_available_target")
	if s := m.Stats(); s.Attempts != 1 || s.StateEntries != 11 || len(s.Restrictions) != 1 || !s.Restrictions[0].Permanent {
		t.Fatalf("换凭据/模型或时间到期绕过共享永久限制：%+v", s)
	}
}
