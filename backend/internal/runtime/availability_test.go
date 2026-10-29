package runtime

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"
)

func TestAvailabilityIsReadOnlyAndRejectsInvalidIdentity(t *testing.T) {
	m, _ := newManager(t, managerData())
	a := choice()
	b := a
	b.CredentialID, b.CredentialVersion = "b", "v2"
	other := a
	other.TargetID, other.QuotaScopes = "p/other/create", []string{"p/project"}
	unknown := a
	unknown.CredentialID = "unknown"
	wrongScopes := a
	wrongScopes.QuotaScopes = []string{"p/project"}
	wrongVersion := a
	wrongVersion.CredentialVersion = "new-version"
	a.QuotaScopes = []string{"p/project", "p/model", "p/project"}
	beforeScopes := slices.Clone(a.QuotaScopes)
	before := m.Stats()
	got := m.Available([]Selection{a, b, other, unknown, wrongScopes, wrongVersion})
	if !slices.Equal(got, []bool{true, true, true, false, false, false}) {
		t.Fatalf("候选可用性不符：%v", got)
	}
	if !reflect.DeepEqual(before, m.Stats()) || !slices.Equal(a.QuotaScopes, beforeScopes) {
		t.Fatal("可用性查询修改了运行状态或调用方的配额列表")
	}
	if len(m.Available(nil)) != 0 {
		t.Fatal("空候选产生了可用性结果")
	}
}

func TestAvailabilityDoesNotReserveExpiredRecovery(t *testing.T) {
	m, clock := newManager(t, managerData())
	a := choice()
	b := a
	b.CredentialID, b.CredentialVersion = "b", "v2"
	lease := start(t, m, a, "failed")
	m.ApplyRestriction(lease, Restriction{Kind: "quota_cooldown", Scope: "quota", QuotaScopeID: "p/project", ReceivedAt: clock.Now(), Until: clock.Now().Add(time.Minute)})
	lease.Settle(false)
	lease.Release()
	if got := m.Available([]Selection{a, b}); !slices.Equal(got, []bool{false, false}) {
		t.Fatalf("共享冷却未排除所有成员：%v", got)
	}
	clock.Advance(time.Minute)
	before := m.Stats()
	for range 3 {
		if got := m.Available([]Selection{a, b}); !slices.Equal(got, []bool{true, true}) {
			t.Fatalf("到期恢复未提供候选预览：%v", got)
		}
	}
	if !reflect.DeepEqual(before, m.Stats()) || len(before.Restrictions) != 1 || before.Restrictions[0].Probing {
		t.Fatal("预览提前领取恢复许可或解除限制")
	}
	recovery := start(t, m, a, "recovery")
	if got := m.Available([]Selection{a, b}); !slices.Equal(got, []bool{false, false}) {
		t.Fatalf("已有恢复尝试时仍显示可调用成员：%v", got)
	}
	if _, err := m.Start(context.Background(), b, "competing-recovery"); err == nil {
		t.Fatal("旧的可用性预览绕过了共享恢复占用")
	} else {
		wantRejected(t, err, "no_available_target")
	}
	recovery.Settle(false)
	recovery.Release()
	if m.Stats().Attempts != 2 || !m.Available([]Selection{b})[0] {
		t.Fatal("失败恢复释放后没有重新开放有限恢复许可")
	}
}

func TestAvailabilityCannotCommitStaleConcurrentPreviews(t *testing.T) {
	d := managerData()
	target := d.Targets["p/model/create"]
	target.Limits.MaxInflight = 1
	d.Targets["p/model/create"] = target
	m, _ := newManager(t, d)
	const workers = 16
	ready := make(chan struct{}, workers)
	commit := make(chan struct{})
	accepted := make(chan *AttemptLease, workers)
	var workersDone sync.WaitGroup
	for i := range workers {
		workersDone.Go(func() {
			if !m.Available([]Selection{choice()})[0] {
				t.Error("提交前没有其它占用，但候选预览不可用")
			}
			ready <- struct{}{}
			<-commit
			lease, err := m.Start(context.Background(), choice(), fmt.Sprintf("competing-%d", i))
			if err == nil {
				accepted <- lease
			} else {
				wantRejected(t, err, "no_available_target")
			}
		})
	}
	for range workers {
		<-ready
	}
	close(commit)
	workersDone.Wait()
	close(accepted)
	if stats := m.Stats(); len(accepted) != 1 || stats.Attempts != 1 || stats.ActiveAttempts != 1 || stats.Quotas["p/model"] != 1 || stats.Quotas["p/project"] != 1 {
		t.Fatalf("过时预览重复提交或留下部分许可：%+v", stats)
	}
	for lease := range accepted {
		lease.Release()
	}
	if stats := m.Stats(); stats.ActiveAttempts != 0 || stats.Targets["p/model/create"] != 0 || stats.Quotas["p/project"] != 0 {
		t.Fatalf("竞争后的许可未回收：%+v", stats)
	}
}

func TestAvailabilityKeepsCredentialAndTargetRestrictionsSeparate(t *testing.T) {
	for _, tc := range []struct {
		name, scope, kind string
		want              []bool
	}{
		{"凭据", "credential", "authentication", []bool{false, true, false}},
		{"目标", "target", "capability", []bool{false, false, true}},
		{"凭据目标", "permission", "permission", []bool{false, true, true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, clock := newManager(t, managerData())
			a := choice()
			b := a
			b.CredentialID, b.CredentialVersion = "b", "v2"
			other := a
			other.TargetID, other.QuotaScopes = "p/other/create", []string{"p/project"}
			lease := start(t, m, a, "restricted")
			m.ApplyRestriction(lease, Restriction{Scope: tc.scope, Kind: tc.kind, Permanent: true, ReceivedAt: clock.Now()})
			lease.Release()
			clock.Advance(24 * time.Hour)
			before := m.Stats()
			if got := m.Available([]Selection{a, b, other}); !slices.Equal(got, tc.want) {
				t.Fatalf("限制范围不符：得到 %v，期望 %v", got, tc.want)
			}
			if !reflect.DeepEqual(before, m.Stats()) {
				t.Fatal("预览改变了永久限制")
			}
		})
	}
}
