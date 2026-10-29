package catalog

import (
	"testing"
	"time"
)

func TestAccessGroupSnapshotIsolation(t *testing.T) {
	d := Data{
		AccessGroups:  map[string]AccessGroup{"general": {ID: "general", Name: "普通"}, "private": {ID: "private"}},
		Providers:     map[string]Provider{"provider": {Credentials: map[string]Credential{"credential": {AccessGroups: []string{"general"}}}}},
		ClientKeys:    map[string]ClientKey{"client": {ID: "client", Enabled: true, AllowedRoutes: []string{"route"}, AllowedAccessGroups: []string{"general"}, DeniedAccessGroups: []string{"private"}}},
		Verifications: []Verification{{ClientID: "client", Material: NewSecret("test-material")}},
	}
	s := NewSnapshot(d)
	delete(d.AccessGroups, "general")
	d.Providers["provider"].Credentials["credential"].AccessGroups[0] = "changed"
	d.ClientKeys["client"].AllowedAccessGroups[0] = "changed"
	d.ClientKeys["client"].DeniedAccessGroups[0] = "changed"
	assertUnchanged := func() {
		t.Helper()
		if s.AccessGroups()["general"].Name != "普通" || s.Providers()["provider"].Credentials["credential"].AccessGroups[0] != "general" {
			t.Fatal("访问组定义或凭据分组被外部修改")
		}
		k := s.ClientKeys()["client"]
		if k.AllowedAccessGroups[0] != "general" || k.DeniedAccessGroups[0] != "private" {
			t.Fatal("客户端组授权被外部修改")
		}
		k, ok := s.Authenticate("test-material", time.Now())
		if !ok || k.AllowedAccessGroups[0] != "general" || k.DeniedAccessGroups[0] != "private" {
			t.Fatal("鉴权返回的组授权被外部修改")
		}
	}
	assertUnchanged()
	groups := s.AccessGroups()
	groups["general"] = AccessGroup{ID: "general", Name: "已修改"}
	delete(groups, "private")
	s.Providers()["provider"].Credentials["credential"].AccessGroups[0] = "changed"
	s.ClientKeys()["client"].AllowedAccessGroups[0] = "changed"
	s.ClientKeys()["client"].DeniedAccessGroups[0] = "changed"
	k, _ := s.Authenticate("test-material", time.Now())
	k.AllowedAccessGroups[0] = "changed"
	k.DeniedAccessGroups[0] = "changed"
	assertUnchanged()
}
