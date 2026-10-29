// Package routing 处理数据入口、调用身份和配置驱动的上游候选。
package routing

import "relayleaf/internal/catalog"

// Candidate 是已经过静态授权筛选的一种上游调用组合。
// QuotaScopes 保存 Provider 限定的完整作用域 ID。
type Candidate struct {
	Provider    catalog.Provider
	Target      catalog.Target
	Endpoint    catalog.Endpoint
	Credential  catalog.Credential
	QuotaScopes []string
}

// CandidateKey 标识一次上游调用组合及其凭据材料版本。
type CandidateKey struct {
	ProviderID, TargetID, CredentialID, CredentialVersion string
}

func (c Candidate) Key() CandidateKey {
	version := c.Credential.Version
	if version == "" {
		version = "none"
	}
	return CandidateKey{c.Provider.ID, c.Target.ID, c.Credential.ID, version}
}

// TargetCandidates 将静态获准凭据保留在所属目标内。
type TargetCandidates struct {
	TargetID   string
	Priority   int
	Weight     float64
	Candidates []Candidate
}

type Selection struct {
	Route   catalog.Route
	Targets []TargetCandidates
}
