// Package routing 处理数据入口、调用身份和配置驱动的单目标候选。
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

type Selection struct {
	Route      catalog.Route
	Candidates []Candidate
}
