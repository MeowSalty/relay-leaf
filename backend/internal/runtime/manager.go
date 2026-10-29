package runtime

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"
	"unicode/utf8"

	"relayleaf/internal/catalog"
)

type counter struct {
	limit, used int
}

type ownerKey struct {
	scope, provider, credential, version, target, quota string
}

func (k ownerKey) String() string {
	switch k.scope {
	case "credential":
		return "credential:" + k.provider + "/" + k.credential
	case "target":
		return "target:" + k.target
	case "quota":
		return "quota:" + k.quota
	default:
		return "permission:" + k.provider + "/" + k.credential + "@" + k.version + ":" + k.target
	}
}

type restriction struct {
	kind, reason string
	until        time.Time
	permanent    bool
	active       bool
	generation   uint64
	probe        *AttemptLease
}

type domain struct {
	key   ownerKey
	slots map[string]*restriction
}

type candidate struct {
	selection Selection
	scopes    []string
	domains   []*domain
}

type candidateKey struct{ target, provider, credential, version string }

// Manager 持有启动配置中的稳定身份、有限限制槽和实际占用。
// 所有准入和反馈共用一个短临界区，区内不执行 I/O 或持久日志。
type Manager struct {
	mu         sync.Mutex
	now        func() time.Time
	limits     catalog.Limits
	clients    map[string]*counter
	targets    map[string]*counter
	quotas     map[string]*counter
	candidates map[candidateKey]candidate
	domains    map[ownerKey]*domain
	inbound    int
	active     int
	attempts   uint64
	stateCount int
	activeIDs  map[string]*AttemptLease
	bodyBytes  int64
	respBytes  int64
}

// NewManager 在启动前验证并分配实际使用身份的全部限制槽。
// StateEntries 按身份计数：客户端、Target、QuotaScope、Credential 和材料×Target。
// 每个身份拥有固定的有限限制类型，不以反馈中的任意字符串新建状态。
func NewManager(snapshot *catalog.Snapshot, now func() time.Time) (*Manager, error) {
	if snapshot == nil {
		return nil, errors.New("运行状态：配置快照不能为空")
	}
	if now == nil {
		now = time.Now
	}
	limits := snapshot.Limits()
	if limits.MaxInflight <= 0 || limits.MaxRuntimeStateEntries <= 0 || limits.MaxTotalBodyBufferBytes < 0 || limits.MaxTotalResponseBufferBytes < 0 {
		return nil, errors.New("运行状态：实例并发、状态容量或缓冲容量无效")
	}
	m := &Manager{now: now, limits: limits, clients: make(map[string]*counter), targets: make(map[string]*counter), quotas: make(map[string]*counter), candidates: make(map[candidateKey]candidate), domains: make(map[ownerKey]*domain), activeIDs: make(map[string]*AttemptLease)}
	addState := func() error {
		// 逐项检查避免组合数量的乘法或累计整数溢出。
		if m.stateCount >= limits.MaxRuntimeStateEntries {
			return errors.New("运行状态：配置中的实际身份和限制槽超出 max_runtime_state_entries")
		}
		m.stateCount++
		return nil
	}
	addDomain := func(key ownerKey, kinds ...string) (*domain, error) {
		if d := m.domains[key]; d != nil {
			return d, nil
		}
		if err := addState(); err != nil {
			return nil, err
		}
		d := &domain{key: key, slots: make(map[string]*restriction, len(kinds))}
		for _, kind := range kinds {
			d.slots[kind] = &restriction{kind: kind}
		}
		m.domains[key] = d
		return d, nil
	}
	for id, client := range snapshot.ClientKeys() {
		if !client.Enabled {
			continue
		}
		if client.Limits.MaxInflight < 0 {
			return nil, fmt.Errorf("运行状态：ClientKey %q 的并发限制无效", id)
		}
		if err := addState(); err != nil {
			return nil, err
		}
		m.clients[id] = &counter{limit: client.Limits.MaxInflight}
	}
	providers, targets := snapshot.Providers(), snapshot.Targets()
	usedTargets := make(map[string]bool)
	for _, route := range snapshot.Routes() {
		if !route.Enabled {
			continue
		}
		for _, rt := range route.Targets {
			usedTargets[rt.Target] = true
		}
	}
	for tid := range usedTargets {
		target, ok := targets[tid]
		if !ok {
			return nil, fmt.Errorf("运行状态：路由引用未知 Target %q", tid)
		}
		provider, ok := providers[target.ProviderID]
		if !ok {
			return nil, fmt.Errorf("运行状态：Target %q 引用未知 Provider", tid)
		}
		if !target.Enabled || !provider.Enabled {
			continue
		}
		if target.Limits.MaxInflight < 0 {
			return nil, fmt.Errorf("运行状态：Target %q 的并发限制无效", tid)
		}
		targetDomain, err := addDomain(ownerKey{scope: "target", target: tid}, "service_cooldown", "capability")
		if err != nil {
			return nil, err
		}
		m.targets[tid] = &counter{limit: target.Limits.MaxInflight}
		seenCredentials := make(map[string]bool)
		for _, cid := range target.Credentials {
			if seenCredentials[cid] {
				continue
			}
			seenCredentials[cid] = true
			cred, ok := provider.Credentials[cid]
			if !ok {
				return nil, fmt.Errorf("运行状态：Target %q 引用未知 Credential %q", tid, cid)
			}
			if !cred.Enabled {
				continue
			}
			version := cred.Version
			if version == "" {
				version = "none"
			}
			credentialDomain, err := addDomain(ownerKey{scope: "credential", provider: target.ProviderID, credential: cid}, "quota_cooldown", "quota_exhausted", "authentication")
			if err != nil {
				return nil, err
			}
			permissionDomain, err := addDomain(ownerKey{scope: "permission", provider: target.ProviderID, credential: cid, version: version, target: tid}, "permission")
			if err != nil {
				return nil, err
			}
			c := candidate{selection: Selection{TargetID: tid, ProviderID: target.ProviderID, CredentialID: cid, CredentialVersion: version}, domains: []*domain{targetDomain, credentialDomain, permissionDomain}}
			declared := append(slices.Clone(cred.QuotaScopes), target.QuotaScopes...)
			seenScopes := make(map[string]bool)
			for _, sid := range declared {
				scope, ok := provider.QuotaScopes[sid]
				if !ok {
					return nil, fmt.Errorf("运行状态：调用引用未知 QuotaScope %q", sid)
				}
				for _, scopeTargetID := range scope.Targets {
					scopeTarget, exists := targets[scopeTargetID]
					if !exists || scopeTarget.ProviderID != target.ProviderID {
						return nil, fmt.Errorf("运行状态：QuotaScope %q 引用未知或跨 Provider 的 Target", sid)
					}
				}
				applies := scope.Targets == nil || slices.Contains(scope.Targets, tid)
				if !applies {
					if slices.Contains(target.QuotaScopes, sid) {
						return nil, fmt.Errorf("运行状态：Binding 引用的 QuotaScope %q 不适用于 Target %q", sid, tid)
					}
					continue
				}
				for attr, owner := range scope.Owner {
					if cred.Attributes[attr] != owner {
						return nil, fmt.Errorf("运行状态：QuotaScope %q 与 Credential %q 的归属不一致", sid, cid)
					}
				}
				qid := target.ProviderID + "/" + sid
				if seenScopes[qid] {
					continue
				}
				seenScopes[qid] = true
				if scope.Limits.MaxInflight < 0 {
					return nil, fmt.Errorf("运行状态：QuotaScope %q 的并发限制无效", qid)
				}
				quotaDomain, err := addDomain(ownerKey{scope: "quota", provider: target.ProviderID, quota: qid}, "quota_cooldown", "quota_exhausted")
				if err != nil {
					return nil, err
				}
				if m.quotas[qid] == nil {
					m.quotas[qid] = &counter{limit: scope.Limits.MaxInflight}
				}
				c.scopes = append(c.scopes, qid)
				c.domains = append(c.domains, quotaDomain)
			}
			slices.Sort(c.scopes)
			m.candidates[candidateKey{tid, target.ProviderID, cid, version}] = c
		}
	}
	return m, nil
}

// Admit 在同一临界区取得实例与 ClientKey 名额，拒绝时没有部分占用。
func (m *Manager) Admit(ctx context.Context, clientID string) (*InboundLease, error) {
	if ctx == nil {
		return nil, errors.New("运行状态：准入 context 不能为空")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c := m.clients[clientID]
	if c == nil {
		return nil, &Rejection{Status: 401, Code: "invalid_client_key"}
	}
	if c.limit > 0 && c.used >= c.limit {
		return nil, &Rejection{Status: 429, Code: "client_concurrency_limited"}
	}
	if m.inbound >= m.limits.MaxInflight {
		return nil, &Rejection{Status: 503, Code: "gateway_capacity_exceeded"}
	}
	c.used++
	m.inbound++
	return &InboundLease{manager: m, client: c}, nil
}

// InboundLease 的释放表示下游交付与归属请求的清理均已完成。
type InboundLease struct {
	manager  *Manager
	client   *counter
	released bool
}

func (l *InboundLease) Release() {
	if l == nil || l.manager == nil {
		return
	}
	m := l.manager
	m.mu.Lock()
	defer m.mu.Unlock()
	if l.released {
		return
	}
	l.released = true
	l.client.used--
	m.inbound--
}

type recovery struct {
	slot       *restriction
	generation uint64
}

type applied struct {
	until     time.Time
	permanent bool
}

// AttemptLease 只代表真实开始的尝试，I/O 收尾后才能释放。
type AttemptLease struct {
	manager   *Manager
	id        string
	candidate candidate
	recovery  []recovery
	applied   map[*restriction]applied
	released  bool
	settled   bool
}

// Start 由传输提交回调调用。全部占用与恢复许可成功后才登记真实尝试。
func (m *Manager) Start(ctx context.Context, selection Selection, attemptID string) (*AttemptLease, error) {
	if ctx == nil {
		return nil, errors.New("运行状态：尝试 context 不能为空")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	version := selection.CredentialVersion
	if version == "" {
		version = "none"
	}
	c, ok := m.candidates[candidateKey{selection.TargetID, selection.ProviderID, selection.CredentialID, version}]
	if !ok || attemptID == "" {
		return nil, &Rejection{Status: 503, Code: "no_available_target"}
	}
	if m.active >= m.limits.MaxInflight || m.activeIDs[attemptID] != nil {
		return nil, &Rejection{Status: 503, Code: "no_available_target"}
	}
	scopes := slices.Clone(selection.QuotaScopes)
	slices.Sort(scopes)
	scopes = slices.Compact(scopes)
	if !slices.Equal(scopes, c.scopes) {
		return nil, &Rejection{Status: 503, Code: "no_available_target"}
	}
	target := m.targets[selection.TargetID]
	if target.limit > 0 && target.used >= target.limit {
		return nil, &Rejection{Status: 503, Code: "no_available_target"}
	}
	for _, qid := range c.scopes {
		q := m.quotas[qid]
		if q.limit > 0 && q.used >= q.limit {
			return nil, &Rejection{Status: 503, Code: "no_available_target"}
		}
	}
	var recoveries []recovery
	now := m.now()
	for _, d := range c.domains {
		for _, slot := range d.slots {
			if !slot.active {
				continue
			}
			if slot.permanent || now.Before(slot.until) || slot.probe != nil {
				return nil, &Rejection{Status: 503, Code: "no_available_target"}
			}
			recoveries = append(recoveries, recovery{slot: slot, generation: slot.generation})
		}
	}
	if m.attempts == ^uint64(0) {
		return nil, &Rejection{Status: 503, Code: "gateway_capacity_exceeded"}
	}
	l := &AttemptLease{manager: m, id: attemptID, candidate: c, recovery: recoveries, applied: make(map[*restriction]applied)}
	for _, r := range recoveries {
		r.slot.probe = l
	}
	target.used++
	for _, qid := range c.scopes {
		m.quotas[qid].used++
	}
	m.attempts++
	m.active++
	m.activeIDs[attemptID] = l
	return l, nil
}

// ApplyRestriction 只接受当前尝试实际使用域中的已登记限制类型。
// Until 已在收到证据时计算；重复结算不能从当前时间重新延长期限。
func (m *Manager) ApplyRestriction(l *AttemptLease, evidence Restriction) {
	if l == nil || l.manager != m {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var slot *restriction
	for _, d := range l.candidate.domains {
		if d.key.scope != evidence.Scope || evidence.Scope == "quota" && d.key.quota != evidence.QuotaScopeID {
			continue
		}
		slot = d.slots[evidence.Kind]
		break
	}
	if slot == nil || !evidence.Permanent && evidence.Until.IsZero() {
		return
	}
	old, repeated := l.applied[slot]
	if repeated && (old.permanent || !evidence.Permanent && !evidence.Until.After(old.until)) {
		return
	}
	l.applied[slot] = applied{until: evidence.Until, permanent: evidence.Permanent}
	if !evidence.Permanent && !evidence.Until.After(m.now()) && !slot.active {
		return
	}
	if slot.generation == ^uint64(0) {
		// 代次耗尽时保守保持限制，不能回绕后让旧许可解除新状态。
		slot.permanent = true
		slot.active = true
		return
	}
	slot.generation++
	slot.active = true
	slot.permanent = slot.permanent || evidence.Permanent
	if evidence.Until.After(slot.until) {
		slot.until = evidence.Until
	}
	reason := evidence.Reason
	if len(reason) > 256 {
		reason = reason[:256]
		for !utf8.ValidString(reason) && len(reason) > 0 {
			reason = reason[:len(reason)-1]
		}
	}
	// 复制有限说明，避免一个短切片仍引用调用方的大块正文内存。
	slot.reason = string(slices.Clone([]byte(reason)))
	// 保留已在途的恢复占用，新代次不能在其收尾前增发另一个恢复许可。
}

// Settle 只解除本次获准恢复且代次仍匹配的非永久限制。
// success 的完整 EOF、协议合法结果与尾部错误条件由执行所有者判断。
func (l *AttemptLease) Settle(success bool) {
	if l == nil || l.manager == nil {
		return
	}
	m := l.manager
	m.mu.Lock()
	defer m.mu.Unlock()
	if l.settled || l.released {
		return
	}
	l.settled = true
	if !success {
		return
	}
	for _, r := range l.recovery {
		if r.slot.generation == r.generation && !r.slot.permanent {
			r.slot.active = false
		}
	}
}

func (l *AttemptLease) Release() {
	if l == nil || l.manager == nil {
		return
	}
	m := l.manager
	m.mu.Lock()
	defer m.mu.Unlock()
	if l.released {
		return
	}
	l.released = true
	m.targets[l.candidate.selection.TargetID].used--
	for _, qid := range l.candidate.scopes {
		m.quotas[qid].used--
	}
	for _, r := range l.recovery {
		if r.slot.probe == l {
			r.slot.probe = nil
		}
	}
	m.active--
	delete(m.activeIDs, l.id)
}

// ReserveBuffer 在固定字节预算内原子预留；负值和未知类型不能取得容量。
func (m *Manager) ReserveBuffer(kind BufferKind, bytes int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	used, limit := m.buffer(kind)
	if used == nil || bytes < 0 || bytes > limit-*used {
		return false
	}
	*used += bytes
	return true
}

func (m *Manager) ReleaseBuffer(kind BufferKind, bytes int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	used, _ := m.buffer(kind)
	if used == nil || bytes < 0 || bytes > *used {
		return
	}
	*used -= bytes
}

func (m *Manager) buffer(kind BufferKind) (*int64, int64) {
	switch kind {
	case RequestBuffer:
		return &m.bodyBytes, m.limits.MaxTotalBodyBufferBytes
	case ResponseBuffer:
		return &m.respBytes, m.limits.MaxTotalResponseBufferBytes
	default:
		return nil, 0
	}
}

// Stats 返回与内部状态隔离的占用和限制快照。
func (m *Manager) Stats() Stats {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := Stats{Inbound: m.inbound, ActiveAttempts: m.active, StateEntries: m.stateCount, Attempts: m.attempts, BodyBytes: m.bodyBytes, ResponseBytes: m.respBytes, Clients: make(map[string]int, len(m.clients)), Targets: make(map[string]int, len(m.targets)), Quotas: make(map[string]int, len(m.quotas))}
	for id, c := range m.clients {
		s.Clients[id] = c.used
	}
	for id, c := range m.targets {
		s.Targets[id] = c.used
	}
	for id, c := range m.quotas {
		s.Quotas[id] = c.used
	}
	for _, d := range m.domains {
		for _, r := range d.slots {
			if r.active {
				s.Restrictions = append(s.Restrictions, RestrictionState{Key: d.key.String(), Kind: r.kind, Reason: r.reason, Until: r.until, Permanent: r.permanent, Probing: r.probe != nil, Generation: r.generation})
			}
		}
	}
	slices.SortFunc(s.Restrictions, func(a, b RestrictionState) int {
		if a.Key < b.Key || a.Key == b.Key && a.Kind < b.Kind {
			return -1
		}
		if a.Key == b.Key && a.Kind == b.Kind {
			return 0
		}
		return 1
	})
	return s
}
