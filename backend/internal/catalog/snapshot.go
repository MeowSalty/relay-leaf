package catalog

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"slices"
	"time"
)

// Secret 在所有常规格式化和序列化输出中隐藏密钥内容。
type Secret struct{ value string }

func NewSecret(value string) Secret           { return Secret{value: value} }
func (s Secret) Reveal() string               { return s.value }
func (s Secret) String() string               { return "[redacted]" }
func (s Secret) GoString() string             { return "[redacted]" }
func (s Secret) Format(f fmt.State, _ rune)   { _, _ = io.WriteString(f, "[redacted]") }
func (s Secret) MarshalJSON() ([]byte, error) { return json.Marshal("[redacted]") }

type Summary struct {
	Providers, Targets, Routes, ClientKeys int
	SecretsResolved                        bool
}
type LocalLimits struct{ MaxInflight int }
type Limits struct {
	MaxInflight, QueueCapacity, MaxRuntimeStateEntries                                                  int
	MaxRequestBodyBytes, MaxDecompressedBodyBytes, MaxTotalBodyBufferBytes, MaxTotalResponseBufferBytes int64
	QueueWaitTimeout                                                                                    time.Duration
	MaxSessionBindings, MaxSessionBindingsPerClient, MaxSessionIDBytes                                  int
}
type Execution struct {
	RetryRules                                                                                      []RetryRule
	MaxRetries, MaxRetriesPerProvider                                                               int
	RetryInterval, RetryMaxInterval, RetryWaitTimeout                                               time.Duration
	ConnectTimeout, TLSHandshakeTimeout, ResponseHeaderTimeout                                      time.Duration
	StreamFirstEventTimeout, StreamFirstContentTimeout, NonStreamResponseTimeout, StreamIdleTimeout time.Duration
	StreamFirstContentEnabled                                                                       bool
	DownstreamWriteIdleTimeout, RequestBodyReadTimeout                                              time.Duration
	EmptyResponseEnabled                                                                            bool
	EmptyResponseMaxBufferBytes                                                                     int64
	EmptyResponseMaxWait                                                                            time.Duration
	ConditionalRetryEnabled                                                                         bool
	MaxErrorBodyBytes                                                                               int64
	MaxErrorBodyWait                                                                                time.Duration
	Sources                                                                                         map[string]string
}
type Health struct {
	NoAvailableTargetAction                                           string
	ProbeInterval                                                     time.Duration
	Rules                                                             []HealthRule
	ExemptUntil                                                       time.Time
	CircuitBreakerEnabled                                             bool
	QuotaCooldown                                                     time.Duration
	ConsecutiveFailureThreshold, FailureRateMinSamples, MaxSamples    int
	UnknownConcurrency, HalfOpenConcurrency, HalfOpenSuccessThreshold int
	FailureRateThreshold                                              float64
	FailureWindow, Cooldown, MaxCooldown, HealthyTTL                  time.Duration
}
type Match struct {
	Protocols, ErrorCodes, ErrorTypes, MessageContains, MessageEquals, FailureKinds []string
	StatusCodes                                                                     []int
	RetryAfter                                                                      string
}
type RetryRule struct {
	ID, Action string
	Match      Match
}
type HealthRule struct {
	ID, Action, Trigger string
	Match               Match
	Count, MinSamples   int
	Ratio               float64
	Window, Duration    time.Duration
	Until               time.Time
}
type ResourceScope struct {
	ID, Name, Sharing  string
	Owner              map[string]string
	ResourceTypes      []string
	CheckedAt          time.Time
	Evidence           string
	CredentialVersions map[string]string
}
type QuotaScope struct {
	ID, Name string
	Owner    map[string]string
	Targets  []string
	Limits   LocalLimits
}
type Observability struct{ RecentRequestCapacity, MaxObservedJSONBytes, MaxObservedSSEEventBytes, MaxObservedStateBytes, LogQueueCapacity int }
type Endpoint struct {
	ID, Protocol, Operation, PathTemplate, AuthType, AuthName string
	RequestModes                                              []string
	Headers, Query                                            map[string]string
}
type Credential struct {
	ID, Name, Version, ResourceScope string
	Enabled                          bool
	Secret                           Secret
	Attributes, Selectors            map[string]string
	QuotaScopes                      []string
	Execution                        Execution
}
type Target struct {
	ID, ProviderID, ModelID, EndpointID, UpstreamName    string
	Enabled                                              bool
	Credentials, RequestModes, Capabilities, QuotaScopes []string
	Limits                                               LocalLimits
}
type Provider struct {
	ID, Name, Origin string
	Enabled          bool
	Execution        Execution
	Health           Health
	Endpoints        map[string]Endpoint
	Credentials      map[string]Credential
	ResourceScopes   map[string]ResourceScope
	QuotaScopes      map[string]QuotaScope
}
type RouteTarget struct {
	Target   string
	Priority int
	Weight   float64
}
type Route struct {
	ID, Name, Protocol, Operation, PublicModel, ResourcePolicy, ResourceScope string
	Enabled                                                                   bool
	Targets                                                                   []RouteTarget
}
type ClientKey struct {
	ID, Name                 string
	Version, RetiringVersion string
	RetiringValidUntil       time.Time
	Enabled                  bool
	ExpiresAt                time.Time
	AllowedRoutes            []string
	Limits                   LocalLimits
}
type Verification struct {
	ClientID   string
	Material   Secret
	ValidUntil time.Time
}

type Data struct {
	Gateway         string
	Providers       map[string]Provider
	Targets         map[string]Target
	Routes          map[string]Route
	ClientKeys      map[string]ClientKey
	Verifications   []Verification
	Limits          Limits
	Execution       Execution
	Health          Health
	Observability   Observability
	SecretsResolved bool
}
type verification struct {
	digest [32]byte
	id     string
	until  time.Time
}

// Snapshot 持有完整的候选配置，访问方法返回独立的值。
type Snapshot struct {
	data          Data
	verifications []verification
}

func (s Snapshot) String() string             { return fmt.Sprintf("catalog.Snapshot%+v", s.Summary()) }
func (s Snapshot) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, s.String()) }

func NewSnapshot(d Data) *Snapshot {
	s := &Snapshot{data: cloneData(d)}
	for _, v := range d.Verifications {
		s.verifications = append(s.verifications, verification{sha256.Sum256([]byte(v.Material.Reveal())), v.ClientID, v.ValidUntil})
	}
	s.data.Verifications = nil
	return s
}
func (s *Snapshot) GatewayAddress() string { return s.data.Gateway }
func (s *Snapshot) Summary() Summary {
	return Summary{len(s.data.Providers), len(s.data.Targets), len(s.data.Routes), len(s.data.ClientKeys), s.data.SecretsResolved}
}
func (s *Snapshot) Providers() map[string]Provider   { return cloneData(s.data).Providers }
func (s *Snapshot) Targets() map[string]Target       { return cloneData(s.data).Targets }
func (s *Snapshot) Routes() map[string]Route         { return cloneData(s.data).Routes }
func (s *Snapshot) ClientKeys() map[string]ClientKey { return cloneData(s.data).ClientKeys }
func (s *Snapshot) Limits() Limits                   { return s.data.Limits }
func (s *Snapshot) Execution() Execution             { return cloneExecution(s.data.Execution) }
func (s *Snapshot) Health() Health                   { return cloneHealth(s.data.Health) }
func (s *Snapshot) Observability() Observability     { return s.data.Observability }

// Authenticate 校验摘要是否相等，不暴露摘要或密钥内容。
func (s *Snapshot) Authenticate(token string, now time.Time) (ClientKey, bool) {
	digest := sha256.Sum256([]byte(token))
	id := ""
	for _, v := range s.verifications {
		equal := subtle.ConstantTimeCompare(digest[:], v.digest[:])
		if equal == 1 && (v.until.IsZero() || now.Before(v.until)) {
			id = v.id
		}
	}
	k, ok := s.data.ClientKeys[id]
	if !ok || !k.Enabled || (!k.ExpiresAt.IsZero() && !now.Before(k.ExpiresAt)) {
		return ClientKey{}, false
	}
	k.AllowedRoutes = slices.Clone(k.AllowedRoutes)
	return k, true
}
func cloneMatch(m Match) Match {
	m.Protocols = slices.Clone(m.Protocols)
	m.ErrorCodes = slices.Clone(m.ErrorCodes)
	m.ErrorTypes = slices.Clone(m.ErrorTypes)
	m.MessageContains = slices.Clone(m.MessageContains)
	m.MessageEquals = slices.Clone(m.MessageEquals)
	m.FailureKinds = slices.Clone(m.FailureKinds)
	m.StatusCodes = slices.Clone(m.StatusCodes)
	return m
}
func cloneExecution(e Execution) Execution {
	e.Sources = maps.Clone(e.Sources)
	e.RetryRules = slices.Clone(e.RetryRules)
	for i := range e.RetryRules {
		e.RetryRules[i].Match = cloneMatch(e.RetryRules[i].Match)
	}
	return e
}
func cloneHealth(h Health) Health {
	h.Rules = slices.Clone(h.Rules)
	for i := range h.Rules {
		h.Rules[i].Match = cloneMatch(h.Rules[i].Match)
	}
	return h
}
func cloneData(d Data) Data {
	d.Providers = maps.Clone(d.Providers)
	d.Targets = maps.Clone(d.Targets)
	d.Routes = maps.Clone(d.Routes)
	d.ClientKeys = maps.Clone(d.ClientKeys)
	d.Execution = cloneExecution(d.Execution)
	d.Health = cloneHealth(d.Health)
	for id, p := range d.Providers {
		p.Execution = cloneExecution(p.Execution)
		p.Health = cloneHealth(p.Health)
		p.Endpoints = maps.Clone(p.Endpoints)
		p.Credentials = maps.Clone(p.Credentials)
		p.ResourceScopes = maps.Clone(p.ResourceScopes)
		p.QuotaScopes = maps.Clone(p.QuotaScopes)
		for k, scope := range p.ResourceScopes {
			scope.Owner = maps.Clone(scope.Owner)
			scope.ResourceTypes = slices.Clone(scope.ResourceTypes)
			scope.CredentialVersions = maps.Clone(scope.CredentialVersions)
			p.ResourceScopes[k] = scope
		}
		for k, scope := range p.QuotaScopes {
			scope.Owner = maps.Clone(scope.Owner)
			scope.Targets = slices.Clone(scope.Targets)
			p.QuotaScopes[k] = scope
		}
		for k, e := range p.Endpoints {
			e.Headers = maps.Clone(e.Headers)
			e.Query = maps.Clone(e.Query)
			e.RequestModes = slices.Clone(e.RequestModes)
			p.Endpoints[k] = e
		}
		for k, c := range p.Credentials {
			c.Attributes = maps.Clone(c.Attributes)
			c.Selectors = maps.Clone(c.Selectors)
			c.QuotaScopes = slices.Clone(c.QuotaScopes)
			c.Execution = cloneExecution(c.Execution)
			p.Credentials[k] = c
		}
		d.Providers[id] = p
	}
	for id, t := range d.Targets {
		t.Credentials = slices.Clone(t.Credentials)
		t.RequestModes = slices.Clone(t.RequestModes)
		t.Capabilities = slices.Clone(t.Capabilities)
		t.QuotaScopes = slices.Clone(t.QuotaScopes)
		d.Targets[id] = t
	}
	for id, r := range d.Routes {
		r.Targets = slices.Clone(r.Targets)
		d.Routes[id] = r
	}
	for id, k := range d.ClientKeys {
		k.AllowedRoutes = slices.Clone(k.AllowedRoutes)
		d.ClientKeys[id] = k
	}
	d.Verifications = slices.Clone(d.Verifications)
	return d
}
