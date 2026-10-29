// Package observe 保存有界的近期请求事实，不参与执行许可或上游反馈。
package observe

import (
	"sync"
	"time"

	"relayleaf/internal/protocol"
)

// AttemptRecord 对应一次真实上游尝试，不保存正文、秘密或上游错误消息。
type AttemptRecord struct {
	ID, ProviderID, TargetID, CredentialID, CredentialVersion string
	UpstreamModel, FailureKind, ErrorSource, ProbeReason      string
	StartedAt, HeadersAt, FirstByteAt, FinishedAt             time.Time
	CommittedAt                                               time.Time
	HTTPStatus                                                int
	UpstreamBytes, DownstreamBytes                            int64
	DownstreamCommitted                                       bool
	Response                                                  protocol.ResponseFacts
}

// RequestRecord 将入站请求、单次尝试和该尝试的用量关联到同一身份。
type RequestRecord struct {
	ID, ClientID, RouteID, PublicModel, Mode string
	ErrorCode, ErrorSource                   string
	StartedAt, FinishedAt                    time.Time
	ConfigRevision                           uint64
	HTTPStatus, Attempts                     int
	Attempt                                  *AttemptRecord
}

// Store 只在短临界区复制小型终局事实，不在请求路径执行日志 I/O。
type Store struct {
	mu      sync.Mutex
	items   []RequestRecord
	next    int
	count   int
	evicted uint64
}

func NewStore(capacity int) *Store {
	if capacity < 1 {
		capacity = 1
	}
	return &Store{items: make([]RequestRecord, capacity)}
}

func (s *Store) Append(r RequestRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.count == len(s.items) {
		s.evicted++
	} else {
		s.count++
	}
	s.items[s.next] = clone(r)
	s.next = (s.next + 1) % len(s.items)
}

func (s *Store) Recent() []RequestRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]RequestRecord, 0, s.count)
	for i := 0; i < s.count; i++ {
		index := (s.next - 1 - i + len(s.items)) % len(s.items)
		out = append(out, clone(s.items[index]))
	}
	return out
}

func (s *Store) Evicted() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.evicted
}

func clone(r RequestRecord) RequestRecord {
	if r.Attempt != nil {
		a := *r.Attempt
		u := &a.Response.Usage
		for _, p := range []**int64{&u.Input, &u.Output, &u.Total, &u.CachedInput, &u.ReasoningOutput} {
			if *p != nil {
				value := **p
				*p = &value
			}
		}
		r.Attempt = &a
	}
	return r
}
