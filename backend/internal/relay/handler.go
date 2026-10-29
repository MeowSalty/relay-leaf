package relay

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptrace"
	"sync"
	"time"

	"relayleaf/internal/catalog"
	"relayleaf/internal/observe"
	"relayleaf/internal/protocol"
	"relayleaf/internal/routing"
	runstate "relayleaf/internal/runtime"
)

// HandlerOptions 只替换可控时钟和传输装配；业务策略始终来自快照。
type HandlerOptions struct {
	BaseTransport *http.Transport
	Now           func() time.Time
	NewTimer      func(time.Duration) Timer
}

// Handler 固定接纳时的配置并拥有请求、尝试、计时器及下游提交。
type Handler struct {
	snapshot  *catalog.Snapshot
	router    *routing.Router
	state     *runstate.Manager
	transport *Transport
	records   *observe.Store
	now       func() time.Time
	newTimer  func(time.Duration) Timer
	ctx       context.Context
	cancel    context.CancelCauseFunc
	mu        sync.Mutex
	closing   bool
	active    sync.WaitGroup
	closeOnce sync.Once
}

func NewHandler(snapshot *catalog.Snapshot, opts HandlerOptions) (*Handler, error) {
	if snapshot == nil || !snapshot.Summary().SecretsResolved {
		return nil, errors.New("数据入口要求已解析秘密的完整配置快照")
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.NewTimer == nil {
		opts.NewTimer = func(d time.Duration) Timer { return wallTimer{time.NewTimer(d)} }
	}
	state, err := runstate.NewManager(snapshot, opts.Now)
	if err != nil {
		return nil, err
	}
	base := opts.BaseTransport
	if base == nil {
		base = &http.Transport{DialContext: (&net.Dialer{KeepAlive: 30 * time.Second}).DialContext, ForceAttemptHTTP2: true, IdleConnTimeout: 90 * time.Second, ExpectContinueTimeout: time.Second}
	} else {
		base = base.Clone()
	}
	// 每次尝试的快照策略负责阶段截止，不让共享连接池的固定默认值提前截断。
	base.TLSHandshakeTimeout = 0
	base.ResponseHeaderTimeout = 0
	transport, err := NewTransport(Options{MaxConnections: snapshot.Limits().MaxInflight, BaseTransport: base})
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	return &Handler{snapshot: snapshot, router: routing.NewRouter(snapshot), state: state, transport: transport, records: observe.NewStore(snapshot.Observability().RecentRequestCapacity), now: opts.Now, newTimer: opts.NewTimer, ctx: ctx, cancel: cancel}, nil
}

func (h *Handler) StopAccepting() {
	h.mu.Lock()
	h.closing = true
	h.mu.Unlock()
}

func (h *Handler) Close() error {
	h.closeOnce.Do(func() {
		h.StopAccepting()
		h.cancel(errShutdown)
		_ = h.transport.Close()
		h.active.Wait()
	})
	return nil
}

func (h *Handler) Stats() runstate.Stats           { return h.state.Stats() }
func (h *Handler) TransportStats() Stats           { return h.transport.Stats() }
func (h *Handler) Recent() []observe.RequestRecord { return h.records.Recent() }

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	record := observe.RequestRecord{ID: rand.Text(), StartedAt: h.now(), ConfigRevision: 1}
	ctx, cancel := context.WithCancelCause(r.Context())
	stop := context.AfterFunc(h.ctx, func() { cancel(errShutdown) })
	defer func() { stop(); cancel(nil) }()
	r = r.WithContext(ctx)
	w.Header().Set("X-RelayLeaf-Request-Id", record.ID)
	defer func() {
		record.FinishedAt = h.now()
		h.records.Append(record)
	}()
	h.mu.Lock()
	if h.closing {
		h.mu.Unlock()
		h.reject(ctx, w, r, &record, gatewayError(http.StatusServiceUnavailable, "no_available_target"))
		return
	}
	h.active.Add(1)
	h.mu.Unlock()
	defer h.active.Done()
	if e := routing.CheckEndpoint(r); e != nil {
		h.reject(ctx, w, r, &record, e)
		return
	}
	client, e := routing.Authenticate(r, h.snapshot, h.now())
	if e != nil {
		h.reject(ctx, w, r, &record, e)
		return
	}
	record.ClientID = client.ID
	inbound, err := h.state.Admit(ctx, client.ID)
	if err != nil {
		h.reject(ctx, w, r, &record, admissionError(err))
		return
	}
	defer inbound.Release()
	encoding, e := routing.CheckFormat(r)
	if e != nil {
		h.reject(ctx, w, r, &record, e)
		return
	}
	limits := h.snapshot.Limits()
	if r.ContentLength > limits.MaxRequestBodyBytes {
		h.reject(ctx, w, r, &record, gatewayError(413, "request_body_too_large"))
		return
	}
	raw := &ownedBuffer{limit: limits.MaxRequestBodyBytes, budget: bufferBudget{h.state, runstate.RequestBuffer}}
	defer raw.close()
	decoded := raw
	if err := h.readInput(ctx, r, w, raw); err != nil {
		e = h.inputError(ctx, err)
		if e != nil {
			h.reject(ctx, w, r, &record, e)
		} else {
			record.ErrorCode, record.ErrorSource = "client_cancelled", "caller"
		}
		return
	}
	if encoding == "gzip" {
		decoded = &ownedBuffer{limit: limits.MaxDecompressedBodyBytes, budget: bufferBudget{h.state, runstate.RequestBuffer}}
		defer decoded.close()
		gz, err := gzip.NewReader(bytes.NewReader(raw.data))
		if err == nil {
			err = decoded.read(ctx, gz)
			closeErr := gz.Close()
			if err == nil {
				err = closeErr
			}
		}
		if err != nil {
			e = h.inputError(ctx, err)
			if e != nil && e.Code == "invalid_request_metadata" {
				e = gatewayError(415, "unsupported_request_format")
			}
			if e != nil {
				h.reject(ctx, w, r, &record, e)
			}
			return
		}
	} else if int64(len(decoded.data)) > limits.MaxDecompressedBodyBytes {
		h.reject(ctx, w, r, &record, gatewayError(413, "request_body_too_large"))
		return
	}
	metadataBytes := int64(h.snapshot.Observability().MaxObservedStateBytes)
	if !h.state.ReserveBuffer(runstate.RequestBuffer, metadataBytes) {
		h.reject(ctx, w, r, &record, gatewayError(503, "gateway_capacity_exceeded"))
		return
	}
	defer h.state.ReleaseBuffer(runstate.RequestBuffer, metadataBytes)
	facts, e := protocol.ScanRequest(decoded.data, int(metadataBytes))
	if e != nil {
		h.reject(ctx, w, r, &record, e)
		return
	}
	record.PublicModel, record.Mode = facts.Model, facts.Mode
	selection, e := h.router.Resolve(client, facts, r)
	if e != nil {
		h.reject(ctx, w, r, &record, e)
		return
	}
	record.RouteID = selection.Route.ID
	// 复制缓冲在出站前取得；探测前缀和语义观察另按实际持有量计费。
	chunkSize := min(int64(32<<10), max(int64(1), limits.MaxTotalResponseBufferBytes/int64(limits.MaxInflight)))
	if !h.state.ReserveBuffer(runstate.ResponseBuffer, chunkSize) {
		h.reject(ctx, w, r, &record, gatewayError(503, "gateway_capacity_exceeded"))
		return
	}
	defer h.state.ReleaseBuffer(runstate.ResponseBuffer, chunkSize)
	chunk := make([]byte, int(chunkSize))
	for _, candidate := range selection.Candidates {
		if ctx.Err() != nil {
			h.finishUnstarted(ctx, w, r, &record)
			return
		}
		mappedBytes := int64(0)
		if facts.Model != candidate.Target.UpstreamName {
			name, _ := json.Marshal(candidate.Target.UpstreamName)
			mappedBytes = int64(len(decoded.data) - (facts.ModelSpan.End - facts.ModelSpan.Start) + len(name))
			if !h.state.ReserveBuffer(runstate.RequestBuffer, mappedBytes) {
				h.reject(ctx, w, r, &record, gatewayError(503, "gateway_capacity_exceeded"))
				return
			}
		}
		out, buildErr := routing.BuildRequest(ctx, r, candidate, facts, raw.data, decoded.data)
		if buildErr != nil {
			h.state.ReleaseBuffer(runstate.RequestBuffer, mappedBytes)
			h.reject(ctx, w, r, &record, buildErr)
			return
		}
		started, committedFailure := h.execute(ctx, w, out, candidate, facts, chunk, &record)
		h.state.ReleaseBuffer(runstate.RequestBuffer, mappedBytes)
		if !started {
			_ = out.Body.Close()
			continue
		}
		if committedFailure {
			// 已提交的协议不能再附加网关 JSON，交由 HTTP 服务器终止该流。
			panic(http.ErrAbortHandler)
		}
		return
	}
	h.finishUnstarted(ctx, w, r, &record)
}

func (h *Handler) execute(ctx context.Context, w http.ResponseWriter, out *http.Request, candidate routing.Candidate, request protocol.RequestFacts, chunk []byte, record *observe.RequestRecord) (bool, bool) {
	exec := candidate.Credential.Execution
	attemptCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	timing := newAttemptTimers(ctx, exec, h.now, h.newTimer, cancel)
	defer timing.close()
	attemptCtx = context.WithValue(attemptCtx, attemptTimersKey{}, timing)
	attemptCtx = httptrace.WithClientTrace(attemptCtx, timing.trace())
	out = out.WithContext(attemptCtx)
	id := rand.Text()
	var lease *runstate.AttemptLease
	a, err := h.transport.Start(attemptCtx, out, func() error {
		var err error
		lease, err = h.state.Start(attemptCtx, runstate.Selection{TargetID: candidate.Target.ID, ProviderID: candidate.Provider.ID, CredentialID: candidate.Credential.ID, CredentialVersion: candidate.Credential.Version, QuotaScopes: candidate.QuotaScopes}, id)
		if err != nil {
			return err
		}
		record.Attempts = 1
		record.Attempt = &observe.AttemptRecord{ID: id, ProviderID: candidate.Provider.ID, TargetID: candidate.Target.ID, CredentialID: candidate.Credential.ID, CredentialVersion: candidate.Credential.Version, UpstreamModel: candidate.Target.UpstreamName, StartedAt: h.now()}
		if request.Mode == "stream" {
			timing.arm("stream_first_event_timeout", exec.StreamFirstEventTimeout)
			if exec.StreamFirstContentEnabled {
				timing.arm("stream_first_content_timeout", exec.StreamFirstContentTimeout)
			}
		} else {
			timing.arm("non_stream_response_timeout", exec.NonStreamResponseTimeout)
		}
		return nil
	})
	if lease == nil {
		return false, false
	}
	defer lease.Release()
	defer func() { record.Attempt.FirstByteAt = timing.byteTime() }()
	if err != nil {
		lease.Settle(false)
		record.Attempt.FinishedAt = h.now()
		h.finishFailure(attemptCtx, w, record, err, false)
		return true, false
	}
	defer a.Close()
	timing.gotHeaders()
	record.Attempt.HeadersAt = h.now()
	record.Attempt.HTTPStatus = a.Response.StatusCode
	h.applyHeaderRestriction(lease, candidate, a.Response, record.Attempt.HeadersAt)
	return true, h.relayResponse(attemptCtx, w, a, lease, timing, candidate, request, chunk, record)
}

func (h *Handler) readInput(ctx context.Context, r *http.Request, w http.ResponseWriter, body *ownedBuffer) error {
	readCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	timing := newAttemptTimers(ctx, h.snapshot.Execution(), h.now, h.newTimer, cancel)
	defer timing.close()
	rc := http.NewResponseController(w)
	if err := rc.SetReadDeadline(time.Time{}); err != nil {
		return errors.New("数据入口的 ResponseWriter 不支持读取截止，包装器必须提供 Unwrap")
	}
	callbackDone := make(chan struct{})
	stop := context.AfterFunc(readCtx, func() {
		_ = rc.SetReadDeadline(time.Now())
		_ = r.Body.Close()
		close(callbackDone)
	})
	defer func() {
		if !stop() {
			<-callbackDone
		}
		_ = rc.SetReadDeadline(time.Time{})
		_ = r.Body.Close()
	}()
	timing.arm("request_body_timeout", h.snapshot.Execution().RequestBodyReadTimeout)
	err := body.read(readCtx, r.Body)
	timing.stop("request_body_timeout")
	if cause := context.Cause(readCtx); cause != nil {
		return cause
	}
	return err
}

func (h *Handler) inputError(ctx context.Context, err error) *protocol.GatewayError {
	var failure *executionFailure
	if errors.As(err, &failure) && failure.kind == "request_body_timeout" {
		if due, ok := ctx.Deadline(); ok && !failure.deadline.IsZero() && !due.After(failure.deadline) {
			return gatewayError(504, "request_deadline_exceeded")
		}
		// 读取截止也可能关闭 HTTP 连接的请求 context，不能因此覆盖已经确认的本地原因。
		return gatewayError(408, "request_body_timeout")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return gatewayError(504, "request_deadline_exceeded")
	}
	if ctx.Err() != nil {
		if errors.Is(context.Cause(ctx), context.DeadlineExceeded) {
			return gatewayError(504, "request_deadline_exceeded")
		}
		return nil
	}
	if errors.Is(err, errBodyTooLarge) {
		return gatewayError(413, "request_body_too_large")
	}
	if errors.Is(err, errBufferFull) {
		return gatewayError(503, "gateway_capacity_exceeded")
	}
	return gatewayError(400, "invalid_request_metadata")
}

func (h *Handler) finishUnstarted(ctx context.Context, w http.ResponseWriter, r *http.Request, record *observe.RequestRecord) {
	if ctx.Err() != nil && !errors.Is(context.Cause(ctx), context.DeadlineExceeded) {
		record.ErrorCode, record.ErrorSource = "client_cancelled", "caller"
		return
	}
	code := gatewayError(503, "no_available_target")
	if errors.Is(context.Cause(ctx), context.DeadlineExceeded) {
		code = gatewayError(504, "request_deadline_exceeded")
	}
	h.reject(ctx, w, r, record, code)
}

func admissionError(err error) *protocol.GatewayError {
	var rejection *runstate.Rejection
	if errors.As(err, &rejection) {
		return gatewayError(rejection.Status, rejection.Code)
	}
	return gatewayError(503, "gateway_capacity_exceeded")
}

// 编译期确认入口仅需要标准 HTTP Handler 契约。
var _ http.Handler = (*Handler)(nil)
