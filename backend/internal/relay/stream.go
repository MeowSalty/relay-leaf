package relay

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"relayleaf/internal/observe"
	"relayleaf/internal/protocol"
	"relayleaf/internal/routing"
	runstate "relayleaf/internal/runtime"
)

type responseDelivery struct {
	ctx         context.Context
	w           http.ResponseWriter
	rc          *http.ResponseController
	now         func() time.Time
	idle        time.Duration
	mu          sync.Mutex
	cancelled   bool
	stop        func() bool
	callback    chan struct{}
	closed      bool
	committed   bool
	committedAt time.Time
	bytes       int64
	announced   map[string]bool
}

func newDelivery(ctx context.Context, w http.ResponseWriter, idle time.Duration, now func() time.Time) (*responseDelivery, error) {
	d := &responseDelivery{ctx: ctx, w: w, rc: http.NewResponseController(w), now: now, idle: idle, callback: make(chan struct{})}
	if err := d.rc.SetWriteDeadline(time.Time{}); err != nil {
		return nil, err
	}
	d.stop = context.AfterFunc(ctx, func() {
		d.mu.Lock()
		d.cancelled = true
		_ = d.rc.SetWriteDeadline(time.Now())
		d.mu.Unlock()
		close(d.callback)
	})
	return d, nil
}

func (d *responseDelivery) close() {
	if d.closed {
		return
	}
	d.closed = true
	if !d.stop() {
		<-d.callback
	}
	_ = d.rc.SetWriteDeadline(time.Time{})
}

func (d *responseDelivery) deadline(active bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.cancelled || d.ctx.Err() != nil {
		return d.ctx.Err()
	}
	due := time.Time{}
	if active && d.idle > 0 {
		due = time.Now().Add(d.idle)
	}
	return d.rc.SetWriteDeadline(due)
}

func cleanResponseHeaders(resp *http.Response) http.Header {
	header := resp.Header.Clone()
	for _, line := range header.Values("Connection") {
		for _, key := range strings.Split(line, ",") {
			header.Del(strings.TrimSpace(key))
		}
	}
	for key := range header {
		if strings.HasPrefix(strings.ToLower(key), "x-relayleaf-") {
			header.Del(key)
		}
	}
	for _, key := range []string{"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "TE", "Trailer", "Transfer-Encoding", "Upgrade"} {
		header.Del(key)
	}
	if len(resp.Trailer) > 0 || resp.ProtoMajor == 2 {
		header.Del("Content-Length")
	}
	return header
}

func allowedTrailer(key string) bool {
	key = strings.ToLower(key)
	if strings.HasPrefix(key, "x-relayleaf-") {
		return false
	}
	switch key {
	case "connection", "proxy-connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer", "transfer-encoding", "upgrade", "content-length", "host":
		return false
	}
	return true
}

func (d *responseDelivery) commit(resp *http.Response) error {
	if d.committed {
		return nil
	}
	if err := d.deadline(true); err != nil {
		return err
	}
	for key, values := range cleanResponseHeaders(resp) {
		d.w.Header()[key] = append([]string(nil), values...)
	}
	d.announced = make(map[string]bool)
	for key := range resp.Trailer {
		if allowedTrailer(key) {
			d.w.Header().Add("Trailer", key)
			d.announced[http.CanonicalHeaderKey(key)] = true
		}
	}
	d.committed, d.committedAt = true, d.now()
	d.w.WriteHeader(resp.StatusCode)
	if err := d.rc.Flush(); err != nil {
		return downstreamError(err)
	}
	return d.deadline(false)
}

func downstreamError(err error) error {
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return &executionFailure{kind: "downstream_write_timeout"}
	}
	return &executionFailure{kind: "downstream_write_error"}
}

func (d *responseDelivery) write(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	if err := d.deadline(true); err != nil {
		return err
	}
	n, err := d.w.Write(data)
	d.bytes += int64(n)
	if err != nil {
		return downstreamError(err)
	}
	if n != len(data) {
		return downstreamError(io.ErrShortWrite)
	}
	if err := d.deadline(true); err != nil {
		return err
	}
	if err := d.rc.Flush(); err != nil {
		return downstreamError(err)
	}
	return d.deadline(false)
}

func (d *responseDelivery) trailers(resp *http.Response) {
	for key, values := range resp.Trailer {
		if !allowedTrailer(key) {
			continue
		}
		if !d.announced[http.CanonicalHeaderKey(key)] {
			key = http.TrailerPrefix + key
		}
		d.w.Header()[key] = append([]string(nil), values...)
	}
}

type readResult struct {
	n   int
	err error
}

func legalTerminal(facts protocol.ResponseFacts) bool {
	if facts.Terminal != "observed" || facts.TerminalCoverage != "complete" || facts.Error.Present {
		return false
	}
	switch facts.Result {
	case "completed", "tool_handoff", "refusal", "filtered", "limited", "paused", "continuation_required":
		return true
	}
	return false
}

func terminalExcludesEmpty(facts protocol.ResponseFacts) bool {
	return legalTerminal(facts) && facts.OutputCoverage == "complete" && facts.Result != "completed"
}

func (h *Handler) relayResponse(ctx context.Context, w http.ResponseWriter, attempt *Attempt, lease *runstate.AttemptLease, timing *attemptTimers, candidate routing.Candidate, request protocol.RequestFacts, chunk []byte, record *observe.RequestRecord) bool {
	resp := attempt.Response
	exec := candidate.Credential.Execution
	delivery, err := newDelivery(ctx, w, exec.DownstreamWriteIdleTimeout, h.now)
	if err != nil {
		_ = attempt.Close()
		lease.Settle(false)
		lease.Release()
		h.finishFailure(ctx, w, record, err, false)
		return false
	}
	defer delivery.close()
	observability := h.snapshot.Observability()
	observer := protocol.NewResponsesObserver(protocol.ObserverOptions{Mode: request.Mode, ContentType: resp.Header.Get("Content-Type"), ContentEncoding: resp.Header.Get("Content-Encoding"), StatusCode: resp.StatusCode, Replay: request.Replay, MaxJSONBytes: observability.MaxObservedJSONBytes, MaxEventBytes: observability.MaxObservedSSEEventBytes, MaxStateBytes: observability.MaxObservedStateBytes, MaxErrorBytes: int(exec.MaxErrorBodyBytes), Budget: bufferBudget{h.state, runstate.ResponseBuffer}})
	defer observer.Close()
	defer func() {
		record.Attempt.Response = observer.Facts()
		record.Attempt.DownstreamBytes = delivery.bytes
		record.Attempt.DownstreamCommitted = delivery.committed
		record.Attempt.CommittedAt = delivery.committedAt
	}()
	prefix := &ownedBuffer{limit: exec.EmptyResponseMaxBufferBytes, budget: bufferBudget{h.state, runstate.ResponseBuffer}}
	defer prefix.close()
	probe := exec.EmptyResponseEnabled && request.Replay == protocol.ReplayAllowed && resp.StatusCode >= 200 && resp.StatusCode < 300
	var probeTimer, errorTimer Timer
	var probeC, errorC <-chan time.Time
	probeDeadline := record.Attempt.HeadersAt.Add(exec.EmptyResponseMaxWait)
	errorDeadline := record.Attempt.HeadersAt.Add(exec.MaxErrorBodyWait)
	if probe {
		probeTimer = h.newTimer(max(time.Duration(0), probeDeadline.Sub(h.now())))
		probeC = probeTimer.C()
		defer probeTimer.Stop()
	}
	if resp.StatusCode >= 400 {
		timing.stop("stream_first_event_timeout")
		timing.stop("stream_first_content_timeout")
		errorTimer = h.newTimer(max(time.Duration(0), errorDeadline.Sub(h.now())))
		errorC = errorTimer.C()
		defer errorTimer.Stop()
	}
	initial := observer.Facts()
	if initial.Coverage == "unavailable" {
		timing.stop("stream_first_event_timeout")
		timing.stop("stream_first_content_timeout")
		if resp.ContentLength != 0 {
			probe = false
			record.Attempt.ProbeReason = initial.Reason
		}
	}
	if !exec.EmptyResponseEnabled {
		record.Attempt.ProbeReason = "disabled"
	} else if request.Replay != protocol.ReplayAllowed {
		record.Attempt.ProbeReason = string(request.Replay)
	}
	stopProbe := func(reason string) error {
		probe, probeC = false, nil
		if probeTimer != nil {
			probeTimer.Stop()
		}
		if reason != "" {
			record.Attempt.ProbeReason = reason
		}
		if err := delivery.commit(resp); err != nil {
			return err
		}
		err := delivery.write(prefix.data)
		prefix.close()
		if err == nil {
			record.HTTPStatus = resp.StatusCode
		}
		return err
	}
	if !probe {
		if err := stopProbe(""); err != nil {
			_ = attempt.Close()
			lease.Settle(false)
			lease.Release()
			delivery.close()
			h.finishFailure(ctx, w, record, err, delivery.committed)
			return delivery.committed
		}
	}
	// 只有消费完当前块才授权下一次读取；不会为绕开慢下游而无限预读。
	readCtx, cancelRead := context.WithCancel(ctx)
	requests := make(chan struct{})
	results := make(chan readResult, 1)
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			select {
			case <-readCtx.Done():
				return
			case <-requests:
			}
			n, err := resp.Body.Read(chunk)
			select {
			case results <- readResult{n, err}:
			case <-readCtx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	defer func() {
		cancelRead()
		_ = attempt.Close()
		<-readerDone
	}()
	upstreamEnded := false
	var observedErrorAt time.Time
	finishUpstream := func(readErr error) {
		if upstreamEnded {
			return
		}
		upstreamEnded = true
		observer.End(readErr)
		_ = attempt.Close()
		<-attempt.Done()
		timing.stop("non_stream_response_timeout")
		timing.stop("stream_first_event_timeout")
		timing.stop("stream_first_content_timeout")
		timing.stop("stream_idle_timeout")
		facts := observer.Facts()
		if facts.Error.Complete && observedErrorAt.IsZero() {
			observedErrorAt = h.now()
		}
		h.applyObservedRestriction(lease, candidate, resp, facts, observedErrorAt, record.Attempt.HeadersAt)
		success := readErr == nil && facts.ProtocolComplete && !facts.Error.Present && (facts.Output == "nonempty" || facts.Output == "not_applicable")
		lease.Settle(success)
		lease.Release()
		record.Attempt.FinishedAt = h.now()
	}
	fail := func(err error) bool {
		finishUpstream(err)
		delivery.close()
		h.finishFailure(ctx, w, record, err, delivery.committed)
		return delivery.committed
	}
	pending := false
	for {
		if ctx.Err() != nil {
			return fail(context.Cause(ctx))
		}
		if !pending {
			facts := observer.Facts()
			if request.Mode == "stream" && (!facts.FirstEvent.IsZero() || facts.Coverage == "unavailable" || resp.StatusCode >= 400) {
				timing.arm("stream_idle_timeout", exec.StreamIdleTimeout)
			}
			select {
			case requests <- struct{}{}:
				pending = true
			case <-ctx.Done():
				return fail(context.Cause(ctx))
			}
		}
		select {
		case <-ctx.Done():
			return fail(context.Cause(ctx))
		case <-probeC:
			if err := stopProbe("empty_probe_timeout"); err != nil {
				return fail(err)
			}
		case <-errorC:
			errorC = nil
			observer.StopObserving("error_observation_timeout")
		case result := <-results:
			pending = false
			timing.stop("stream_idle_timeout")
			if errorC != nil && !h.now().Before(errorDeadline) {
				errorC = nil
				observer.StopObserving("error_observation_timeout")
			}
			if result.n > 0 {
				record.Attempt.UpstreamBytes += int64(result.n)
				observer.Feed(chunk[:result.n], h.now())
			}
			facts := observer.Facts()
			if !facts.FirstEvent.IsZero() {
				timing.stop("stream_first_event_timeout")
			}
			if !facts.FirstContent.IsZero() || facts.Output == "not_applicable" || legalTerminal(facts) || facts.Error.Present || facts.OutputCoverage == "unavailable" || facts.OutputCoverage == "partial" {
				timing.stop("stream_first_content_timeout")
			}
			if facts.Error.Complete && observedErrorAt.IsZero() {
				observedErrorAt = h.now()
			}
			h.applyObservedRestriction(lease, candidate, resp, facts, observedErrorAt, record.Attempt.HeadersAt)
			if result.err == io.EOF {
				finishUpstream(nil)
				facts = observer.Facts()
			}
			if result.err != nil && result.err != io.EOF {
				// 已读到的最后字节仍按已提交协议交付；错误结果绝不变成成功。
				if delivery.committed && result.n > 0 {
					if err := delivery.write(chunk[:result.n]); err != nil {
						return fail(err)
					}
				}
				return fail(result.err)
			}
			// 即使本次 Read 同时带来 EOF，也先检查探测预算，再决定是否允许丢弃响应。
			if probe {
				reason := ""
				if !h.now().Before(probeDeadline) {
					reason = "empty_probe_timeout"
				} else if int64(len(prefix.data))+int64(result.n) > exec.EmptyResponseMaxBufferBytes {
					reason = "empty_probe_buffer_limit"
				}
				if reason != "" {
					if err := stopProbe(reason); err != nil {
						return fail(err)
					}
				}
			}
			if probe && facts.Replay == protocol.ReplayAllowed && facts.Output == "empty" && result.err == io.EOF {
				record.Attempt.ProbeReason = "empty_retry_exhausted"
				prefix.close()
				delivery.close()
				record.HTTPStatus, record.ErrorCode, record.ErrorSource = 502, "upstream_empty_response", "gateway"
				record.Attempt.FailureKind, record.Attempt.ErrorSource = "empty_response", "upstream"
				h.writeError(w, gatewayError(502, "upstream_empty_response"), record.ID)
				return false
			}
			if probe {
				reason := ""
				switch {
				case facts.Replay != protocol.ReplayAllowed:
					reason = "replay_constraint_tightened"
				case facts.Output == "nonempty" || facts.Output == "not_applicable":
					reason = "output_observed"
				case terminalExcludesEmpty(facts):
					reason = "legal_terminal_observed"
				case facts.Error.Present:
					reason = "protocol_error_observed"
				case facts.OutputCoverage == "unavailable" || facts.OutputCoverage == "partial" || facts.Coverage == "unavailable":
					reason = "observation_unavailable"
				case result.err == io.EOF:
					reason = "unconfirmed_empty"
				}
				if reason == "" && result.n > 0 {
					if !prefix.append(chunk[:result.n]) {
						reason = "empty_probe_buffer_limit"
					} else {
						continue
					}
				}
				if reason != "" {
					if err := stopProbe(reason); err != nil {
						return fail(err)
					}
				}
			}
			if !probe && result.n > 0 {
				if err := delivery.write(chunk[:result.n]); err != nil {
					return fail(err)
				}
			}
			if result.err == io.EOF {
				delivery.trailers(resp)
				record.HTTPStatus = resp.StatusCode
				if facts.Error.Present {
					record.Attempt.FailureKind, record.Attempt.ErrorSource = "protocol_error", "upstream"
				} else if !facts.ProtocolComplete {
					record.Attempt.FailureKind = "unverified_result"
				}
				return false
			}
		}
	}
}
