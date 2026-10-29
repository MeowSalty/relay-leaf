package relay

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http/httptrace"
	"sync"
	"time"

	"relayleaf/internal/catalog"
)

// Timer 允许测试用可控时间验证逻辑截止，实际阻塞 I/O 仍由取消解除。
type Timer interface {
	C() <-chan time.Time
	Stop() bool
}

type wallTimer struct{ timer *time.Timer }

func (t wallTimer) C() <-chan time.Time { return t.timer.C }
func (t wallTimer) Stop() bool          { return t.timer.Stop() }

type executionFailure struct {
	kind     string
	deadline time.Time
}

func (e *executionFailure) Error() string { return "请求执行结束：" + e.kind }

var errShutdown = errors.New("服务正在停止")

// attemptTimers 的所有期限都归同一执行所有者；每个完成阶段独立停止。
type attemptTimers struct {
	mu             sync.Mutex
	timers         map[string]time.Time
	wake           chan struct{}
	done           chan struct{}
	closed         bool
	headers        bool
	cancel         context.CancelCauseFunc
	newTimer       func(time.Duration) Timer
	now            func() time.Time
	exec           catalog.Execution
	firstByte      time.Time
	callerDeadline time.Time
}

type attemptTimersKey struct{}

func newAttemptTimers(ctx context.Context, exec catalog.Execution, now func() time.Time, newTimer func(time.Duration) Timer, cancel context.CancelCauseFunc) *attemptTimers {
	t := &attemptTimers{timers: make(map[string]time.Time), wake: make(chan struct{}, 1), done: make(chan struct{}), exec: exec, now: now, newTimer: newTimer, cancel: cancel}
	t.callerDeadline, _ = ctx.Deadline()
	go t.run()
	return t
}

func (t *attemptTimers) changed() {
	select {
	case t.wake <- struct{}{}:
	default:
	}
}

// 每个尝试只有一个计时任务，快速分块读取不会制造每块一个 goroutine。
func (t *attemptTimers) run() {
	defer close(t.done)
	for {
		t.mu.Lock()
		if t.closed {
			t.mu.Unlock()
			return
		}
		kind := ""
		var deadline time.Time
		for name, due := range t.timers {
			if kind == "" || due.Before(deadline) || (due.Equal(deadline) && name < kind) {
				kind, deadline = name, due
			}
		}
		t.mu.Unlock()
		if kind == "" {
			<-t.wake
			continue
		}
		timer := t.newTimer(max(time.Duration(0), deadline.Sub(t.now())))
		select {
		case <-t.wake:
			timer.Stop()
		case <-timer.C():
			t.mu.Lock()
			if due, ok := t.timers[kind]; !t.closed && ok && due.Equal(deadline) && !t.now().Before(due) {
				delete(t.timers, kind)
				if !t.callerDeadline.IsZero() && !t.callerDeadline.After(due) {
					t.cancel(context.DeadlineExceeded)
				} else {
					t.cancel(&executionFailure{kind: kind, deadline: due})
				}
			}
			t.mu.Unlock()
		}
	}
}

func (t *attemptTimers) arm(kind string, duration time.Duration) {
	if duration <= 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return
	}
	t.timers[kind] = t.now().Add(duration)
	t.changed()
}

func (t *attemptTimers) stopLocked(kind string) {
	delete(t.timers, kind)
	t.changed()
}

func (t *attemptTimers) stop(kind string) {
	t.mu.Lock()
	t.stopLocked(kind)
	t.mu.Unlock()
}

func (t *attemptTimers) close() {
	t.mu.Lock()
	t.closed = true
	for kind := range t.timers {
		t.stopLocked(kind)
	}
	t.changed()
	t.mu.Unlock()
	<-t.done
}

func (t *attemptTimers) gotHeaders() {
	t.mu.Lock()
	t.headers = true
	t.stopLocked("response_header_timeout")
	t.mu.Unlock()
}

func (t *attemptTimers) byteTime() time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.firstByte
}

func (t *attemptTimers) trace() *httptrace.ClientTrace {
	return &httptrace.ClientTrace{
		TLSHandshakeStart: func() { t.arm("tls_handshake_timeout", t.exec.TLSHandshakeTimeout) },
		TLSHandshakeDone:  func(_ tls.ConnectionState, _ error) { t.stop("tls_handshake_timeout") },
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			if info.Err != nil {
				return
			}
			t.mu.Lock()
			already := t.headers || t.closed
			t.mu.Unlock()
			if !already {
				t.arm("response_header_timeout", t.exec.ResponseHeaderTimeout)
				// 响应头可能与完成上传同时到达；再次复核避免留下过期的计时器。
				t.mu.Lock()
				if t.headers {
					t.stopLocked("response_header_timeout")
				}
				t.mu.Unlock()
			}
		},
		GotFirstResponseByte: func() {
			t.mu.Lock()
			if t.firstByte.IsZero() {
				t.firstByte = t.now()
			}
			t.mu.Unlock()
		},
	}
}
