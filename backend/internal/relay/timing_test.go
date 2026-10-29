package relay

import (
	"context"
	"errors"
	"testing"
	"time"

	"relayleaf/internal/catalog"
	"relayleaf/internal/testkit"
)

func TestStageDeadlineOrderingDoesNotDependOnTimerCallbacks(t *testing.T) {
	for _, callerFirst := range []bool{true, false} {
		t.Run(map[bool]string{true: "相同截止由调用方约束", false: "阶段更早由阶段约束"}[callerFirst], func(t *testing.T) {
			base := time.Now()
			clock := testkit.NewClock(base)
			callerDue := base.Add(2 * time.Hour)
			if callerFirst {
				callerDue = base.Add(time.Hour)
			}
			parent, cancelParent := context.WithDeadline(context.Background(), callerDue)
			defer cancelParent()
			ctx, cancel := context.WithCancelCause(parent)
			defer cancel(nil)
			armed := make(chan struct{}, 1)
			factory := func(d time.Duration) Timer {
				timer := clock.NewTimer(d)
				select {
				case armed <- struct{}{}:
				default:
				}
				return timer
			}
			timers := newAttemptTimers(parent, catalog.Execution{}, clock.Now, factory, cancel)
			defer timers.close()
			timers.arm("request_body_timeout", time.Hour)
			select {
			case <-armed:
			case <-time.After(5 * time.Second):
				t.Fatal("阶段定时器没有建立")
			}
			clock.Advance(time.Hour)
			select {
			case <-ctx.Done():
			case <-time.After(5 * time.Second):
				t.Fatal("推进逻辑时间后没有触发截止")
			}
			cause := context.Cause(ctx)
			if callerFirst {
				if !errors.Is(cause, context.DeadlineExceeded) {
					t.Fatalf("相同截止不应按回调先后归罪阶段：%v", cause)
				}
			} else {
				var phase *executionFailure
				if !errors.As(cause, &phase) || phase.kind != "request_body_timeout" {
					t.Fatalf("更早的阶段截止丢失：%v", cause)
				}
			}
		})
	}
}
