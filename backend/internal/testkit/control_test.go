package testkit

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

func waitSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("等待信号超过安全截止时间")
	}
}

func TestGateReachedCancellationAndIdempotentOpen(t *testing.T) {
	g := NewGate()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- g.Wait(ctx) }()
	waitSignal(t, g.Reached())
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("取消：%v", err)
	}
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(g.Open)
	}
	wg.Wait()
	if err := g.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestClockTimers(t *testing.T) {
	now := time.Date(2026, 10, 28, 0, 0, 0, 0, time.UTC)
	c := NewClock(now)
	a, b := c.NewTimer(time.Second), c.NewTimer(2*time.Second)
	if !b.Stop() || b.Stop() {
		t.Fatal("停止操作必须恰好阻止一次发送")
	}
	c.Advance(999 * time.Millisecond)
	select {
	case <-a.C():
		t.Fatal("定时器提前触发")
	default:
	}
	c.Advance(time.Millisecond)
	if got := <-a.C(); !got.Equal(now.Add(time.Second)) {
		t.Fatalf("截止时间：%v", got)
	}
	if a.Stop() {
		t.Fatal("已触发的定时器被停止")
	}
	c.Advance(time.Hour)
	select {
	case <-b.C():
		t.Fatal("已停止的定时器仍触发")
	default:
	}
	if got := <-c.NewTimer(0).C(); !got.Equal(c.Now()) {
		t.Fatal("立即触发的定时器时间不符")
	}
}

func TestScriptedReaderPreservesBytesAndTailError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g := NewGate()
	tail := errors.New("注入的尾部读取错误")
	r := NewScriptedReader(ctx, ReadStep{Bytes: []byte("one")}, ReadStep{Gate: g, Bytes: []byte("二"), Err: tail})
	done := make(chan struct{})
	var body []byte
	var err error
	go func() { body, err = io.ReadAll(r); close(done) }()
	waitSignal(t, g.Reached())
	g.Open()
	waitSignal(t, done)
	if !bytes.Equal(body, []byte("one二")) || !errors.Is(err, tail) {
		t.Fatalf("实际得到 %q %v", body, err)
	}
}

func TestScriptedReaderCancellationAtBlockedStep(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	g := NewGate()
	r := NewScriptedReader(ctx, ReadStep{Gate: g, Bytes: []byte("never")})
	done := make(chan error, 1)
	go func() { _, err := io.ReadAll(r); done <- err }()
	waitSignal(t, g.Reached())
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
