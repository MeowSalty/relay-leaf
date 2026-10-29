package testkit

import (
	"sync"
	"time"
)

// Clock 仅在测试要求时推进时间。它模拟一次性定时器，
// 不控制真实 socket 的实际截止时间；后者仍需安全超时。
type Clock struct {
	mu     sync.Mutex
	now    time.Time
	timers map[*Timer]struct{}
}

func NewClock(now time.Time) *Clock { return &Clock{now: now, timers: make(map[*Timer]struct{})} }
func (c *Clock) Now() time.Time     { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *Clock) NewTimer(d time.Duration) *Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &Timer{clock: c, deadline: c.now.Add(d), ch: make(chan time.Time, 1), active: true}
	if d <= 0 {
		t.ch <- c.now
		t.active = false
	} else {
		c.timers[t] = struct{}{}
	}
	return t
}

// Advance 在时间倒退时 panic，并向每个到期定时器发送其截止时间。
func (c *Clock) Advance(d time.Duration) {
	if d < 0 {
		panic("testkit.Clock 不能倒退")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	for t := range c.timers {
		if !t.deadline.After(c.now) {
			t.ch <- t.deadline
			t.active = false
			delete(c.timers, t)
		}
	}
}

type Timer struct {
	clock    *Clock
	deadline time.Time
	ch       chan time.Time
	active   bool
}

func (t *Timer) C() <-chan time.Time { return t.ch }

// Stop 仅在阻止了发送时返回 true。它不会关闭 C。
func (t *Timer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	if !t.active {
		return false
	}
	t.active = false
	delete(t.clock.timers, t)
	return true
}
