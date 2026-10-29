// Package testkit 为中继测试提供确定性的本地样本。
package testkit

import (
	"context"
	"sync"
)

// Gate 在阻塞前记录到达，随后等待显式打开或取消。
// Reached 记录等待尝试，即使 context 已被取消也会记录。
type Gate struct {
	reached   chan struct{}
	opened    chan struct{}
	reachOnce sync.Once
	openOnce  sync.Once
}

func NewGate() *Gate                     { return &Gate{reached: make(chan struct{}), opened: make(chan struct{})} }
func (g *Gate) Reached() <-chan struct{} { return g.reached }
func (g *Gate) Open()                    { g.openOnce.Do(func() { close(g.opened) }) }
func (g *Gate) Wait(ctx context.Context) error {
	g.reachOnce.Do(func() { close(g.reached) })
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-g.opened:
		return ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}
