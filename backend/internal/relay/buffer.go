package relay

import (
	"context"
	"errors"
	"io"

	runstate "relayleaf/internal/runtime"
)

var (
	errBodyTooLarge = errors.New("请求正文超过大小限制")
	errBufferFull   = errors.New("实例缓冲容量不足")
)

type bufferBudget struct {
	manager *runstate.Manager
	kind    runstate.BufferKind
}

func (b bufferBudget) Reserve(n int64) bool { return b.manager.ReserveBuffer(b.kind, n) }
func (b bufferBudget) Release(n int64)      { b.manager.ReleaseBuffer(b.kind, n) }

// ownedBuffer 在扩容期间同时计入新旧数组，避免低估实际持有的内存。
type ownedBuffer struct {
	data   []byte
	limit  int64
	budget bufferBudget
}

func (b *ownedBuffer) grow(need int64) bool {
	if need <= int64(cap(b.data)) {
		return true
	}
	if need > b.limit {
		return false
	}
	next := max(int64(4096), int64(cap(b.data))*2, need)
	next = min(next, b.limit)
	if !b.budget.Reserve(next) {
		// 不退化成每字节重新分配和复制，以免接近共享额度时产生平方级工作量。
		return false
	}
	data := make([]byte, len(b.data), int(next))
	copy(data, b.data)
	old := int64(cap(b.data))
	b.data = data
	b.budget.Release(old)
	return true
}

func (b *ownedBuffer) append(data []byte) bool {
	if !b.grow(int64(len(b.data)) + int64(len(data))) {
		return false
	}
	b.data = append(b.data, data...)
	return true
}

func (b *ownedBuffer) close() {
	n := int64(cap(b.data))
	b.data = nil
	b.budget.Release(n)
}

func (b *ownedBuffer) read(ctx context.Context, r io.Reader) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if int64(len(b.data)) == b.limit {
			var extra [1]byte
			n, err := r.Read(extra[:])
			if n > 0 {
				return errBodyTooLarge
			}
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			continue
		}
		if len(b.data) == cap(b.data) && !b.grow(int64(len(b.data))+1) {
			return errBufferFull
		}
		start := len(b.data)
		chunk := b.data[start:cap(b.data)]
		n, err := r.Read(chunk)
		b.data = b.data[:start+n]
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
