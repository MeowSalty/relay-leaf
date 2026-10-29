package testkit

import (
	"context"
	"io"
)

// ReadStep 提供原始字节，可选择在返回字节前暂停，并在返回后
// 报错。错误属于传输状态，不会编码进 JSON/SSE 正文。
type ReadStep struct {
	Bytes []byte
	Gate  *Gate
	Err   error
}

// ScriptedReader 由单个 goroutine 读取；构造时复制输入字节。
type ScriptedReader struct {
	ctx           context.Context
	steps         []ReadStep
	index, offset int
	entered       bool
}

func NewScriptedReader(ctx context.Context, steps ...ReadStep) *ScriptedReader {
	r := &ScriptedReader{ctx: ctx, steps: make([]ReadStep, len(steps))}
	for i, s := range steps {
		s.Bytes = append([]byte(nil), s.Bytes...)
		r.steps[i] = s
	}
	return r
}
func (r *ScriptedReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for r.index < len(r.steps) {
		if err := r.ctx.Err(); err != nil {
			return 0, err
		}
		s := &r.steps[r.index]
		if !r.entered {
			r.entered = true
			if s.Gate != nil {
				if err := s.Gate.Wait(r.ctx); err != nil {
					return 0, err
				}
			}
		}
		if r.offset < len(s.Bytes) {
			n := copy(p, s.Bytes[r.offset:])
			r.offset += n
			return n, nil
		}
		err := s.Err
		r.index++
		r.offset = 0
		r.entered = false
		if err != nil {
			return 0, err
		}
	}
	return 0, io.EOF
}
