package relay

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// CopyResponse 是职责有限的字节复制原语，不处理 relay 重试。
// 写入端必须支持 ResponseController 的截止时间设置（包装器必须实现 Unwrap）。
// 每个可用数据块都会刷新，不设置固定的流总超时。
func CopyResponse(ctx context.Context, w http.ResponseWriter, resp *http.Response, writeIdle time.Duration) error {
	if ctx == nil || resp == nil || resp.Body == nil || writeIdle < 0 {
		return errors.New("relay: 响应复制参数无效")
	}
	defer resp.Body.Close()
	rc := http.NewResponseController(w)
	if err := rc.SetWriteDeadline(time.Time{}); err != nil {
		return err
	}
	var mu sync.Mutex
	cancelled := false
	callbackDone := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		mu.Lock()
		cancelled = true
		_ = rc.SetWriteDeadline(time.Now())
		mu.Unlock()
		_ = resp.Body.Close()
		close(callbackDone)
	})
	defer func() {
		if !stop() {
			<-callbackDone
		}
		_ = rc.SetWriteDeadline(time.Time{})
	}()
	arm := func() error {
		mu.Lock()
		defer mu.Unlock()
		if cancelled || ctx.Err() != nil {
			return ctx.Err()
		}
		deadline := time.Time{}
		if writeIdle > 0 {
			deadline = time.Now().Add(writeIdle)
		}
		return rc.SetWriteDeadline(deadline)
	}
	disarm := func() error {
		mu.Lock()
		defer mu.Unlock()
		if cancelled {
			return ctx.Err()
		}
		return rc.SetWriteDeadline(time.Time{})
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	header := resp.Header.Clone()
	for _, field := range header.Values("Connection") {
		for _, token := range strings.Split(field, ",") {
			header.Del(strings.TrimSpace(token))
		}
	}
	for _, key := range []string{"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "TE", "Trailer", "Transfer-Encoding", "Upgrade"} {
		header.Del(key)
	}
	announced := make(map[string]bool, len(resp.Trailer))
	// HTTP/2 的 trailer 无需预先声明。即使初始 Trailer 映射为空，
	// 也要移除帧定界长度，以便后续出现的 trailer 能转发到 HTTP/1。
	if len(resp.Trailer) > 0 || resp.ProtoMajor == 2 {
		header.Del("Content-Length")
	}
	for key, values := range header {
		w.Header()[key] = append([]string(nil), values...)
	}
	for key := range resp.Trailer {
		w.Header().Add("Trailer", key)
		announced[http.CanonicalHeaderKey(key)] = true
	}
	if err := arm(); err != nil {
		return err
	}
	w.WriteHeader(resp.StatusCode)
	if err := rc.Flush(); err != nil {
		return err
	}
	if err := disarm(); err != nil {
		return err
	}
	buf := make([]byte, 32*1024)
	for {
		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			if err := arm(); err != nil {
				return err
			}
			written, err := w.Write(buf[:n])
			if err != nil {
				return err
			}
			if written != n {
				return io.ErrShortWrite
			}
			if err := arm(); err != nil {
				return err
			}
			if err := rc.Flush(); err != nil {
				return err
			}
			if err := disarm(); err != nil {
				return err
			}
		}
		if readErr == io.EOF {
			for key, values := range resp.Trailer {
				if !announced[http.CanonicalHeaderKey(key)] {
					key = http.TrailerPrefix + key
				}
				w.Header()[key] = append([]string(nil), values...)
			}
			return nil
		}
		if readErr != nil {
			return readErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
}
