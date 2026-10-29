package bootstrap

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"time"
)

func serve(ctx context.Context, address string, output io.Writer) error {
	if err := ctx.Err(); err != nil {
		return nil
	}
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", address)
	if err != nil {
		return fmt.Errorf("数据监听端口绑定失败：%w", err)
	}
	logger := slog.New(slog.NewJSONHandler(output, nil)).With("instance_id", rand.Text(), "config_revision", 1)
	server := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "no-store")
			w.Header().Set("Connection", "close")
			r.Close = true
			http.Error(w, "RelayLeaf 尚未提供数据转发。", http.StatusServiceUnavailable)
		}),
		ReadHeaderTimeout: 5 * time.Second,
		MaxHeaderBytes:    32 << 10,
		// 不设置固定的 WriteTimeout，避免限制长时间响应流的总时长。
	}
	logger.Info("配置已编译，数据转发尚未启用", "address", listener.Addr().String(), "ready", false)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	select {
	case err := <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			closeErr := server.Close()
			logger.Warn("关闭超时，已强制关闭连接")
			if closeErr != nil {
				return closeErr
			}
		}
		err := <-done
		logger.Info("已停止", "ready", false)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	}
}
