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

	"relayleaf/internal/catalog"
	"relayleaf/internal/relay"
)

func serve(ctx context.Context, snapshot *catalog.Snapshot, output io.Writer) (result error) {
	if err := ctx.Err(); err != nil {
		return nil
	}
	handler, err := relay.NewHandler(snapshot, relay.HandlerOptions{})
	if err != nil {
		return fmt.Errorf("数据转发初始化失败：%w", err)
	}
	defer func() { result = errors.Join(result, handler.Close()) }()
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", snapshot.GatewayAddress())
	if err != nil {
		return fmt.Errorf("数据监听端口绑定失败：%w", err)
	}
	logger := slog.New(slog.NewJSONHandler(output, nil)).With("instance_id", rand.Text(), "config_revision", 1)
	server := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		MaxHeaderBytes:    32 << 10,
		// 不设置固定的 WriteTimeout，避免限制长时间响应流的总时长。
	}
	logger.Info("数据监听已启动", "address", listener.Addr().String(), "ready", true)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	select {
	case err := <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		handler.StopAccepting()
		logger.Info("停止接纳，正在排空在途请求", "ready", false)
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
