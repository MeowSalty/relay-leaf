package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"relayleaf/internal/bootstrap"
)

func main() {
	os.Exit(run())
}

func run() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return bootstrap.Run(ctx, os.Args[1:], os.Stdout, os.Stderr)
}
