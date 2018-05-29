package commons

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"
)

func CaptureSigint(ctx context.Context, cancel context.CancelFunc) {
	if ctx == nil || cancel == nil {
		Log.Error("ctx or cancel == nil")
	}

	sigs := make(chan os.Signal, 10)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL)

	go func() {
		select {
		case sig := <-sigs:
			Log.Info("caught signal", zap.Any("signal", sig))

		case <-ctx.Done():
			Log.Info("context cancelled")
		}

		if cancel != nil {
			Log.Info("Canceling all processes...")
			cancel()
		}

		Log.Sync()

		time.Sleep(5 * time.Second)
		os.Exit(0)
	}()
}
