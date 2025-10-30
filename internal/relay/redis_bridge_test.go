package relay

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func TestRedisBridgePublishAndRun(t *testing.T) {
	mini, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer mini.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bridge, err := NewRedisBridge(ctx, RedisOptions{
		Addr:    mini.Addr(),
		Channel: "test-channel",
	})
	if err != nil {
		t.Fatalf("NewRedisBridge returned error: %v", err)
	}

	recv := make(chan []byte, 1)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := bridge.Run(ctx, func(payload []byte) {
			recv <- payload
		}); err != nil && ctx.Err() == nil {
			errCh <- err
		}
	}()

	if err := bridge.Publish(ctx, []byte("payload")); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}

	select {
	case got := <-recv:
		if string(got) != "payload" {
			t.Fatalf("expected payload, got %q", got)
		}
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for payload")
	}

	cancel()
	wg.Wait()

	select {
	case err := <-errCh:
		t.Fatalf("bridge.Run returned unexpected error: %v", err)
	default:
	}

	if err := bridge.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	if err := bridge.Close(); err == nil {
		t.Fatalf("expected error when closing twice")
	}
}

func TestNewRedisBridgeRequiresChannel(t *testing.T) {
	mini, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer mini.Close()

	ctx := context.Background()

	if _, err := NewRedisBridge(ctx, RedisOptions{Addr: mini.Addr()}); err == nil {
		t.Fatalf("expected error when channel missing")
	}
}

func TestRedisBridgeRunSubscriptionClosed(t *testing.T) {
	mini, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer mini.Close()

	bridge, err := NewRedisBridge(context.Background(), RedisOptions{
		Addr:    mini.Addr(),
		Channel: "closing",
	})
	if err != nil {
		t.Fatalf("NewRedisBridge returned error: %v", err)
	}
	defer bridge.Close()

	errCh := make(chan error, 1)

	go func() {
		errCh <- bridge.Run(context.Background(), func([]byte) {})
	}()

	// allow subscription loop to start
	time.Sleep(50 * time.Millisecond)

	if err := bridge.pubsub.Close(); err != nil {
		t.Fatalf("failed to close pubsub: %v", err)
	}

	select {
	case err := <-errCh:
		if err == nil || !strings.Contains(err.Error(), "redis subscription closed") {
			t.Fatalf("expected subscription closed error, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for Run to return")
	}
}

func TestNewRedisBridgePingFailure(t *testing.T) {
	mini, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	addr := mini.Addr()
	mini.Close()

	ctx := context.Background()

	if _, err := NewRedisBridge(ctx, RedisOptions{Addr: addr, Channel: "chan"}); err == nil {
		t.Fatalf("expected ping failure error")
	}
}
