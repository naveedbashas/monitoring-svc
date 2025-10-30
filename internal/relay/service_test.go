package relay

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"monitoring/internal/pubsub"
)

type stubBridge struct {
	mu       sync.Mutex
	payloads [][]byte
	err      error
}

func (s *stubBridge) Publish(_ context.Context, payload []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	copy := append([]byte(nil), payload...)
	s.payloads = append(s.payloads, copy)
	return nil
}

func TestServiceHandleMessage(t *testing.T) {
	bridge := &stubBridge{}
	service := NewService(bridge)

	msg := pubsub.Message{Data: []byte(`{"label":"channel","profile":{"label":"default"}}`)}

	if err := service.HandleMessage(context.Background(), msg); err != nil {
		t.Fatalf("HandleMessage returned error: %v", err)
	}

	if len(bridge.payloads) != 1 {
		t.Fatalf("expected 1 payload published, got %d", len(bridge.payloads))
	}
}

func TestServiceHandleMessagePublishError(t *testing.T) {
	bridge := &stubBridge{err: errors.New("publish error")}
	service := NewService(bridge)

	msg := pubsub.Message{}

	if err := service.HandleMessage(context.Background(), msg); err == nil || !strings.Contains(err.Error(), "publish error") {
		t.Fatalf("expected publish error, got %v", err)
	}
}

func TestServiceHandleMessageInvalidPayload(t *testing.T) {
	bridge := &stubBridge{}
	service := NewService(bridge)

	msg := pubsub.Message{Data: []byte("invalid json")}

	if err := service.HandleMessage(context.Background(), msg); err == nil || !strings.Contains(err.Error(), "unmarshal") {
		t.Fatalf("expected unmarshal error, got %v", err)
	}
}
