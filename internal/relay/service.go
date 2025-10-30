package relay

import (
	"context"
	"fmt"

	"monitoring/internal/pubsub"
)

// Bridge represents a component that delivers payloads to downstream consumers.
type Bridge interface {
	Publish(context.Context, []byte) error
}

// Service transforms Pub/Sub messages into websocket payloads and broadcasts them.
type Service struct {
	bridge Bridge
}

// NewService constructs a new relay service instance.
func NewService(bridge Bridge) *Service {
	return &Service{bridge: bridge}
}

// HandleMessage marshals the Pub/Sub message into JSON and forwards it to connected clients.
func (s *Service) HandleMessage(ctx context.Context, message pubsub.Message) error {
	payload, err := buildRelayPayload(message)
	if err != nil {
		return fmt.Errorf("build relay payload: %w", err)
	}

	if err := s.bridge.Publish(ctx, payload); err != nil {
		return fmt.Errorf("publish message: %w", err)
	}

	return nil
}
