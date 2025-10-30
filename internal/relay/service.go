package relay

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

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
	envelope := struct {
		ID          string            `json:"id"`
		Data        json.RawMessage   `json:"data"`
		Attributes  map[string]string `json:"attributes,omitempty"`
		PublishTime string            `json:"publishTime"`
	}{
		ID:          message.ID,
		Data:        json.RawMessage(message.Data),
		Attributes:  message.Attributes,
		PublishTime: message.PublishTime.UTC().Format(time.RFC3339Nano),
	}

	payload, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}

	if err := s.bridge.Publish(ctx, payload); err != nil {
		return fmt.Errorf("publish message: %w", err)
	}

	return nil
}
