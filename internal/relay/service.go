package relay

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"monitoring/internal/pubsub"
)

// Broadcaster represents a destination capable of broadcasting messages to clients.
type Broadcaster interface {
	Broadcast([]byte) error
}

// Service transforms Pub/Sub messages into websocket payloads and broadcasts them.
type Service struct {
	broadcaster Broadcaster
}

// NewService constructs a new relay service instance.
func NewService(broadcaster Broadcaster) *Service {
	return &Service{broadcaster: broadcaster}
}

// HandleMessage marshals the Pub/Sub message into JSON and forwards it to connected clients.
func (s *Service) HandleMessage(_ context.Context, message pubsub.Message) error {
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

	if err := s.broadcaster.Broadcast(payload); err != nil {
		return fmt.Errorf("broadcast message: %w", err)
	}

	return nil
}
