package pubsub

import (
	"context"
	"log"
	"time"

	gcppubsub "cloud.google.com/go/pubsub"
)

// Message represents a Pub/Sub message delivered to the service.
type Message struct {
	ID          string
	Data        []byte
	Attributes  map[string]string
	PublishTime time.Time
}

// Handler processes a single Pub/Sub message.
type Handler func(context.Context, Message) error

// Subscriber wraps a Pub/Sub subscription and invokes a handler for each message.
type Subscriber struct {
	subscription *gcppubsub.Subscription
}

// Option configures subscriber behaviour.
type Option func(*gcppubsub.ReceiveSettings)

// WithMaxOutstandingMessages sets the maximum number of outstanding messages for the subscription.
func WithMaxOutstandingMessages(max int) Option {
	return func(settings *gcppubsub.ReceiveSettings) {
		settings.MaxOutstandingMessages = max
	}
}

// WithReceiveSettings applies a custom ReceiveSettings struct directly.
func WithReceiveSettings(settings gcppubsub.ReceiveSettings) Option {
	return func(dst *gcppubsub.ReceiveSettings) {
		*dst = settings
	}
}

// NewSubscriber creates a new Subscriber for the given Pub/Sub subscription ID.
func NewSubscriber(client *gcppubsub.Client, subscriptionID string, opts ...Option) *Subscriber {
	subscription := client.Subscription(subscriptionID)
	settings := subscription.ReceiveSettings
	for _, opt := range opts {
		opt(&settings)
	}
	subscription.ReceiveSettings = settings

	return &Subscriber{subscription: subscription}
}

// Run starts receiving messages until the context is cancelled or an error occurs.
func (s *Subscriber) Run(ctx context.Context, handler Handler) error {
	return s.subscription.Receive(ctx, func(ctx context.Context, msg *gcppubsub.Message) {
		message := Message{
			ID:          msg.ID,
			Data:        append([]byte(nil), msg.Data...),
			Attributes:  cloneAttributes(msg.Attributes),
			PublishTime: msg.PublishTime,
		}

		if err := handler(ctx, message); err != nil {
			log.Printf("handler error for message %s: %v", message.ID, err)
			msg.Nack()
			return
		}

		msg.Ack()
	})
}

func cloneAttributes(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}

	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
