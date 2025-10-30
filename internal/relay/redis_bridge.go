package relay

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/redis/go-redis/v9"
)

// RedisOptions defines configuration for Redis bridge.
type RedisOptions struct {
	Addr     string
	Password string
	DB       int
	Channel  string
}

// RedisBridge publishes messages to a Redis channel and reads them back for local dispatch.
type RedisBridge struct {
	client *redis.Client
	pubsub *redis.PubSub
	opts   RedisOptions
}

// NewRedisBridge constructs a Redis-backed relay bridge.
func NewRedisBridge(ctx context.Context, opts RedisOptions) (*RedisBridge, error) {
	if opts.Channel == "" {
		return nil, errors.New("redis channel must be provided")
	}

	client := redis.NewClient(&redis.Options{
		Addr:     opts.Addr,
		Password: opts.Password,
		DB:       opts.DB,
	})

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}

	pubsub := client.Subscribe(ctx, opts.Channel)
	if _, err := pubsub.Receive(ctx); err != nil {
		_ = pubsub.Close()
		_ = client.Close()
		return nil, fmt.Errorf("redis subscribe: %w", err)
	}

	return &RedisBridge{
		client: client,
		pubsub: pubsub,
		opts:   opts,
	}, nil
}

// Publish pushes payloads to the configured Redis channel.
func (b *RedisBridge) Publish(ctx context.Context, payload []byte) error {
	return b.client.Publish(ctx, b.opts.Channel, payload).Err()
}

// Run consumes messages from Redis and forwards them to the supplied handler.
func (b *RedisBridge) Run(ctx context.Context, handler func([]byte)) error {
	ch := b.pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-ch:
			if !ok {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return errors.New("redis subscription closed")
			}
			if msg == nil {
				continue
			}
			handler([]byte(msg.Payload))
		}
	}
}

// Close releases the Redis resources.
func (b *RedisBridge) Close() error {
	var errs []error
	if err := b.pubsub.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := b.client.Close(); err != nil {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		for _, err := range errs {
			log.Printf("redis bridge close error: %v", err)
		}
		return fmt.Errorf("failed to close redis bridge: %v", errs[len(errs)-1])
	}
	return nil
}
