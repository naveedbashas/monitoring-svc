package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	gcppubsub "cloud.google.com/go/pubsub"

	"monitoring/internal/config"
	"monitoring/internal/pubsub"
	"monitoring/internal/relay"
	"monitoring/internal/server"
	"monitoring/internal/websocket"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("failed to load configuration: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	hub := websocket.NewHub()
	go hub.Run()

	redisBridge, err := relay.NewRedisBridge(ctx, relay.RedisOptions{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
		Channel:  cfg.Redis.Channel,
	})
	if err != nil {
		log.Fatalf("failed to initialise redis bridge: %v", err)
	}
	defer func() {
		if err := redisBridge.Close(); err != nil {
			log.Printf("error closing redis bridge: %v", err)
		}
	}()

	psClient, err := gcppubsub.NewClient(ctx, cfg.ProjectID)
	if err != nil {
		log.Fatalf("failed to create pubsub client: %v", err)
	}
	defer func() {
		if err := psClient.Close(); err != nil {
			log.Printf("error closing pubsub client: %v", err)
		}
	}()

	subscriber := pubsub.NewSubscriber(psClient, cfg.SubscriptionID,
		pubsub.WithMaxOutstandingMessages(cfg.MaxOutstandingMessages),
	)

	relayService := relay.NewService(redisBridge)

	go func() {
		if err := subscriber.Run(ctx, relayService.HandleMessage); err != nil && !errors.Is(err, context.Canceled) {
			log.Fatalf("pubsub subscriber error: %v", err)
		}
	}()

	go func() {
		err := redisBridge.Run(ctx, func(payload []byte) {
			if err := hub.Broadcast(payload); err != nil {
				switch {
				case errors.Is(err, websocket.ErrBroadcastOverflow):
					log.Printf("dropping message due to slow websocket consumers: %v", err)
				case errors.Is(err, websocket.ErrHubClosed):
					log.Printf("hub closed while broadcasting message: %v", err)
				default:
					log.Printf("failed to broadcast redis payload: %v", err)
				}
			}
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Fatalf("redis bridge run error: %v", err)
		}
	}()

	srv := server.New(cfg, hub)

	go func() {
		log.Printf("http server listening on %s", cfg.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutdown initiated")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("http server shutdown error: %v", err)
	}

	hub.Shutdown()
}
