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

	relayService := relay.NewService(hub)

	go func() {
		if err := subscriber.Run(ctx, relayService.HandleMessage); err != nil && !errors.Is(err, context.Canceled) {
			log.Fatalf("pubsub subscriber error: %v", err)
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
