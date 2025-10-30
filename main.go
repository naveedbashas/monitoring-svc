package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cloud.google.com/go/pubsub"
)

type pubSubEnvelope struct {
	ID          string            `json:"id"`
	Data        json.RawMessage   `json:"data"`
	Attributes  map[string]string `json:"attributes,omitempty"`
	PublishTime time.Time         `json:"publishTime"`
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	addr := flag.String("addr", envOr("LISTEN_ADDR", ":8080"), "HTTP listen address")
	projectID := flag.String("project", envOr("PUBSUB_PROJECT_ID", ""), "Google Cloud Project ID")
	subscriptionID := flag.String("subscription", envOr("PUBSUB_SUBSCRIPTION_ID", ""), "Pub/Sub subscription ID")
	flag.Parse()

	if *projectID == "" {
		log.Fatal("missing project ID: set PUBSUB_PROJECT_ID or pass --project")
	}

	if *subscriptionID == "" {
		log.Fatal("missing subscription ID: set PUBSUB_SUBSCRIPTION_ID or pass --subscription")
	}

	pubsubClient, err := pubsub.NewClient(ctx, *projectID)
	if err != nil {
		log.Fatalf("failed to create pubsub client: %v", err)
	}
	defer func() {
		if err := pubsubClient.Close(); err != nil {
			log.Printf("error closing pubsub client: %v", err)
		}
	}()

	hub := NewHub()
	go hub.Run()

	go func() {
		sub := pubsubClient.Subscription(*subscriptionID)
		if err := startPubSubReceiver(ctx, sub, hub); err != nil && !errors.Is(err, context.Canceled) {
			log.Fatalf("pubsub receiver error: %v", err)
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWs(hub, w, r)
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	server := &http.Server{
		Addr:    *addr,
		Handler: mux,
	}

	go func() {
		log.Printf("http server listening on %s", *addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutdown initiated")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("http server shutdown error: %v", err)
	}

	hub.Shutdown()
}

func envOr(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}

func startPubSubReceiver(ctx context.Context, sub *pubsub.Subscription, hub *Hub) error {
	sub.ReceiveSettings.Synchronous = false
	sub.ReceiveSettings.MaxOutstandingMessages = 100

	return sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		envelope := pubSubEnvelope{
			ID:          msg.ID,
			Data:        json.RawMessage(msg.Data),
			Attributes:  msg.Attributes,
			PublishTime: msg.PublishTime,
		}

		payload, err := json.Marshal(envelope)
		if err != nil {
			log.Printf("failed to marshal message %s: %v", msg.ID, err)
			msg.Nack()
			return
		}

		if err := hub.Broadcast(payload); err != nil {
			log.Printf("failed to broadcast message %s: %v", msg.ID, err)
			msg.Nack()
			return
		}

		msg.Ack()
	})
}
