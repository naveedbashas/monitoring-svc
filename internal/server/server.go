package server

import (
	"context"
	"net/http"
	"time"

	"monitoring/internal/config"
	"monitoring/internal/websocket"
)

// Server encapsulates the HTTP server and its handlers.
type Server struct {
	httpServer *http.Server
}

// New constructs an HTTP server configured with websocket and health handlers.
func New(cfg config.Config, hub *websocket.Hub) *Server {
	mux := http.NewServeMux()
	mux.Handle("/ws", websocket.Handler(hub))
	mux.Handle("/healthz", healthHandler())

	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	return &Server{httpServer: srv}
}

// ListenAndServe starts the HTTP server.
func (s *Server) ListenAndServe() error {
	return s.httpServer.ListenAndServe()
}

// Shutdown gracefully shuts down the HTTP server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

func healthHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
}
