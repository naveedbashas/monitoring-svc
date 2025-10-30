package websocket

import "net/http"

// Handler returns an HTTP handler that upgrades requests to websocket connections.
func Handler(hub *Hub) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveWebsocket(hub, w, r)
	})
}
