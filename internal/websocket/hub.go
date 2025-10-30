package websocket

import (
	"errors"
	"log"
	"sync"
)

var (
	// ErrHubClosed indicates the hub has been shut down and no further broadcasts are accepted.
	ErrHubClosed = errors.New("hub closed")
	// ErrBroadcastOverflow indicates the broadcast buffer is full.
	ErrBroadcastOverflow = errors.New("hub broadcast buffer full")
)

// Hub maintains active websocket clients and broadcasts messages to them.
type Hub struct {
	register   chan *client
	unregister chan *client
	broadcast  chan []byte

	clients map[*client]struct{}

	closing chan struct{}
	done    chan struct{}
	once    sync.Once
}

// NewHub constructs a hub instance.
func NewHub() *Hub {
	return &Hub{
		register:   make(chan *client),
		unregister: make(chan *client),
		broadcast:  make(chan []byte, 256),
		clients:    make(map[*client]struct{}),
		closing:    make(chan struct{}),
		done:       make(chan struct{}),
	}
}

// Run executes the hub loop and should be invoked in a goroutine.
func (h *Hub) Run() {
	defer close(h.done)

	for {
		select {
		case client := <-h.register:
			h.clients[client] = struct{}{}
		case client := <-h.unregister:
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
			}
		case message := <-h.broadcast:
			for client := range h.clients {
				select {
				case client.send <- append([]byte(nil), message...):
				default:
					log.Printf("dropping message for slow client %p", client)
				}
			}
		case <-h.closing:
			for client := range h.clients {
				close(client.send)
				delete(h.clients, client)
			}
			return
		}
	}
}

// Broadcast fan-outs the message to all connected clients.
func (h *Hub) Broadcast(message []byte) error {
	select {
	case <-h.closing:
		return ErrHubClosed
	default:
	}

	payload := append([]byte(nil), message...)

	select {
	case h.broadcast <- payload:
		return nil
	case <-h.closing:
		return ErrHubClosed
	default:
		return ErrBroadcastOverflow
	}
}

// Shutdown terminates the hub loop and closes client connections.
func (h *Hub) Shutdown() {
	h.once.Do(func() {
		close(h.closing)
		<-h.done
	})
}

func (h *Hub) registerClient(c *client) {
	select {
	case <-h.closing:
		close(c.send)
	case h.register <- c:
	}
}

func (h *Hub) unregisterClient(c *client) {
	select {
	case <-h.closing:
	case h.unregister <- c:
	}
}
