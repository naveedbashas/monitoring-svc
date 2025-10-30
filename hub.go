package main

import (
	"errors"
	"log"
	"sync"
)

var (
	errHubClosed         = errors.New("hub closed")
	errBroadcastOverflow = errors.New("hub broadcast buffer full")
)

type Hub struct {
	register   chan *Client
	unregister chan *Client
	broadcast  chan []byte

	clients map[*Client]struct{}

	closing chan struct{}
	done    chan struct{}
	once    sync.Once
}

func NewHub() *Hub {
	return &Hub{
		register:   make(chan *Client),
		unregister: make(chan *Client),
		broadcast:  make(chan []byte, 256),
		clients:    make(map[*Client]struct{}),
		closing:    make(chan struct{}),
		done:       make(chan struct{}),
	}
}

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

func (h *Hub) Broadcast(message []byte) error {
	select {
	case <-h.closing:
		return errHubClosed
	default:
	}

	payload := append([]byte(nil), message...)

	select {
	case h.broadcast <- payload:
		return nil
	case <-h.closing:
		return errHubClosed
	default:
		return errBroadcastOverflow
	}
}

func (h *Hub) Shutdown() {
	h.once.Do(func() {
		close(h.closing)
		<-h.done
	})
}

func (h *Hub) Register(client *Client) {
	select {
	case <-h.closing:
		close(client.send)
	case h.register <- client:
	}
}

func (h *Hub) Unregister(client *Client) {
	select {
	case <-h.closing:
	case h.unregister <- client:
	}
}
