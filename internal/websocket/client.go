package websocket

import (
	"log"
	"net/http"
	"time"

	gorilla "github.com/gorilla/websocket"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 512
)

var upgrader = gorilla.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(_ *http.Request) bool {
		return true
	},
}

type client struct {
	hub  *Hub
	conn *gorilla.Conn
	send chan []byte
}

func newClient(hub *Hub, conn *gorilla.Conn) *client {
	return &client{
		hub:  hub,
		conn: conn,
		send: make(chan []byte, 128),
	}
}

func serveWebsocket(hub *Hub, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("failed to upgrade websocket: %v", err)
		return
	}

	c := newClient(hub, conn)
	hub.registerClient(c)

	go c.writePump()
	go c.readPump()
}

func (c *client) readPump() {
	defer func() {
		c.hub.unregisterClient(c)
		if err := c.conn.Close(); err != nil {
			log.Printf("error closing websocket connection: %v", err)
		}
	}()

	c.conn.SetReadLimit(maxMessageSize)
	_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		if _, _, err := c.conn.ReadMessage(); err != nil {
			if gorilla.IsUnexpectedCloseError(err, gorilla.CloseGoingAway, gorilla.CloseAbnormalClosure) {
				log.Printf("websocket close error: %v", err)
			}
			break
		}
	}
}

func (c *client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		if err := c.conn.Close(); err != nil {
			log.Printf("error closing websocket connection: %v", err)
		}
	}()

	for {
		select {
		case message, ok := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				_ = c.conn.WriteMessage(gorilla.CloseMessage, nil)
				return
			}

			if err := c.conn.WriteMessage(gorilla.TextMessage, message); err != nil {
				log.Printf("error writing websocket message: %v", err)
				return
			}
		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(gorilla.PingMessage, nil); err != nil {
				log.Printf("error writing websocket ping: %v", err)
				return
			}
		}
	}
}
