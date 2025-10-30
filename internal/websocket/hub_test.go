package websocket

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestHubBroadcastToRegisteredClients(t *testing.T) {
	hub := NewHub()
	go hub.Run()
	defer hub.Shutdown()

	clientA := &client{hub: hub, send: make(chan []byte, 1)}
	clientB := &client{hub: hub, send: make(chan []byte, 1)}

	hub.registerClient(clientA)
	hub.registerClient(clientB)

	msg := []byte("hello")
	if err := hub.Broadcast(msg); err != nil {
		t.Fatalf("Broadcast returned error: %v", err)
	}

	for _, c := range []*client{clientA, clientB} {
		select {
		case got := <-c.send:
			if string(got) != string(msg) {
				t.Fatalf("expected %q, got %q", msg, got)
			}
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for broadcast")
		}
	}
}

func TestHubRegisterUnregister(t *testing.T) {
	hub := NewHub()
	go hub.Run()
	defer hub.Shutdown()

	client := &client{hub: hub, send: make(chan []byte, 1)}

	hub.registerClient(client)
	hub.unregisterClient(client)

	select {
	case _, ok := <-client.send:
		if ok {
			t.Fatalf("expected channel closed after unregister")
		}
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for channel close")
	}
}

func TestHubBroadcastOverflow(t *testing.T) {
	hub := NewHub()
	// do not start Run so broadcast channel is never drained
	for i := 0; i < cap(hub.broadcast); i++ {
		if err := hub.Broadcast([]byte("x")); err != nil {
			t.Fatalf("unexpected error while filling broadcast buffer: %v", err)
		}
	}

	if err := hub.Broadcast([]byte("overflow")); !errors.Is(err, ErrBroadcastOverflow) {
		t.Fatalf("expected ErrBroadcastOverflow, got %v", err)
	}
}

func TestHubShutdownClosesClients(t *testing.T) {
	hub := NewHub()
	go hub.Run()

	client := &client{hub: hub, send: make(chan []byte, 1)}
	hub.registerClient(client)

	hub.Shutdown()

	select {
	case _, ok := <-client.send:
		if ok {
			t.Fatalf("expected client channel closed on shutdown")
		}
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for channel close")
	}
}

func TestHubBroadcastAfterShutdown(t *testing.T) {
	hub := NewHub()
	go hub.Run()

	hub.Shutdown()

	if err := hub.Broadcast([]byte("late")); !errors.Is(err, ErrHubClosed) {
		t.Fatalf("expected ErrHubClosed, got %v", err)
	}
}

func TestHubSlowConsumerDrop(t *testing.T) {
	hub := NewHub()
	go hub.Run()
	defer hub.Shutdown()

	slow := &client{hub: hub, send: make(chan []byte)}
	fast := &client{hub: hub, send: make(chan []byte, 1)}

	hub.registerClient(slow)
	hub.registerClient(fast)

	if err := hub.Broadcast([]byte("message")); err != nil {
		t.Fatalf("Broadcast error: %v", err)
	}

	select {
	case <-fast.send:
	case <-time.After(time.Second):
		t.Fatalf("expected fast client to receive message")
	}

	select {
	case <-slow.send:
		t.Fatalf("slow client should not have received message due to drop")
	case <-time.After(100 * time.Millisecond):
	}

	hub.unregisterClient(slow)
}

func TestServeWebsocketIntegration(t *testing.T) {
	hub := NewHub()
	go hub.Run()
	defer hub.Shutdown()

	server := httptest.NewServer(Handler(hub))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("failed to dial websocket: %v", err)
	}
	defer conn.Close()

	if err := hub.Broadcast([]byte("payload")); err != nil {
		t.Fatalf("Broadcast returned error: %v", err)
	}

	_, msg, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("failed to read websocket message: %v", err)
	}
	if string(msg) != "payload" {
		t.Fatalf("expected payload, got %q", msg)
	}

	if err := conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "")); err != nil {
		t.Fatalf("failed to send close message: %v", err)
	}

	// allow readPump/writePump to observe the close
	time.Sleep(100 * time.Millisecond)
}

func TestServeWebsocketUpgradeFailure(t *testing.T) {
	hub := NewHub()
	handler := Handler(hub)

	req := httptest.NewRequest(http.MethodGet, "/ws", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rec.Code)
	}
}

func TestRegisterAfterShutdown(t *testing.T) {
	hub := NewHub()
	go hub.Run()

	hub.Shutdown()

	client := &client{hub: hub, send: make(chan []byte, 1)}
	hub.registerClient(client)

	select {
	case _, ok := <-client.send:
		if ok {
			t.Fatalf("expected channel closed after shutdown registration")
		}
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for channel close")
	}
}

func TestWritePumpSendsPing(t *testing.T) {
	origWriteWait, origPongWait, origPingPeriod := writeWait, pongWait, pingPeriod
	writeWait = 10 * time.Millisecond
	pongWait = 30 * time.Millisecond
	pingPeriod = 10 * time.Millisecond
	defer func() {
		writeWait = origWriteWait
		pongWait = origPongWait
		pingPeriod = origPingPeriod
	}()

	hub := NewHub()
	go hub.Run()
	defer hub.Shutdown()

	server := httptest.NewServer(Handler(hub))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("failed to dial websocket: %v", err)
	}
	defer conn.Close()

	pingReceived := make(chan struct{}, 1)
	conn.SetPingHandler(func(appData string) error {
		pingReceived <- struct{}{}
		return nil
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	select {
	case <-pingReceived:
	case <-time.After(time.Second):
		t.Fatalf("expected ping within interval")
	}

	if err := conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "")); err != nil {
		t.Fatalf("failed to send close message: %v", err)
	}

	<-done
}
