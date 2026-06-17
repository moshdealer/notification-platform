package websocket

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/moshdealer/notification-platform/pkg/observability"
)

const (
	// Интервал отправки ping
	pingPeriod = 25 * time.Second
	// Сколько ждать pong перед тем, как считать соединение мёртвым
	pongWait = 60 * time.Second
)

type Client struct {
	conn   *websocket.Conn
	userID string
	send   chan []byte
	once   sync.Once
	ctx    context.Context
	cancel context.CancelFunc
}

type Manager struct {
	clients map[string]map[*Client]bool
	mu      sync.RWMutex
	ctx     context.Context
	cancel  context.CancelFunc
}

func NewManagerWithContext(rootCtx context.Context) *Manager {
	ctx, cancel := context.WithCancel(rootCtx)
	return &Manager{
		clients: make(map[string]map[*Client]bool),
		ctx:     ctx,
		cancel:  cancel,
	}
}

func (m *Manager) Register(userID string, conn *websocket.Conn) *Client {
	clientCtx, clientCancel := context.WithCancel(m.ctx)

	client := &Client{
		conn:   conn,
		userID: userID,
		send:   make(chan []byte, 256),
		ctx:    clientCtx,
		cancel: clientCancel,
	}

	conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	m.mu.Lock()
	if _, ok := m.clients[userID]; !ok {
		m.clients[userID] = make(map[*Client]bool)
	}
	m.clients[userID][client] = true
	m.mu.Unlock()

	go client.writePump(m)
	go client.readPump(m)

	observability.Info(clientCtx, "User connected", "user_id", userID)
	observability.WebSocketConnectionsActive.Inc()
	return client
}

func (m *Manager) Unregister(userID string, c *Client) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if conns, ok := m.clients[userID]; ok {
		delete(conns, c)
		if len(conns) == 0 {
			delete(m.clients, userID)
			observability.Info(m.ctx, "User fully disconnected", "user_id", userID)
		}
	}

	c.once.Do(func() {
		close(c.send)
	})

	if c.cancel != nil {
		c.cancel()
	}

	observability.WebSocketConnectionsActive.Dec()
	c.conn.Close()
}

func (m *Manager) SendToUser(ctx context.Context, userID string, data []byte) error {
	m.mu.RLock()
	connsMap, ok := m.clients[userID]
	if !ok || len(connsMap) == 0 {
		m.mu.RUnlock()
		return fmt.Errorf("no active connections for user %s", userID)
	}

	clients := make([]*Client, 0, len(connsMap))
	for c := range connsMap {
		clients = append(clients, c)
	}
	m.mu.RUnlock()

	allAccepted := true

	for _, client := range clients {
		select {
		case client.send <- data:
			// успешно поставили в буфер клиента
		case <-ctx.Done():
			allAccepted = false
			observability.Warn(ctx, "SendToUser: context cancelled during send",
				"user_id", userID)
		default:
			allAccepted = false
			observability.Warn(ctx, "SendToUser: client send buffer full (backpressure). Client will NOT be unregistered.",
				"user_id", userID)
		}
	}

	if !allAccepted {
		return fmt.Errorf("not all clients accepted the message for user %s", userID)
	}
	return nil
}
func (m *Manager) HasActiveConnections(userID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	conns, ok := m.clients[userID]
	return ok && len(conns) > 0
}

func (c *Client) readPump(m *Manager) {
	defer m.Unregister(c.userID, c)

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		_, _, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
	}
}

func (c *Client) writePump(m *Manager) {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		m.Unregister(c.userID, c)
	}()

	for {
		select {
		case <-c.ctx.Done():
			return

		case message, ok := <-c.send:
			if !ok {
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
				return
			}

		case <-ticker.C:
			if err := c.conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(10*time.Second)); err != nil {
				return
			}
		}
	}
}

func (m *Manager) CloseAll() {
	if m.cancel != nil {
		m.cancel()
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for userID, conns := range m.clients {
		for client := range conns {
			client.once.Do(func() { close(client.send) })
			if client.cancel != nil {
				client.cancel()
			}
			client.conn.Close()
		}
		delete(m.clients, userID)
	}

	m.clients = make(map[string]map[*Client]bool)
	observability.Info(m.ctx, "WS All WebSocket connections closed")
}
