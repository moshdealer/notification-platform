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

	observability.WebSocketConnectionsActive.Inc()
	go client.writePump(m)
	go client.readPump(m)

	observability.Info(clientCtx, "User connected", "user_id", userID)
	return client
}

func (m *Manager) Unregister(userID string, c *Client) {
	c.once.Do(func() {
		removed := false
		fullyDisconnected := false

		m.mu.Lock()

		if conns, ok := m.clients[userID]; ok {
			if _, exists := conns[c]; exists {
				delete(conns, c)
				removed = true
			}

			if len(conns) == 0 {
				delete(m.clients, userID)
				fullyDisconnected = true
			}
		}

		m.mu.Unlock()
		c.cancel()

		if err := c.conn.Close(); err != nil {
			observability.Debug(
				m.ctx,
				"WebSocket close error",
				"user_id", userID,
				"error", err,
			)
		}

		if removed {
			observability.WebSocketConnectionsActive.Dec()
		}

		if fullyDisconnected {
			observability.Info(
				m.ctx,
				"User fully disconnected",
				"user_id", userID,
			)
		}
	})
}

func (m *Manager) SendToUser(
	ctx context.Context,
	userID string,
	data []byte,
) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	conns, ok := m.clients[userID]
	if !ok || len(conns) == 0 {
		return fmt.Errorf(
			"no active connections for user %s",
			userID,
		)
	}

	allAccepted := true

	for client := range conns {
		if err := ctx.Err(); err != nil {
			allAccepted = false
			break
		}

		if err := client.ctx.Err(); err != nil {
			allAccepted = false
			continue
		}

		select {
		case client.send <- data:
		default:
			allAccepted = false

			observability.Warn(
				ctx,
				"WebSocket client send buffer full",
				"user_id", userID,
			)
		}
	}

	if !allAccepted {
		return fmt.Errorf(
			"not all clients accepted message for user %s",
			userID,
		)
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

		case message := <-c.send:
			if err := c.conn.WriteMessage(
				websocket.TextMessage,
				message,
			); err != nil {
				return
			}

		case <-ticker.C:
			if err := c.conn.WriteControl(
				websocket.PingMessage,
				nil,
				time.Now().Add(10*time.Second),
			); err != nil {
				return
			}
		}
	}
}

func (m *Manager) CloseAll() {
	m.cancel()

	m.mu.RLock()

	clients := make([]*Client, 0)

	for _, conns := range m.clients {
		for client := range conns {
			clients = append(clients, client)
		}
	}

	m.mu.RUnlock()

	for _, client := range clients {
		m.Unregister(client.userID, client)
	}

	observability.Info(
		m.ctx,
		"All WebSocket connections closed",
	)
}
