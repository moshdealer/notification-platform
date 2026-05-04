package websocket

import (
	"fmt"
	"sync"

	"github.com/gorilla/websocket"
)

type Client struct {
	conn   *websocket.Conn
	userID string
	send   chan []byte
	once   sync.Once
}

type Manager struct {
	clients map[string]map[*Client]bool
	mu      sync.RWMutex
}

func NewManager() *Manager {
	return &Manager{
		clients: make(map[string]map[*Client]bool),
	}
}

func (m *Manager) Register(userID string, conn *websocket.Conn) *Client {
	client := &Client{
		conn:   conn,
		userID: userID,
		send:   make(chan []byte, 256),
	}

	m.mu.Lock()
	if _, ok := m.clients[userID]; !ok {
		m.clients[userID] = make(map[*Client]bool)
	}
	m.clients[userID][client] = true
	m.mu.Unlock()

	go client.writePump(m)
	go client.readPump(m)

	fmt.Printf("[WS] User %s connected\n", userID)
	return client
}

func (m *Manager) Unregister(userID string, c *Client) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if conns, ok := m.clients[userID]; ok {
		delete(conns, c)
		if len(conns) == 0 {
			delete(m.clients, userID)
			fmt.Printf("[WS] User %s fully disconnected\n", userID)
		}
	}

	c.once.Do(func() {
		close(c.send)
	})
	c.conn.Close()
}

func (m *Manager) SendToUser(userID string, data []byte) error {
	m.mu.RLock()
	conns, ok := m.clients[userID]
	m.mu.RUnlock()

	if !ok || len(conns) == 0 {
		return fmt.Errorf("no active connections for user %s", userID)
	}

	var lastErr error
	for client := range conns {
		select {
		case client.send <- data:
			lastErr = nil
		default:
			lastErr = fmt.Errorf("send channel full or closed")
			go m.Unregister(userID, client)
		}
	}
	return lastErr
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
		_, _, err := c.conn.ReadMessage()
		if err != nil {
			// клиент отключился или произошла ошибка
			return // defer вызовет Unregister
		}
	}
}

func (c *Client) writePump(m *Manager) {
	defer m.Unregister(c.userID, c)

	for message := range c.send {
		if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
			return // defer сделает cleanup
		}
	}
}

func (m *Manager) CloseAll() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for userID, conns := range m.clients {
		for client := range conns {
			client.once.Do(func() {
				close(client.send)
			})
			client.conn.Close()
		}
		delete(m.clients, userID)
	}

	m.clients = make(map[string]map[*Client]bool)

	fmt.Println("[WS] All WebSocket connections closed")
}
