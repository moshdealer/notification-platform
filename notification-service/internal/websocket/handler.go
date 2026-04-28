package websocket

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/moshdealer/notification-platform/notification-service/internal/redis"
)

type Handler struct {
	wsManager   *Manager
	redisClient *redis.Client
	//	repo        repository.NotificationRepository
}

func NewHandler(wsManager *Manager, redisClient *redis.Client) *Handler {
	return &Handler{
		wsManager:   wsManager,
		redisClient: redisClient,
	}
}

func (h *Handler) WebSocket(c *gin.Context) {
	userID := c.Query("user_id")
	token := c.Query("token")

	if userID == "" || !h.validateToken(token, userID) {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return
	}

	upgrader := websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin:     func(r *http.Request) bool { return true },
	}

	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		return
	}

	h.wsManager.Register(userID, conn)

	// Приветственное сообщение
	h.wsManager.SendToUser(userID, []byte(`{"type":"connected"}`))

	// Догружаем unread из Redis
	unread, err := h.redisClient.GetUnread(c.Request.Context(), userID)
	if err == nil && len(unread) > 0 {
		for _, data := range unread {
			h.wsManager.SendToUser(userID, data)
		}
		// Очищаем после отправки
		go h.redisClient.ClearUnread(c.Request.Context(), userID)
	}
}

func (h *Handler) validateToken(token, userID string) bool {
	return token != "" // заглушка
}
