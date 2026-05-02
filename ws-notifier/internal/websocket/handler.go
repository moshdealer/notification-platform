package websocket

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/moshdealer/notification-platform/ws-notifier/internal/redis"
	"github.com/moshdealer/notification-platform/ws-notifier/internal/repository"
)

type Handler struct {
	wsManager   *Manager
	redisClient *redis.Client
	repo        repository.OutBoxRepository
}

func NewHandler(wsManager *Manager, redisClient *redis.Client, repo repository.OutBoxRepository) *Handler {
	return &Handler{
		wsManager:   wsManager,
		redisClient: redisClient,
		repo:        repo,
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
	if err = h.wsManager.SendToUser(userID, []byte(`{"type":"connected"}`)); err != nil {
		fmt.Println("Failed to send message: ", err)
	}

	ctx := c.Request.Context()

	// Догружаем unread
	unread, err := h.redisClient.GetUnread(ctx, userID)
	if err == nil && len(unread) > 0 {
		for _, data := range unread {
			eventID := extractEventID(data)

			if sendErr := h.wsManager.SendToUser(userID, data); sendErr == nil {
				// Успешно отправили
				if markErr := h.repo.MarkAsDelivered(ctx, eventID); markErr != nil {
					fmt.Printf("failed to mark delivered notification %d: %v", eventID, markErr)
				}
			} else {
				// Не удалось отправить
				if markErr := h.repo.MarkAsFailed(ctx, eventID); markErr != nil {
					fmt.Printf("failed to mark failed notification %d: %v", eventID, markErr)
				}
			}
		}
	}
	// Очищаем после отправки
	go h.redisClient.ClearUnread(c.Request.Context(), userID)
}

func (h *Handler) validateToken(token, userID string) bool {
	return token != "" // заглушка
}

// В nats/subscriber.go и в websocket/handler.go (можно вынести в общий utils)
func extractEventID(data []byte) uint {
	type outer struct {
		EventID uint `json:"event_id"`
	}

	var o outer
	if err := json.Unmarshal(data, &o); err != nil {
		fmt.Printf("ERROR: failed to parse event_id. Raw: %s\n", string(data))
		return 0
	}

	if o.EventID != 0 {
		return o.EventID
	}

	fmt.Printf("WARNING: event_id not found or zero in message: %s\n", string(data))
	return 0
}
