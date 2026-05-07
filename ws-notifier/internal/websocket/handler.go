package websocket

import (
	"encoding/json"
	"fmt"
	"github.com/moshdealer/notification-platform/pkg/model"
	"log"
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

	if err = h.wsManager.SendToUser(userID, []byte(`{"type":"connected"}`)); err != nil {
		fmt.Println("Failed to send message: ", err)
	}

	ctx := c.Request.Context()

	// Догружаем unread
	unread, err := h.redisClient.GetUnread(ctx, userID)
	if err == nil && len(unread) > 0 {
		for _, data := range unread {
			natsMessage := model.NatsMessage{}
			if err := json.Unmarshal(data, &natsMessage); err != nil {
				log.Printf("Failed to unmarshal NatsEvent: %v", err)
			}
			eventID := natsMessage.EventID

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
