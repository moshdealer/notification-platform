package websocket

import (
	"encoding/json"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/moshdealer/notification-platform/pkg/model"
	"github.com/moshdealer/notification-platform/pkg/observability"
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

	wsCtx := c.Request.Context()

	if err = h.wsManager.SendToUser(wsCtx, userID, []byte(`{"type":"connected"}`)); err != nil {
		observability.Error(wsCtx, "Failed to send connected message",
			"user_id", userID,
			"error", err,
		)
	}

	// Догружаем unread
	unread, err := h.redisClient.GetUnread(wsCtx, userID)
	if err == nil && len(unread) > 0 {
		for _, data := range unread {
			natsMessage := model.NatsMessage{}
			if err := json.Unmarshal(data, &natsMessage); err != nil {
				observability.Error(wsCtx, "Failed to unmarshal message NatsEvent",
					"user_id", userID,
					"error", err,
				)
			}
			eventID := natsMessage.EventID

			if sendErr := h.wsManager.SendToUser(wsCtx, userID, data); sendErr == nil {
				// Успешно отправили
				if markErr := h.repo.MarkAsDelivered(wsCtx, eventID); markErr != nil {
					observability.Error(wsCtx, "failed to mark delivered notification",
						"event_id", eventID,
						"user_id", userID,
						"error", markErr,
					)
				}
				observability.Info(wsCtx, "Notification sent from Redis cache",
					"event_id", eventID,
					"user_id", userID,
				)
				observability.NotificationsDeliveredTotal.WithLabelValues("delivered").Inc()
			} else {
				// Не удалось отправить
				if markErr := h.repo.MarkAsFailed(wsCtx, eventID); markErr != nil {
					observability.Error(wsCtx, "Failed to mark notification as failed",
						"event_id", eventID,
						"user_id", userID,
						"error", markErr,
					)
				}
			}
		}
	}
	// Очищаем после отправки
	go h.redisClient.ClearAllUnread(c.Request.Context(), userID)
}

func (h *Handler) validateToken(token, userID string) bool {
	return token != "" // заглушка
}
