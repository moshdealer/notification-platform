package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/moshdealer/notification-service/internal/config"
	"github.com/moshdealer/notification-service/internal/redis"
	"github.com/moshdealer/notification-service/internal/websocket"
	"github.com/nats-io/nats.go"
)

// Notification — минимальная структура для парсинга (можно расширить)
type Notification struct {
	Priority string `json:"priority"`
	UserID   string `json:"user_id,omitempty"`
}

type Subscriber struct {
	natsConn    *nats.Conn
	wsManager   *websocket.Manager
	redisClient *redis.Client
}

func NewSubscriber(cfg *config.NATSCfg, wm *websocket.Manager, rc *redis.Client) *Subscriber {
	nc, err := nats.Connect(cfg.NATSAddr)
	if err != nil {
		panic("NATS Subscriber connect error: " + err.Error())
	}

	return &Subscriber{
		natsConn:    nc,
		wsManager:   wm,
		redisClient: rc,
	}
}

func (s *Subscriber) Start() error {
	_, err := s.natsConn.QueueSubscribe("notifications.new.*", "notification-service-ws", func(msg *nats.Msg) {
		userID := strings.TrimPrefix(msg.Subject, "notifications.new.")

		s.wsManager.SendToUser(userID, msg.Data)

		// Если клиента НЕТ онлайн — сохраняем в Redis
		if !s.wsManager.HasActiveConnections(userID) {
			if addErr := s.redisClient.AddUnread(context.Background(), userID, msg.Data); addErr != nil {
				fmt.Printf("Failed to save to Redis: %v\n", addErr)
			} else {
				fmt.Printf("Saved to Redis for offline user %s\n", userID)
			}
		}
	})
	return err
}

// saveToRedisIfOffline — отдельный метод, если пользователь офлайн
func (s *Subscriber) saveToRedisIfOffline(msg *nats.Msg, userID string) error {
	var notif Notification

	if err := json.Unmarshal(msg.Data, &notif); err != nil {
		return s.redisClient.AddUnread(context.Background(), userID, msg.Data)
	}

	return s.redisClient.AddUnread(
		context.Background(),
		userID,
		msg.Data,
	)
}
