package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/moshdealer/notification-platform/pkg/config"
	"github.com/moshdealer/notification-platform/ws-notifier/internal/redis"
	"github.com/moshdealer/notification-platform/ws-notifier/internal/repository" // ← добавь
	"github.com/moshdealer/notification-platform/ws-notifier/internal/websocket"
	"github.com/nats-io/nats.go"
)

type Notification struct {
	NotificationID uint   `json:"notification_id"`
	UserID         string `json:"user_id"`
}

type Subscriber struct {
	natsConn    *nats.Conn
	sub         *nats.Subscription
	wsManager   *websocket.Manager
	redisClient *redis.Client
	repo        repository.OutBoxRepository // ← добавили
}

func NewSubscriber(
	cfg *config.NATSCfg,
	wm *websocket.Manager,
	rc *redis.Client,
	repo repository.OutBoxRepository, // ← добавили
) *Subscriber {
	nc, err := nats.Connect(cfg.NATSAddr)
	if err != nil {
		panic("NATS Subscriber connect error: " + err.Error())
	}

	return &Subscriber{
		natsConn:    nc,
		wsManager:   wm,
		redisClient: rc,
		repo:        repo,
	}
}

func (s *Subscriber) Start() error {
	sub, err := s.natsConn.QueueSubscribe("notifications.new.*", "notification-service-ws", func(msg *nats.Msg) {
		userID := strings.TrimPrefix(msg.Subject, "notifications.new.")

		// Извлекаем notification_id из сообщения
		eventID := extractEventID(msg.Data)

		if s.wsManager.HasActiveConnections(userID) {
			// Пользователь онлайн — пытаемся отправить
			if sendErr := s.wsManager.SendToUser(userID, msg.Data); sendErr == nil {
				fmt.Println("Зашли в отправку ", sendErr)
				// Успешно отправили
				if err := s.repo.MarkAsDelivered(context.Background(), eventID); err != nil {
					fmt.Printf("Failed to mark as sent %d: %v\n", eventID, err)
				}
			} else {
				// Не удалось отправить (хотя был онлайн) — сохраняем в Redis
				if addErr := s.redisClient.AddUnread(context.Background(), userID, msg.Data); addErr != nil {
					fmt.Printf("Failed to save to Redis: %v\n", addErr)
				}
			}
		} else {
			// Пользователь оффлайн — сразу в Redis
			if addErr := s.redisClient.AddUnread(context.Background(), userID, msg.Data); addErr != nil {
				fmt.Printf("Failed to save to Redis: %v\n", addErr)
			}
		}
	})

	if err != nil {
		return err
	}

	s.sub = sub
	return nil
}

func (s *Subscriber) Close() {
	if s.sub != nil {
		_ = s.sub.Unsubscribe()
		s.sub = nil
	}
	if s.natsConn != nil {
		s.natsConn.Close()
		s.natsConn = nil
	}
	fmt.Println("NATS Subscriber closed")
}

// TODO убрать дубль в хэндлере
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
