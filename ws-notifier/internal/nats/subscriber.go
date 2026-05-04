package nats

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/moshdealer/notification-platform/pkg/config"
	"github.com/moshdealer/notification-platform/pkg/utils"
	"github.com/moshdealer/notification-platform/ws-notifier/internal/redis"
	"github.com/moshdealer/notification-platform/ws-notifier/internal/repository"
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
	repo        repository.OutBoxRepository
}

func NewSubscriber(
	cfg *config.NATSCfg,
	wm *websocket.Manager,
	rc *redis.Client,
	repo repository.OutBoxRepository,
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

		eventID := utils.ExtractEventID(msg.Data)
		priority := utils.ExtractPriority(msg.Data)

		var ttl time.Duration
		if priority == "high" {
			ttl = redis.HighTTL
		} else {
			ttl = redis.DefaultTTL
		}

		if s.wsManager.HasActiveConnections(userID) {
			// Пользователь онлайн — пытаемся отправить
			if sendErr := s.wsManager.SendToUser(userID, msg.Data); sendErr == nil {
				// Успешно отправили
				if err := s.repo.MarkAsDelivered(context.Background(), eventID); err != nil {
					fmt.Printf("Failed to mark as sent %d: %v\n", eventID, err)
				}
			} else {
				// Не удалось отправить - сохраняем в Redis
				if addErr := s.redisClient.AddUnread(context.Background(), userID, msg.Data); addErr != nil {
					fmt.Printf("Failed to save to Redis: %v\n", addErr)
					if err := s.repo.MarkAsFailed(context.Background(), eventID); err != nil {
						fmt.Printf("Failed to mark as failed %d: %v\n", eventID, err)
					}
				} else {
					if err := s.repo.MarkAsWaiting(context.Background(), eventID, ttl); err != nil {
						fmt.Printf("Failed to mark as waiting %d: %v\n", eventID, err)
					}
				}
			}
		} else {
			// Пользователь оффлайн - сразу в Redis
			if addErr := s.redisClient.AddUnread(context.Background(), userID, msg.Data); addErr != nil {
				fmt.Printf("Failed to save to Redis: %v\n", addErr)
				if err := s.repo.MarkAsFailed(context.Background(), eventID); err != nil {
					fmt.Printf("Failed to mark as failed %d: %v\n", eventID, err)
				}
			} else {
				if err := s.repo.MarkAsWaiting(context.Background(), eventID, ttl); err != nil {
					fmt.Printf("Failed to mark as waiting %d: %v\n", eventID, err)
				}
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
