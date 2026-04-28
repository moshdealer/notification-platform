// internal/service/notification_service.go
package service

import (
	"context"
	"fmt"
	"time"

	"github.com/moshdealer/notification-service/internal/model"
	"github.com/moshdealer/notification-service/internal/nats"
	"github.com/moshdealer/notification-service/internal/repository"
)

// NotificationService — бизнес-логика уведомлений
type NotificationService struct {
	repo      repository.NotificationRepository
	publisher *nats.Publisher
	//wsManager websocket.Manager
}

// NewNotificationService — конструктор (Dependency Injection)
func NewNotificationService(
	repo repository.NotificationRepository,
	publisher *nats.Publisher,
	// wsManager websocket.Manager,
) *NotificationService {
	return &NotificationService{
		repo:      repo,
		publisher: publisher,
		//wsManager: wsManager,
	}
}

// Create — основной сценарий: создание уведомления
func (s *NotificationService) Create(ctx context.Context, n *model.Notification, e *model.OutboxEvent) error {
	// 1. Сохраняем в PostgreSQL
	if err := s.repo.Create(ctx, n, e); err != nil {
		return err
	}
	return nil
}

// MarkAsRead — обратный поток (статус "прочитано")
func (s *NotificationService) MarkAsRead(ctx context.Context, id uint, userID string) error {
	if err := s.repo.MarkAsRead(ctx, id, userID); err != nil {
		return err
	}

	readEvent := map[string]any{
		"notification_id": id,
		"user_id":         userID,
		"read":            true,
	}
	fmt.Println(readEvent)
	return nil
}
func (s *NotificationService) StartOutboxDispatcher(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	fmt.Println("Outbox Dispatcher started (every 5 seconds)")

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Outbox Dispatcher stopped")
			return

		case <-ticker.C:
			events, err := s.repo.GetPendingOutboxEvents(ctx, 10)
			if err != nil {
				fmt.Printf("Failed to get pending outbox events: %v\n", err)
				continue
			}

			for _, event := range events {
				payload := map[string]any{
					"event_id": event.ID,
					"payload":  event.Payload,
				}

				if err := s.publisher.Publish(ctx, event.UserID, payload); err == nil {
					if markErr := s.repo.MarkOutboxAsSent(ctx, event.ID); markErr == nil {
						fmt.Printf("Published outbox event %d to NATS\n", event.ID)
					} else {
						fmt.Printf("Failed to mark event %d as sent: %v\n", event.ID, markErr)
					}
				} else {
					fmt.Printf("Failed to publish event %d to NATS: %v\n", event.ID, err)
				}
			}
		}
	}
}
