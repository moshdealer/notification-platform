// internal/service/notification_service.go
package service

import (
	"context"
	"time"

	"github.com/moshdealer/notification-platform/notification-service/internal/nats"
	"github.com/moshdealer/notification-platform/notification-service/internal/repository"
	"github.com/moshdealer/notification-platform/pkg/model"
	"github.com/moshdealer/notification-platform/pkg/observability"
)

// NotificationService — бизнес-логика уведомлений
type NotificationService struct {
	repo      repository.NotificationRepository
	publisher *nats.Publisher
}

// NewNotificationService — конструктор (Dependency Injection)
func NewNotificationService(
	repo repository.NotificationRepository,
	publisher *nats.Publisher,
) *NotificationService {
	return &NotificationService{
		repo:      repo,
		publisher: publisher,
	}
}

// Create — основной сценарий: создание уведомления
func (s *NotificationService) Create(ctx context.Context, n *model.Notification, e *model.OutboxEvent) error {
	// 1. Сохраняем в PostgreSQL
	logger := observability.FromContext(ctx)

	logger.Info("creating notification",
		"user_id", n.UserID,
		"type", n.Type,
		"priority", n.Priority,
		"title", n.Title,
	)

	if err := s.repo.Create(ctx, n, e); err != nil {
		logger.Error("failed to create notification in database",
			"error", err,
			"user_id", n.UserID,
		)
		return err
	}

	logger.Info("notification created successfully",
		"notification_id", n.ID,
		"user_id", n.UserID,
	)

	return nil
}

// MarkAsRead — обратный поток (статус "прочитано")
func (s *NotificationService) MarkAsRead(ctx context.Context, id uint) error {
	logger := observability.FromContext(ctx)

	if err := s.repo.MarkAsRead(ctx, id); err != nil {
		logger.Error("failed to mark notification as read",
			"error", err,
			"notification_id", id,
		)
		return err
	}
	logger.Debug("notification marked as read", "notification_id", id)

	return nil
}
func (s *NotificationService) StartOutboxDispatcher(ctx context.Context) {
	//TODO в конфиг вынести
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	logger := observability.FromContext(ctx)
	logger.Info("Outbox Dispatcher started", "interval", "5s")

	for {
		select {
		case <-ctx.Done():
			logger.Info("Outbox Dispatcher stopped")
			return

		case <-ticker.C:
			events, err := s.repo.GetPendingOutboxEvents(ctx, 10)
			if err != nil {
				logger.Error("Failed to get pending outbox events:", "error", err)
				continue
			}

			for _, event := range events {
				eventLogger := logger.With("event_id", event.ID)

				natsMessage := model.NatsMessage{
					EventID: event.ID,
					Payload: event.Payload,
				}

				if err := s.publisher.Publish(ctx, event.UserID, natsMessage); err == nil {
					if markErr := s.repo.MarkOutboxAsSent(ctx, event.ID); markErr == nil {
						eventLogger.Info("Outbox event published to NATS")
					} else {
						eventLogger.Error("failed to mark outbox event as sent", "error", markErr)
					}
				} else {
					eventLogger.Error("Failed to publish event to NATS", "error", err)
				}
			}
		}
	}
}
