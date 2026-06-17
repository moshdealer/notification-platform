// internal/service/notification_service.go
package service

import (
	"context"
	"fmt"
	"time"

	"github.com/moshdealer/notification-platform/notification-service/internal/nats"
	"github.com/moshdealer/notification-platform/notification-service/internal/repository"
	"github.com/moshdealer/notification-platform/pkg/model"
	"github.com/moshdealer/notification-platform/pkg/observability"
)

// NotificationService - бизнес-логика уведомлений
type NotificationService struct {
	repo      repository.NotificationRepository
	publisher *nats.Publisher
}

// NewNotificationService - конструктор (Dependency Injection)
func NewNotificationService(
	repo repository.NotificationRepository,
	publisher *nats.Publisher,
) *NotificationService {
	return &NotificationService{
		repo:      repo,
		publisher: publisher,
	}
}

// Create - основной сценарий: создание уведомления
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

	observability.NotificationsCreatedTotal.WithLabelValues(n.Type, n.Priority).Inc()
	observability.OutboxEventsTotal.WithLabelValues(e.Priority).Inc()

	return nil
}

// MarkAsRead - обратный поток (статус "прочитано")
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

// StartOutboxDispatcher - запускает указанное кол-во воркеров
func (s *NotificationService) StartOutboxDispatcher(ctx context.Context, workerCount int) {
	logger := observability.FromContext(ctx)
	logger.Info("Outbox Dispatcher starting", "worker_count", workerCount)

	for i := 0; i < workerCount; i++ {
		go s.runOutboxWorker(ctx, i)
	}
}

// runOutboxWorker - логика работы одного воркера (непрерывный цикл)
func (s *NotificationService) runOutboxWorker(ctx context.Context, workerID int) {
	logger := observability.FromContext(ctx).With("worker_id", workerID)

	const (
		minBatchSize     = 40
		maxBatchSize     = 500
		basePollInterval = 350 * time.Millisecond
		maxPollInterval  = 1500 * time.Millisecond
		errorBackoff     = 800 * time.Millisecond
	)

	pollInterval := basePollInterval
	batchSize := minBatchSize

	logger.Info("Outbox worker started")

	for {
		select {
		case <-ctx.Done():
			logger.Info("Outbox worker stopped")
			return
		default:
		}

		start := time.Now()

		events, err := s.repo.ClaimPendingOutboxEvents(ctx, batchSize)
		claimDuration := time.Since(start)

		if err != nil {
			logger.Error("Failed to claim outbox events", "error", err)
			time.Sleep(errorBackoff)
			continue
		}

		if len(events) == 0 {
			time.Sleep(pollInterval)

			if pollInterval < maxPollInterval {
				pollInterval += 50 * time.Millisecond
			}
			batchSize = minBatchSize
			continue
		}

		// Есть работа - сбрасываем интервал на базовый
		pollInterval = basePollInterval

		// Обработка выборки
		published := 0

		for _, event := range events {
			pubStart := time.Now()

			natsMessage := model.NatsMessage{
				EventID: event.ID,
				Payload: event.Payload,
			}

			// Создаём отдельный контекст с таймаутом именно для этой публикации
			pubCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

			err := s.publisher.Publish(pubCtx, event.UserID, natsMessage)

			if err != nil {
				logger.Warn("Failed to publish event to NATS",
					"event_id", event.ID,
					"error", err,
				)
				cancel()
				continue
			}

			// Метрика времени одного Publish
			observability.NatsPublishDuration.
				WithLabelValues(fmt.Sprintf("%d", workerID)).
				Observe(time.Since(pubStart).Seconds())

			if err := s.repo.IncrementSendAttempts(ctx, event.ID); err != nil {
				logger.Debug("Failed to increment send attempts", "event_id", event.ID, "error", err)
			}

			published++
			cancel()
		}

		totalDuration := time.Since(start)

		// Логируем только значимые батчи
		if len(events) >= 10 || totalDuration > 500*time.Millisecond {
			logger.Info("Outbox batch processed",
				"claimed", len(events),
				"published", published,
				"claim_ms", claimDuration.Milliseconds(),
				"total_ms", totalDuration.Milliseconds(),
				"batch_size", batchSize,
			)
		}

		// Адаптивно увеличиваем размер батча
		if published > 0 && batchSize < maxBatchSize {
			batchSize += 15
			if batchSize > maxBatchSize {
				batchSize = maxBatchSize
			}
		}
	}
}
