// internal/worker/outbox/syncer.go
package outbox

import (
	"context"
	"log/slog"
	"time"

	"github.com/moshdealer/notification-platform/notification-service/internal/repository"
	"github.com/moshdealer/notification-platform/pkg/model"
	"github.com/moshdealer/notification-platform/pkg/observability"
)

// TODO поправить вывод длительности операций

type Syncer struct {
	repo      repository.NotificationRepository
	interval  time.Duration
	batchSize int
}

func NewSyncer(repo repository.NotificationRepository, interval time.Duration, batchSize int) *Syncer {
	if interval == 0 {
		interval = 5 * time.Second
	}
	if batchSize == 0 {
		batchSize = 100
	}
	return &Syncer{
		repo:      repo,
		interval:  interval,
		batchSize: batchSize,
	}
}

func (s *Syncer) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	logger := observability.FromContext(ctx)
	logger.Info("OutboxSyncer started",
		slog.Duration("interval", s.interval),
		"batch", s.batchSize,
	)

	for {
		select {
		case <-ctx.Done():
			logger.Info("OutboxSyncer stopped")

			return
		case <-ticker.C:
			s.syncBatch(ctx)
		}
	}
}

func (s *Syncer) syncBatch(ctx context.Context) {
	start := time.Now()
	logger := observability.FromContext(ctx)

	// Получаем события, которые нужно синхронизировать
	events, err := s.repo.GetOutboxEventsForSync(context.Background(), s.batchSize)
	if err != nil {
		logger.Error("GetOutboxEventsForSync error", "error", err)
		return
	}

	if len(events) == 0 {
		return
	}

	syncedCount := 0

	for _, event := range events {
		var err error

		switch event.Status {
		case model.StatusSent: // в зависимости от твоих констант
			err = s.repo.MarkAsSent(ctx, event.NotificationID)

		case model.StatusDelivered:
			err = s.repo.MarkAsDelivered(ctx, event.NotificationID)

		case model.StatusRead: // в зависимости от твоих констант
			err = s.repo.MarkAsRead(ctx, event.NotificationID)

		case model.StatusFailed:
			err = s.repo.MarkAsFailed(ctx, event.NotificationID)

		case model.StatusExpired: // в зависимости от твоих констант
			err = s.repo.MarkAsExpired(ctx, event.NotificationID)

		case model.StatusWaiting:
			err = s.repo.MarkAsWaiting(ctx, event.NotificationID)

		}

		if err != nil {
			logger.Error("OutboxSyncer failed to sync notification",
				"notification_id", event.NotificationID, "error", err)
			continue
		}

		if markErr := s.repo.MarkOutboxAsSynced(context.Background(), event.ID); markErr != nil {
			logger.Error("OutboxSyncer failed to mark as synced",
				"event_id", event.ID, "error", markErr)
		} else {
			syncedCount++
		}
	}

	if syncedCount > 0 {
		logger.Info("OutboxSyncer successfully synced events",
			"count", syncedCount,
			slog.Duration("processing_time", time.Since(start)),
		)
	}
}
