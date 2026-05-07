// internal/worker/outbox/syncer.go
package outbox

import (
	"context"
	"log"
	"time"

	"github.com/moshdealer/notification-platform/notification-service/internal/repository"
	"github.com/moshdealer/notification-platform/pkg/model"
)

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

	log.Printf("[OutboxSyncer] started, interval=%v, batch=%d", s.interval, s.batchSize)

	for {
		select {
		case <-ctx.Done():
			log.Println("[OutboxSyncer] stopped")
			return
		case <-ticker.C:
			s.syncBatch()
		}
	}
}

func (s *Syncer) syncBatch() {
	start := time.Now()

	// Получаем события, которые нужно синхронизировать
	events, err := s.repo.GetOutboxEventsForSync(context.Background(), s.batchSize)
	if err != nil {
		log.Printf("[OutboxSyncer] GetOutboxEventsForSync error: %v", err)
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
			err = s.repo.MarkAsSent(context.Background(), event.NotificationID)

		case model.StatusDelivered:
			err = s.repo.MarkAsDelivered(context.Background(), event.NotificationID)

		case model.StatusRead: // в зависимости от твоих констант
			err = s.repo.MarkAsRead(context.Background(), event.NotificationID)

		case model.StatusFailed:
			err = s.repo.MarkAsFailed(context.Background(), event.NotificationID)

		case model.StatusExpired: // в зависимости от твоих констант
			err = s.repo.MarkAsExpired(context.Background(), event.NotificationID)

		case model.StatusWaiting:
			err = s.repo.MarkAsWaiting(context.Background(), event.NotificationID)

		}

		if err != nil {
			log.Printf("[OutboxSyncer] failed to sync notification %d: %v", event.NotificationID, err)
			continue
		}

		if markErr := s.repo.MarkOutboxAsSynced(context.Background(), event.ID); markErr != nil {
			log.Printf("[OutboxSyncer] failed to mark as synced %d: %v", event.ID, markErr)
		} else {
			syncedCount++
		}
	}

	if syncedCount > 0 {
		log.Printf("[OutboxSyncer] successfully synced %d events in %v", syncedCount, time.Since(start))
	}
}
