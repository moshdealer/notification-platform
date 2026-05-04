package repository

import (
	"context"
	"gorm.io/gorm"
	"time"

	"github.com/moshdealer/notification-platform/pkg/model"
)

type OutBoxRepository interface {
	//Create(ctx context.Context, n *model.Notification, e *model.OutboxEvent) error
	//GetByID(ctx context.Context, id uint) (*model.Notification, error)
	MarkAsSent(ctx context.Context, id uint) error
	MarkAsDelivered(ctx context.Context, id uint) error
	MarkAsRead(ctx context.Context, id uint) error
	MarkAsExpired(ctx context.Context, id uint) error
	MarkAsFailed(ctx context.Context, id uint) error
	MarkAsWaiting(ctx context.Context, id uint, ttl time.Duration) error
	//GetPendingOutboxEvents(ctx context.Context, limit int) ([]model.OutboxEvent, error)
	//MarkOutboxAsSent(ctx context.Context, id uint) error
	//GetOutboxEventsForSync(ctx context.Context, limit int) ([]model.OutboxEvent, error)
	//MarkOutboxAsSynced(ctx context.Context, outboxID uint) error
}

type outboxRepo struct {
	db *gorm.DB
}

func NewOutBoxRepository(db *gorm.DB) OutBoxRepository {
	return &outboxRepo{db: db}
}

/*
	func (r *outboxRepo) Create(ctx context.Context, n *model.Notification, e *model.OutboxEvent) error {
		return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			if err := tx.Create(n).Error; err != nil {
				return err
			}

			e.NotificationID = &n.ID
			e.UserID = n.UserID
			e.Priority = n.Priority

			if e.Payload == nil {
				e.Payload = make(map[string]any)
			}
			e.Payload = map[string]any{
				"notification_id": n.ID,
				"user_id":         n.UserID,
				"title":           n.Title,
				"body":            n.Body,
				"type":            n.Type,
				"priority":        n.Priority,
				"data":            n.Data,
				"created_at":      n.CreatedAt,
			}

			if err := tx.Create(&e).Error; err != nil {
				return err
			}

			return nil
		})
	}

	func (r *outboxRepo) GetByID(ctx context.Context, id uint) (*model.Notification, error) {
		var n model.Notification
		err := r.db.WithContext(ctx).First(&n, id).Error
		if err != nil {
			return nil, err
		}
		return &n, nil
	}
*/
func (r *outboxRepo) MarkAsSent(ctx context.Context, id uint) error {
	return r.db.WithContext(ctx).
		Model(&model.OutboxEvent{}).
		Where("id = ?", id).
		Updates(map[string]any{
			"status":       model.StatusSent,
			"updated_at":   time.Now(),
			"need_to_sync": true,
		}).Error
}

func (r *outboxRepo) MarkAsDelivered(ctx context.Context, id uint) error {
	return r.db.WithContext(ctx).
		Model(&model.OutboxEvent{}).
		Where("id = ?", id).
		Updates(map[string]any{
			"status":       model.StatusDelivered,
			"updated_at":   time.Now(),
			"need_to_sync": true,
		}).Error
}

func (r *outboxRepo) MarkAsRead(ctx context.Context, id uint) error {
	return r.db.WithContext(ctx).
		Model(&model.OutboxEvent{}).
		Where("id = ?", id).
		Updates(map[string]any{
			"status":       model.StatusRead,
			"read":         true,
			"updated_at":   time.Now(),
			"need_to_sync": true,
		}).Error
}

func (r *outboxRepo) MarkAsExpired(ctx context.Context, id uint) error {
	return r.db.WithContext(ctx).
		Model(&model.OutboxEvent{}).
		Where("id = ?", id).
		Updates(map[string]any{
			"status":       model.StatusExpired,
			"updated_at":   time.Now(),
			"need_to_sync": true,
		}).Error
}

func (r *outboxRepo) MarkAsFailed(ctx context.Context, id uint) error {
	return r.db.WithContext(ctx).
		Model(&model.OutboxEvent{}).
		Where("id = ?", id).
		Updates(map[string]any{
			"status":       model.StatusFailed,
			"updated_at":   time.Now(),
			"need_to_sync": true,
		}).Error
}

func (r *outboxRepo) MarkAsWaiting(ctx context.Context, id uint, ttl time.Duration) error {
	now := time.Now()

	updates := map[string]any{
		"status":     model.StatusWaiting,
		"updated_at": now,
	}

	if ttl > 0 {
		updates["expired_at"] = now.Add(ttl)
	} else {
		// Для high-priority — никогда не истекает
		updates["expired_at"] = nil
	}

	return r.db.WithContext(ctx).
		Model(&model.OutboxEvent{}).
		Where("id = ?", id).
		Updates(updates).Error
}

/*
// GetPendingOutboxEvents возвращает события, которые ещё не отправлены в NATS
func (r *outboxRepo) GetPendingOutboxEvents(ctx context.Context, limit int) ([]model.OutboxEvent, error) {
	var events []model.OutboxEvent

	err := r.db.WithContext(ctx).
		Where("status = ?", model.StatusPending).
		Order("created_at ASC").
		Limit(limit).
		Find(&events).Error

	if err != nil {
		return nil, fmt.Errorf("failed to get pending outbox events: %w", err)
	}

	return events, nil
}

// MarkOutboxAsSent помечает событие как успешно отправленное
func (r *outboxRepo) MarkOutboxAsSent(ctx context.Context, id uint) error {
	err := r.db.WithContext(ctx).
		Model(&model.OutboxEvent{}).
		Where("id = ?", id).
		Updates(map[string]any{
			"status":       model.StatusSent,
			"updated_at":   time.Now(),
			"need_to_sync": true,
		}).Error

	if err != nil {
		return fmt.Errorf("failed to mark outbox event as sent: %w", err)
	}

	return nil
}

func (r *outboxRepo) GetOutboxEventsForSync(ctx context.Context, limit int) ([]model.OutboxEvent, error) {
	var events []model.OutboxEvent

	err := r.db.WithContext(ctx).
		Where("need_to_sync = true").
		Order("updated_at ASC").
		Limit(limit).
		Find(&events).Error

	if err != nil {
		return nil, fmt.Errorf("failed to get outbox events for sync: %w", err)
	}
	return events, nil
}

func (r *outboxRepo) MarkOutboxAsSynced(ctx context.Context, outboxID uint) error {
	return r.db.WithContext(ctx).
		Model(&model.OutboxEvent{}).
		Where("id = ?", outboxID).
		Update("need_to_sync", false).Error
}
*/
