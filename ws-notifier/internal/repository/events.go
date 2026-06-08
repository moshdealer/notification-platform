package repository

import (
	"context"
	"gorm.io/gorm"
	"time"

	"github.com/moshdealer/notification-platform/pkg/model"
)

type OutBoxRepository interface {
	MarkAsSent(ctx context.Context, id uint) error
	MarkAsDelivered(ctx context.Context, id uint) error
	MarkAsRead(ctx context.Context, id uint) error
	MarkAsExpired(ctx context.Context, id uint) error
	MarkAsFailed(ctx context.Context, id uint) error
	MarkAsWaiting(ctx context.Context, id uint, ttl time.Duration) error
}

type outboxRepo struct {
	db *gorm.DB
}

func NewOutBoxRepository(db *gorm.DB) OutBoxRepository {
	return &outboxRepo{db: db}
}

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
