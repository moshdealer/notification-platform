package repository

import (
	"context"
	"fmt"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"time"

	"github.com/moshdealer/notification-platform/pkg/model"
)

type NotificationRepository interface {
	Create(ctx context.Context, n *model.Notification, e *model.OutboxEvent) error
	GetByID(ctx context.Context, id uint) (*model.Notification, error)
	MarkAsSent(ctx context.Context, id uint) error
	MarkAsDelivered(ctx context.Context, id uint) error
	MarkAsRead(ctx context.Context, id uint) error
	MarkAsExpired(ctx context.Context, id uint) error
	MarkAsWaiting(ctx context.Context, id uint) error
	MarkAsFailed(ctx context.Context, id uint) error
	MarkAsPending(ctx context.Context, id uint) error
	GetPendingOutboxEvents(ctx context.Context, limit int) ([]model.OutboxEvent, error)
	ClaimPendingOutboxEvents(ctx context.Context, limit int) ([]model.OutboxEvent, error)
	MarkOutboxAsSent(ctx context.Context, id uint) error
	GetOutboxEventsForSync(ctx context.Context, limit int) ([]model.OutboxEvent, error)
	MarkOutboxAsSynced(ctx context.Context, outboxID uint) error
	IncrementSendAttempts(ctx context.Context, id uint) error
}

type notificationRepo struct {
	db *gorm.DB
}

func NewNotificationRepository(db *gorm.DB) NotificationRepository {
	return &notificationRepo{db: db}
}

func (r *notificationRepo) Create(ctx context.Context, n *model.Notification, e *model.OutboxEvent) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(n).Error; err != nil {
			return err
		}

		e.NotificationID = n.ID
		e.UserID = n.UserID
		e.Priority = n.Priority

		e.Payload = model.NatsPayload{
			NotificationID: n.ID,
			UserID:         n.UserID,
			Title:          n.Title,
			Body:           n.Body,
			Type:           n.Type,
			Priority:       n.Priority,
			Data:           n.Data,
			CreatedAt:      n.CreatedAt,
		}

		if err := tx.Create(&e).Error; err != nil {
			return err
		}

		return nil
	})
}

func (r *notificationRepo) GetByID(ctx context.Context, id uint) (*model.Notification, error) {
	var n model.Notification
	err := r.db.WithContext(ctx).First(&n, id).Error
	if err != nil {
		return nil, err
	}
	return &n, nil
}

func (r *notificationRepo) MarkAsSent(ctx context.Context, id uint) error {
	return r.db.WithContext(ctx).
		Model(&model.Notification{}).
		Where("id = ?", id).
		Updates(map[string]any{
			"status":     model.StatusSent,
			"updated_at": time.Now(),
		}).Error
}

func (r *notificationRepo) MarkAsDelivered(ctx context.Context, id uint) error {
	return r.db.WithContext(ctx).
		Model(&model.Notification{}).
		Where("id = ?", id).
		Updates(map[string]any{
			"status":     model.StatusDelivered,
			"updated_at": time.Now(),
		}).Error
}

func (r *notificationRepo) MarkAsRead(ctx context.Context, id uint) error {
	return r.db.WithContext(ctx).
		Model(&model.Notification{}).
		Where("id = ?", id).
		Updates(map[string]any{
			"status":     model.StatusRead,
			"read":       true,
			"updated_at": time.Now(),
		}).Error
}

func (r *notificationRepo) MarkAsExpired(ctx context.Context, id uint) error {
	return r.db.WithContext(ctx).
		Model(&model.Notification{}).
		Where("id = ?", id).
		Updates(map[string]any{
			"status":     model.StatusExpired,
			"updated_at": time.Now(),
		}).Error
}

func (r *notificationRepo) MarkAsFailed(ctx context.Context, id uint) error {
	return r.db.WithContext(ctx).
		Model(&model.Notification{}).
		Where("id = ?", id).
		Updates(map[string]any{
			"status":     model.StatusFailed,
			"updated_at": time.Now(),
		}).Error
}

func (r *notificationRepo) MarkAsWaiting(ctx context.Context, id uint) error {
	updates := map[string]any{
		"status":     model.StatusWaiting,
		"updated_at": time.Now(),
	}

	return r.db.WithContext(ctx).
		Model(&model.Notification{}).
		Where("id = ?", id).
		Updates(updates).Error
}

// GetPendingOutboxEvents возвращает события, которые ещё не отправлены в NATS (рудимент, актуален только если воркер 1)
func (r *notificationRepo) GetPendingOutboxEvents(ctx context.Context, limit int) ([]model.OutboxEvent, error) {
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

// ClaimPendingOutboxEvents - безопасное забирание событий (для нескольких воркеров)
func (r *notificationRepo) ClaimPendingOutboxEvents(ctx context.Context, limit int) ([]model.OutboxEvent, error) {
	var events []model.OutboxEvent

	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 1. Забираем с блокировкой
		if err := tx.Clauses(clause.Locking{
			Strength: "UPDATE",
			Options:  "SKIP LOCKED",
		}).
			Where("status = ?", model.StatusPending).
			Order("created_at ASC").
			Limit(limit).
			Find(&events).Error; err != nil {
			return err
		}

		if len(events) == 0 {
			return nil
		}

		// 2. Сразу помечаем как sent (для избежания гонки и дублирования нотификаций)
		ids := make([]uint, len(events))
		for i, e := range events {
			ids[i] = e.ID
		}

		return tx.Model(&model.OutboxEvent{}).
			Where("id IN ?", ids).
			Updates(map[string]any{
				"status": model.StatusSent,
			}).Error
	})

	return events, err
}

// MarkOutboxAsSent помечает событие как успешно отправленное
func (r *notificationRepo) MarkOutboxAsSent(ctx context.Context, id uint) error {
	err := r.db.WithContext(ctx).
		Model(&model.OutboxEvent{}).
		Where("id = ?", id).
		Where("status = ?", model.StatusPending).
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

func (r *notificationRepo) MarkAsPending(ctx context.Context, id uint) error {
	err := r.db.WithContext(ctx).
		Model(&model.OutboxEvent{}).
		Where("id = ?", id).
		Updates(map[string]any{
			"status":       model.StatusPending,
			"updated_at":   time.Now(),
			"need_to_sync": true,
		}).Error

	if err != nil {
		return fmt.Errorf("failed to mark outbox event as pending: %w", err)
	}

	return nil
}

func (r *notificationRepo) GetOutboxEventsForSync(ctx context.Context, limit int) ([]model.OutboxEvent, error) {
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

func (r *notificationRepo) MarkOutboxAsSynced(ctx context.Context, outboxID uint) error {
	return r.db.WithContext(ctx).
		Model(&model.OutboxEvent{}).
		Where("id = ?", outboxID).
		Update("need_to_sync", false).Error
}

func (r *notificationRepo) IncrementSendAttempts(ctx context.Context, id uint) error {
	return r.db.WithContext(ctx).
		Model(&model.OutboxEvent{}).
		Where("id = ?", id).
		UpdateColumn("send_attempts", gorm.Expr("send_attempts + 1")).Error
}
