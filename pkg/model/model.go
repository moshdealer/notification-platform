package model

import (
	"time"

	"gorm.io/datatypes"
)

const (
	StatusPending   = "pending"
	StatusSent      = "sent"
	StatusDelivered = "delivered"
	StatusRead      = "read"
	StatusFailed    = "failed"
	StatusExpired   = "expired"
)

type Notification struct {
	ID        uint           `gorm:"primaryKey;autoIncrement" json:"id"`
	UserID    string         `gorm:"column:user_id;type:varchar(50);not null;index" json:"user_id"`
	Title     string         `gorm:"type:varchar(255);not null" json:"title"`
	Body      string         `gorm:"type:text" json:"body"`
	Type      string         `gorm:"type:varchar(50);not null;index" json:"type"`
	Priority  string         `gorm:"type:varchar(20);default:medium;index" json:"priority"`
	Data      map[string]any `gorm:"type:jsonb" json:"data,omitempty"`
	Status    string         `gorm:"type:varchar(20);not null;default:pending;index" json:"status"`
	Read      bool           `gorm:"default:false;index" json:"read"`
	CreatedAt time.Time      `gorm:"type:timestamptz;default:now();index" json:"created_at"`
	UpdatedAt time.Time      `gorm:"type:timestamptz;default:now()" json:"updated_at"`
}

type OutboxEvent struct {
	ID             uint              `gorm:"primaryKey;autoIncrement" json:"id"`
	UserID         string            `gorm:"column:user_id;type:varchar(50);not null;index" json:"user_id"`
	Topic          string            `gorm:"type:varchar(255);not null;index" json:"topic"`
	NotificationID *uint             `gorm:"column:notification_id;index;constraint:OnDelete:CASCADE" json:"notification_id,omitempty"`
	Payload        datatypes.JSONMap `gorm:"type:jsonb;not null;default:'{}'::jsonb"`
	Status         string            `gorm:"type:varchar(20);not null;default:pending;index:idx_outbox_status_created" json:"status"`
	Priority       string            `gorm:"type:varchar(20);default:medium;index" json:"priority"`
	Retries        int               `gorm:"default:0" json:"retries"`
	CreatedAt      time.Time         `gorm:"default:now()" json:"created_at"`
	UpdatedAt      *time.Time        `json:"sent_at,omitempty"`
	NeedToSync     bool              `gorm:"column:need_to_sync;default:false;index:idx_outbox_need_sync"`
}
