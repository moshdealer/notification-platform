package messaging

import (
	"context"

	"github.com/moshdealer/notification-platform/pkg/model"
)

// Publisher - общий интерфейс для отправки сообщений в брокер.
// Реализуют: NATS Publisher и Kafka Publisher.
type Publisher interface {
	// Publish отправляет сообщение конкретному пользователю
	Publish(ctx context.Context, userID string, message model.NatsMessage) error

	// Close закрывает соединение с брокером
	Close()
}
