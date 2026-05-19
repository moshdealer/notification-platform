package nats

import (
	"context"
	"fmt"
	"time"

	"github.com/moshdealer/notification-platform/pkg/config"
	"github.com/nats-io/nats.go/jetstream"
)

func CreateNotificationsStream(js jetstream.JetStream, cfg config.NATSCfg) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	fmt.Sprintf("%v.>", cfg.SubjectNew)
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     cfg.SubjectNew,
		Subjects: []string{fmt.Sprintf("%v.>", cfg.SubjectNew)},

		Retention:  jetstream.WorkQueuePolicy, // сообщения удаляются после Ack
		Duplicates: 5 * time.Minute,
		// Если хотим работать с WorkQueuePolicy:
		MaxAge:   7 * 24 * time.Hour, // максимальное время жизни
		MaxMsgs:  -1,                 // без лимита по количеству
		MaxBytes: 200 * 1024 * 1024,  // 1 мб

		Storage: jetstream.FileStorage,
		// Replicas: 3,

	})
	return err
}
