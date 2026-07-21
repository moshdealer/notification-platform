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
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     cfg.SubjectNew,
		Subjects: []string{fmt.Sprintf("%v.>", cfg.SubjectNew)},

		Retention: jetstream.WorkQueuePolicy,
		Discard:   jetstream.DiscardOld,

		Duplicates: 10 * time.Second,
		// Оставим лимиты на всякий случай
		MaxAge:   1 * 24 * time.Hour,     // максимальное время жизни
		MaxMsgs:  -1,                     // без лимита по количеству
		MaxBytes: 2 * 1024 * 1024 * 1024, // 2 ГБ,  // 1 мб
		Storage:  jetstream.FileStorage,
		// Replicas: 3,

	})
	return err
}
