package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/moshdealer/notification-platform/pkg/config"
	"github.com/moshdealer/notification-platform/pkg/observability"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// EnsureTopic инициализирует временный клиент для проверки и создания топика
func EnsureTopic(cfg *config.KafkaCfg) error {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.DialTimeout(3 * time.Second),
	}

	adminClient, err := kgo.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("failed to create admin kafka client: %w", err)
	}

	defer adminClient.Close()

	adminCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	admin := kadm.NewClient(adminClient)

	observability.Info(adminCtx, "Checking Kafka topics...", "topic", cfg.Topic)

	topicDetails, err := admin.ListTopics(adminCtx, cfg.Topic)
	if err != nil {
		return fmt.Errorf("admin.ListTopics failed for %s: %w", cfg.Topic, err)
	}

	if !topicDetails.Has(cfg.Topic) {
		// Создаем топик: 5 партиции, 1 реплика
		_, err = admin.CreateTopics(adminCtx, 5, 1, nil, cfg.Topic)
		if err != nil {
			return fmt.Errorf("failed to create topic %s: %w", cfg.Topic, err)
		}
		observability.Info(context.Background(), "Kafka topic created", "topic", cfg.Topic)
	}

	return nil
}
