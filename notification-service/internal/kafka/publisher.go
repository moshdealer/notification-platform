package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/moshdealer/notification-platform/pkg/config"
	"github.com/moshdealer/notification-platform/pkg/model"
	"github.com/moshdealer/notification-platform/pkg/observability"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Publisher struct {
	client *kgo.Client
	topic  string
}

func NewPublisher(cfg *config.KafkaCfg) (*Publisher, error) {
	// Инициализируем основной клиент для отправки сообщений
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.DefaultProduceTopic(cfg.Topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordDeliveryTimeout(10 * time.Second),
		kgo.DialTimeout(3 * time.Second),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	observability.Info(context.Background(), "Kafka Publisher connected",
		"brokers", cfg.Brokers,
		"topic", cfg.Topic,
	)

	return &Publisher{
		client: client,
		topic:  cfg.Topic,
	}, nil
}

func (p *Publisher) Publish(ctx context.Context, userID string, message model.NatsMessage) error {

	data, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	record := &kgo.Record{
		Key:   []byte(userID),
		Value: data,
	}

	err = p.client.ProduceSync(ctx, record).FirstErr()
	if err != nil {
		return fmt.Errorf("failed to produce to kafka: %w", err)
	}

	return nil
}

func (p *Publisher) Close() {
	if p.client != nil {
		p.client.Close()
	}
	observability.Info(context.Background(), "Kafka Publisher closed")
}
