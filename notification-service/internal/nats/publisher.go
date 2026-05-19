package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/moshdealer/notification-platform/pkg/model"
	"time"

	"github.com/moshdealer/notification-platform/pkg/config"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type Publisher struct {
	js      jetstream.JetStream
	subject string
}

func NewPublisher(cfg *config.NATSCfg) (*Publisher, error) {
	nc, err := nats.Connect(
		cfg.NATSAddr,
		nats.Name("notification-publisher"),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
		nats.Timeout(10*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	fmt.Printf("NATS JetStream Publisher connected to %s\n", cfg.NATSAddr)

	return &Publisher{
		js:      js,
		subject: cfg.SubjectNew,
	}, nil
}

// Publish отправляет уведомление и ждёт подтверждения от JetStream
func (p *Publisher) Publish(ctx context.Context, userId string, message model.NatsMessage) error {
	data, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	subject := fmt.Sprintf("%s.%s", p.subject, userId)
	msgID := fmt.Sprintf("notif-%d", message.EventID)
	// Publish с ожиданием Ack от стрима
	_, err = p.js.Publish(ctx, subject, data, jetstream.WithMsgID(msgID))
	if err != nil {
		return fmt.Errorf("failed to publish to %s: %w", subject, err)
	}

	return nil
}

func (p *Publisher) GetJetStream() jetstream.JetStream {
	return p.js
}

func (p *Publisher) Close() {
	fmt.Println("NATS JetStream Publisher closed")
}
