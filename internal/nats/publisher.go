package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/moshdealer/notification-service/internal/config"
	"github.com/nats-io/nats.go"
)

// Publisher отвечает за отправку событий в NATS
type Publisher struct {
	conn    *nats.Conn
	subject string
}

// NewPublisher создаёт и подключает publisher
func NewPublisher(cfg *config.NATSCfg) (*Publisher, error) {
	conn, err := nats.Connect(
		cfg.NATSAddr,
		nats.Name("notification-publisher"),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS at %s: %w", cfg.NATSAddr, err)
	}

	fmt.Printf("NATS Publisher connected to %s\n", cfg.NATSAddr)

	return &Publisher{
		conn:    conn,
		subject: cfg.SubjectNew,
	}, nil
}

// Publish отправляет любое событие в NATS
func (p *Publisher) Publish(ctx context.Context, userId string, payload interface{}) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	subject := fmt.Sprintf("%s.%s", p.subject, userId)
	if err := p.conn.Publish(subject, data); err != nil {
		return fmt.Errorf("failed to publish to subject %s: %w", p.subject, err)
	}

	return nil
}

// Close закрывает соединение (вызывается при shutdown)
func (p *Publisher) Close() {
	if p.conn != nil {
		p.conn.Close()
		fmt.Println("NATS Publisher connection closed")
	}
}
