package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/moshdealer/notification-platform/pkg/config"
	"github.com/moshdealer/notification-platform/pkg/model"
	"github.com/moshdealer/notification-platform/pkg/observability"
	"github.com/moshdealer/notification-platform/ws-notifier/internal/redis"
	"github.com/moshdealer/notification-platform/ws-notifier/internal/repository"
	"github.com/moshdealer/notification-platform/ws-notifier/internal/websocket"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Subscriber struct {
	client      *kgo.Client
	wsManager   *websocket.Manager
	redisClient *redis.Client
	repo        repository.OutBoxRepository
	topic       string
	group       string
	rootCtx     context.Context
	cancel      context.CancelFunc
}

func NewSubscriber(
	cfg *config.KafkaCfg,
	wm *websocket.Manager,
	rc *redis.Client,
	repo repository.OutBoxRepository,
	rootCtx context.Context,
) (*Subscriber, error) {
	if rootCtx == nil {
		rootCtx = context.Background()
	}

	ctx, cancel := context.WithCancel(rootCtx)

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.ConsumerGroup),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.FetchMaxWait(500 * time.Millisecond),
		kgo.DisableAutoCommit(), // будем коммитить вручную после обработки
		kgo.SessionTimeout(30 * time.Second),
		kgo.HeartbeatInterval(3 * time.Second),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create kafka consumer: %w", err)
	}

	return &Subscriber{
		client:      client,
		wsManager:   wm,
		redisClient: rc,
		repo:        repo,
		topic:       cfg.Topic,
		group:       cfg.ConsumerGroup,
		rootCtx:     ctx,
		cancel:      cancel,
	}, nil
}

func (s *Subscriber) Start() error {
	logger := observability.FromContext(s.rootCtx)
	logger.Info("Kafka Subscriber starting", "topic", s.topic, "group", s.group)

	s.startRedisBroadcastListener()

	go func() {
		for {
			select {
			case <-s.rootCtx.Done():
				return
			default:
				fetches := s.client.PollFetches(s.rootCtx)
				if fetches.IsClientClosed() {
					return
				}

				// Обрабатываем записи батчами по 500
				records := fetches.Records()

				for i := 0; i < len(records); i += 500 {
					end := i + 500
					if end > len(records) {
						end = len(records)
					}

					batch := records[i:end]

					for _, record := range batch {
						s.processRecord(record)
					}
				}
			}
		}
	}()

	return nil
}

// processRecord
func (s *Subscriber) processRecord(record *kgo.Record) {
	msgCtx, cancel := context.WithTimeout(s.rootCtx, 10*time.Second)
	defer cancel()

	logger := observability.FromContext(msgCtx)

	var natsMessage model.NatsMessage
	if err := json.Unmarshal(record.Value, &natsMessage); err != nil {
		logger.Error("Failed to unmarshal kafka message", "error", err)
		return
	}

	s.client.MarkCommitRecords(record) // Коммитим запись после успешного unmarshal

	priority := natsMessage.Payload.Priority
	if priority == "" {
		priority = "medium"
	}

	observability.BrokerConsumeReceivedTotal.WithLabelValues("kafka", priority).Inc()

	if !record.Timestamp.IsZero() {
		latency := time.Since(record.Timestamp).Seconds()
		observability.BrokerConsumeLatencySeconds.WithLabelValues("kafka", priority).Observe(latency)
	}

	userID := natsMessage.Payload.UserID
	eventID := natsMessage.EventID

	sentLocally := false

	if s.wsManager.HasActiveConnections(userID) {
		if sendErr := s.wsManager.SendToUser(msgCtx, userID, record.Value); sendErr == nil {
			if err := s.repo.MarkAsDelivered(msgCtx, eventID); err != nil {
				logger.Warn("Failed to mark as delivered", "error", err, "event_id", eventID)
			}
			sentLocally = true
			observability.NotificationsDeliveredTotal.WithLabelValues("delivered").Inc()
		}
	}

	if !sentLocally {
		if addErr := s.redisClient.AddUnread(msgCtx, userID, record.Value); addErr != nil {
			logger.Error("Failed to save to Redis", "error", addErr, "user_id", userID)
			return
		}

		ttl := s.redisClient.DefaultTTL
		if priority == "high" {
			ttl = s.redisClient.HighTTL
		}

		if err := s.repo.MarkAsWaiting(msgCtx, eventID, ttl); err != nil {
			logger.Error("Failed to mark as waiting", "error", err, "event_id", eventID)
		}

		if err := s.redisClient.PublishBroadcast(msgCtx, s.redisClient.BroadcastChannel, record.Value); err != nil {
			logger.Error("Failed to publish to broadcast", "error", err)
		}
	}

	logger.Info("Kafka message processed",
		"user_id", userID,
		"event_id", eventID,
		"priority", priority,
	)
}

func (s *Subscriber) startRedisBroadcastListener() {
	s.redisClient.SubscribeBroadcast(s.rootCtx, s.redisClient.BroadcastChannel, func(data []byte) {
		bcCtx, bcCancel := context.WithTimeout(s.rootCtx, 5*time.Second)
		defer bcCancel()

		var natsMessage model.NatsMessage
		if err := json.Unmarshal(data, &natsMessage); err != nil {
			return
		}

		userID := natsMessage.Payload.UserID
		eventID := natsMessage.EventID

		// Отправляем, только если юзер онлайн именно на этой ноде
		if s.wsManager.HasActiveConnections(userID) {
			if err := s.wsManager.SendToUser(bcCtx, userID, data); err == nil {
				if markErr := s.repo.MarkAsDelivered(bcCtx, natsMessage.EventID); markErr != nil {
					observability.Error(bcCtx, "Failed to mark as delivered from broadcast",
						"error", markErr, "event_id", natsMessage.EventID)
				}
				s.redisClient.RemoveUnread(bcCtx, userID, eventID)
				observability.Info(bcCtx, "Sent Message to User from Redis broadcast",
					"user_id", userID, "event_id", eventID)
				observability.NotificationsDeliveredTotal.WithLabelValues("delivered").Inc()
			}
		}
	})
}

// Close останавливает подписку и закрывает соединение с Kafka
func (s *Subscriber) Close() {
	if s.cancel != nil {
		s.cancel()
	}

	if s.client != nil {
		s.client.Close()
	}

	observability.Info(context.Background(), "Kafka Subscriber closed")
}
