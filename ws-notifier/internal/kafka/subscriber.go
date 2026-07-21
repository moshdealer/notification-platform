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
		// Offset отправляется вручную только после обработки
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
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

	logger.Info("Kafka Subscriber starting",
		"topic", s.topic,
		"group", s.group,
	)

	s.startRedisBroadcastListener()

	go s.runPollLoop()

	return nil
}

func (s *Subscriber) runPollLoop() {
	logger := observability.FromContext(s.rootCtx)

	const maxPollRecords = 500

	for {
		if s.rootCtx.Err() != nil {
			return
		}

		// В отличие от PollFetches, ограничивает число записей,
		// которые приложение должно обработать перед следующим poll.
		fetches := s.client.PollRecords(
			s.rootCtx,
			maxPollRecords,
		)

		if fetches.IsClientClosed() {
			return
		}

		if s.rootCtx.Err() != nil {
			s.client.AllowRebalance()
			return
		}

		for _, fetchErr := range fetches.Errors() {
			logger.Error("Kafka fetch error",
				"topic", fetchErr.Topic,
				"partition", fetchErr.Partition,
				"error", fetchErr.Err,
			)
		}

		/*Сюда попадёт последняя последовательно обработанная запись
		каждой partition. CommitRecords выберет максимальный offset
		для каждой partition и отправит один commit-запрос*/
		recordsToCommit := make([]*kgo.Record, 0)

		fetches.EachPartition(func(partition kgo.FetchTopicPartition) {
			var lastProcessed *kgo.Record

			for _, record := range partition.Records {
				if err := s.processRecord(record); err != nil {
					logger.Error("Kafka record processing failed",
						"topic", record.Topic,
						"partition", record.Partition,
						"offset", record.Offset,
						"error", err,
					)
					break
				}

				lastProcessed = record
			}

			if lastProcessed != nil {
				recordsToCommit = append(
					recordsToCommit,
					lastProcessed,
				)
			}
		})

		if len(recordsToCommit) > 0 {
			commitCtx, cancel := context.WithTimeout(
				s.rootCtx,
				5*time.Second,
			)

			err := s.client.CommitRecords(
				commitCtx,
				recordsToCommit...,
			)

			cancel()

			if err != nil {
				logger.Error("Failed to commit Kafka offsets",
					"error", err,
					"partitions", len(recordsToCommit),
				)
			}
		}

		s.client.AllowRebalance()
	}
}

// processRecord
func (s *Subscriber) processRecord(record *kgo.Record) error {
	msgCtx, cancel := context.WithTimeout(
		s.rootCtx,
		10*time.Second,
	)
	defer cancel()

	logger := observability.FromContext(msgCtx)

	var kafkaMessage model.NatsMessage
	if err := json.Unmarshal(record.Value, &kafkaMessage); err != nil {
		logger.Error("Failed to unmarshal Kafka message",
			"error", err,
			"topic", record.Topic,
			"partition", record.Partition,
			"offset", record.Offset,
			"action", "skip_and_commit",
		)
		return nil
	}

	priority := kafkaMessage.Payload.Priority
	if priority == "" {
		priority = "medium"
	}

	observability.BrokerConsumeReceivedTotal.
		WithLabelValues("kafka", priority).
		Inc()

	if !record.Timestamp.IsZero() {
		latency := time.Since(record.Timestamp).Seconds()

		observability.BrokerConsumeLatencySeconds.
			WithLabelValues("kafka", priority).
			Observe(latency)
	}

	userID := kafkaMessage.Payload.UserID
	eventID := kafkaMessage.EventID

	// Сначала пытаемся доставить локально
	if s.wsManager.HasActiveConnections(userID) {
		if sendErr := s.wsManager.SendToUser(
			msgCtx,
			userID,
			record.Value,
		); sendErr == nil {
			if err := s.repo.MarkAsDelivered(
				msgCtx,
				eventID,
			); err != nil {
				logger.Warn("Failed to mark Kafka message as delivered",
					"error", err,
					"event_id", eventID,
				)
			}
			observability.NotificationsDeliveredTotal.
				WithLabelValues("delivered").
				Inc()

			return nil
		}
	}

	// Если локально не доставили, сначала надёжно сохраняем в Redis.
	if err := s.redisClient.AddUnread(
		msgCtx,
		userID,
		record.Value,
	); err != nil {
		return fmt.Errorf(
			"failed to save event %d to Redis: %w",
			eventID,
			err,
		)
	}

	ttl := s.redisClient.DefaultTTL
	if priority == "high" {
		ttl = s.redisClient.HighTTL
	}

	if err := s.repo.MarkAsWaiting(
		msgCtx,
		eventID,
		ttl,
	); err != nil {
		logger.Error("Failed to mark Kafka message as waiting",
			"error", err,
			"event_id", eventID,
		)
	}

	if err := s.redisClient.PublishBroadcast(
		msgCtx,
		s.redisClient.BroadcastChannel,
		record.Value,
	); err != nil {
		logger.Error("Failed to publish Kafka message to broadcast",
			"error", err,
			"event_id", eventID,
		)
	}

	logger.Info("Kafka message processed",
		"user_id", userID,
		"event_id", eventID,
		"priority", priority,
		"partition", record.Partition,
		"offset", record.Offset,
	)

	return nil
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
