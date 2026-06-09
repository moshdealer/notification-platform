package nats

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
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type Subscriber struct {
	nc               *nats.Conn
	js               jetstream.JetStream
	cons             jetstream.ConsumeContext
	wsManager        *websocket.Manager
	redisClient      *redis.Client
	repo             repository.OutBoxRepository
	streamName       string
	filterStreamName string
}

func NewSubscriber(
	cfg *config.NATSCfg,
	wm *websocket.Manager,
	rc *redis.Client,
	repo repository.OutBoxRepository,
) (*Subscriber, error) {
	nc, err := nats.Connect(cfg.NATSAddr,
		nats.Name("ws-notifier-subscriber"),
		nats.MaxReconnects(-1),
	)
	if err != nil {
		return nil, fmt.Errorf("NATS connect error: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("JetStream init error: %w", err)
	}

	filterStreamName := fmt.Sprintf("%v.>", cfg.SubjectNew)

	return &Subscriber{
		nc:               nc,
		js:               js,
		wsManager:        wm,
		redisClient:      rc,
		repo:             repo,
		streamName:       cfg.SubjectNew,
		filterStreamName: filterStreamName,
	}, nil
}

func (s *Subscriber) Start() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	logger := observability.FromContext(ctx)

	consumer, err := s.js.CreateOrUpdateConsumer(ctx, s.streamName, jetstream.ConsumerConfig{
		Durable:       "notification-service-ws",
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxDeliver:    30,
		AckWait:       60 * time.Second,
		FilterSubject: s.filterStreamName,
		BackOff: []time.Duration{
			1 * time.Second,
			5 * time.Second,
			10 * time.Second,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}

	// Запускаем слушателя Redis broadcast (все ноды будут слушать)
	s.startRedisBroadcastListener()

	consumeCtx, err := consumer.Consume(func(msg jetstream.Msg) {
		natsMessage := model.NatsMessage{}
		if err := json.Unmarshal(msg.Data(), &natsMessage); err != nil {
			logger.Error("Failed to unmarshal", "error", err)
			msg.Nak()
			return
		}

		userID := natsMessage.Payload.UserID
		priority := natsMessage.Payload.Priority
		eventID := natsMessage.EventID

		var ttl time.Duration
		if priority == "high" {
			ttl = s.redisClient.HighTTL
		} else {
			ttl = s.redisClient.DefaultTTL
		}

		sentLocally := false

		// 1. Пытаемся отправить локально
		if s.wsManager.HasActiveConnections(userID) {
			if sendErr := s.wsManager.SendToUser(userID, msg.Data()); sendErr == nil {
				if err := s.repo.MarkAsDelivered(context.Background(), eventID); err != nil {
					logger.Warn("Failed to mark as delivered", "error", err, "event_id", eventID)
				}
				sentLocally = true
				logger.Info("Sent Message to online local user", "user_id", userID, "event_id", eventID)
				observability.NotificationsDeliveredTotal.WithLabelValues("delivered").Inc()
			} else {
				logger.Info("Try deliver via Redis", "user_id", userID, "event_id", eventID)
			}
		}

		// 2. Сохраняем в Redis ТОЛЬКО если не отправили локально
		if !sentLocally {
			if addErr := s.redisClient.AddUnread(context.Background(), userID, msg.Data()); addErr != nil {
				logger.Error("Failed to save to Redis for user", "user_id", userID)
				if err := s.repo.MarkAsFailed(context.Background(), eventID); err != nil {
					logger.Error("Failed to mark as failed", "error", err, "event_id", eventID)
				}
				msg.NakWithDelay(time.Second * 15)
				return
			}
			logger.Info("Save Message to Redis Cache", "event_id", eventID)

			if err := s.repo.MarkAsWaiting(context.Background(), eventID, ttl); err != nil {
				logger.Error("Failed to mark as waiting", "error", err, "event_id", eventID)
			}

			// 3. Публикуем в broadcast — чтобы другие ноды попробовали отправить
			if err := s.redisClient.PublishBroadcast(context.Background(), s.redisClient.BroadcastChannel, msg.Data()); err == nil {
				logger.Info("Publish Message to broadcast", "event_id", eventID)
			} else {
				logger.Error("Failed publish Message to broadcast", "error", err)
				msg.NakWithDelay(time.Second * 15)
				return
			}
		}

		observability.NATSConsumedTotal.WithLabelValues(priority).Inc()
		msg.Ack()
	})

	if err != nil {
		return fmt.Errorf("failed to start consumer: %w", err)
	}

	s.cons = consumeCtx
	logger.Info("NATS JetStream Subscriber started with Redis Broadcast")
	return nil
}

// TODO  навести порядок в слоях, это нужно в другой модуль вынести
// Слушаем broadcast от других нод
func (s *Subscriber) startRedisBroadcastListener() {
	s.redisClient.SubscribeBroadcast(context.Background(), s.redisClient.BroadcastChannel, func(data []byte) {
		var natsMessage model.NatsMessage
		if err := json.Unmarshal(data, &natsMessage); err != nil {
			return
		}

		userID := natsMessage.Payload.UserID
		eventID := natsMessage.EventID

		// Отправляем, только если юзер онлайн именно на этой ноде
		if s.wsManager.HasActiveConnections(userID) {
			if err := s.wsManager.SendToUser(userID, data); err == nil {
				if markErr := s.repo.MarkAsDelivered(context.Background(), natsMessage.EventID); markErr != nil {
					observability.Error(context.Background(), "Failed to mark as delivered from broadcast",
						"error", markErr, "event_id", natsMessage.EventID)
				}
				s.redisClient.RemoveUnread(context.Background(), userID, eventID)
				observability.Info(context.Background(), "Sent Message to User from Redis broadcast",
					"user_id", userID, "event_id", eventID)
				observability.NotificationsDeliveredTotal.WithLabelValues("delivered").Inc()
			}
		}
	})
}

func (s *Subscriber) Close() {
	if s.cons != nil {
		s.cons.Stop()
		s.cons = nil
	}
	if s.nc != nil {
		s.nc.Close()
	}
	observability.Info(context.Background(), "NATS JetStream Subscriber closed")
}
