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
	nc          *nats.Conn
	js          jetstream.JetStream
	consumer    jetstream.Consumer
	cons        jetstream.ConsumeContext
	wsManager   *websocket.Manager
	redisClient *redis.Client
	repo        repository.OutBoxRepository
	streamName  string
	nodeName    string
	rootCtx     context.Context
	cancel      context.CancelFunc
}

func NewSubscriber(
	cfg *config.NATSCfg,
	wm *websocket.Manager,
	rc *redis.Client,
	repo repository.OutBoxRepository,
	rootCtx context.Context,
) (*Subscriber, error) {
	if rootCtx == nil {
		rootCtx = context.Background()
	}

	ctx, cancel := context.WithCancel(rootCtx)

	nc, err := nats.Connect(cfg.NATSAddr,
		nats.Name("ws-notifier-subscriber"),
		nats.MaxReconnects(-1),
	)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("NATS connect error: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		cancel()
		nc.Close()
		return nil, fmt.Errorf("JetStream init error: %w", err)
	}

	return &Subscriber{
		nc:          nc,
		js:          js,
		wsManager:   wm,
		redisClient: rc,
		repo:        repo,
		streamName:  cfg.SubjectNew,
		nodeName:    cfg.NodeName,
		rootCtx:     ctx,
		cancel:      cancel,
	}, nil
}

func (s *Subscriber) Start() error {
	logger := observability.FromContext(s.rootCtx)
	logger.Info("NATS JetStream Pull Subscriber starting",
		"stream", s.streamName,
	)

	consumer, err := s.js.CreateOrUpdateConsumer(
		s.rootCtx,
		s.streamName,
		jetstream.ConsumerConfig{
			Name:          "notification-service-ws-pull",
			Durable:       "notification-service-ws-pull",
			AckPolicy:     jetstream.AckExplicitPolicy,
			MaxDeliver:    10,
			AckWait:       180 * time.Second,
			MaxAckPending: 50000,
			FilterSubject: s.streamName + ".>",
		},
	)
	if err != nil {
		return fmt.Errorf("failed to create pull consumer: %w", err)
	}

	s.consumer = consumer

	s.startRedisBroadcastListener()

	consumeCtx, err := consumer.Consume(
		s.processMessage,
		jetstream.PullMaxMessages(64),
		jetstream.ConsumeErrHandler(
			func(_ jetstream.ConsumeContext, consumeErr error) {
				// При штатном shutdown ошибку не логируем
				if s.rootCtx.Err() != nil {
					return
				}

				observability.Error(
					s.rootCtx,
					"NATS consumer error",
					"error", consumeErr,
				)
			},
		),
	)
	if err != nil {
		return fmt.Errorf("failed to start pull consumer: %w", err)
	}

	s.cons = consumeCtx

	logger.Info("NATS JetStream Pull Subscriber started",
		"prefetch_messages", 64,
	)

	return nil
}

func (s *Subscriber) processMessage(msg jetstream.Msg) {
	msgCtx, cancel := context.WithTimeout(s.rootCtx, 10*time.Second)
	defer cancel()

	logger := observability.FromContext(msgCtx)

	var natsMessage model.NatsMessage
	if err := json.Unmarshal(msg.Data(), &natsMessage); err != nil {
		logger.Error("Failed to unmarshal NATS message",
			"error", err,
		)

		if termErr := msg.Term(); termErr != nil {
			logger.Error("Failed to terminate malformed NATS message",
				"error", termErr,
			)
		}

		return
	}

	userID := natsMessage.Payload.UserID
	priority := natsMessage.Payload.Priority
	eventID := natsMessage.EventID

	// Сначала пытаемся доставить локальному WebSocket-клиенту.
	if s.wsManager.HasActiveConnections(userID) {
		if sendErr := s.wsManager.SendToUser(
			msgCtx,
			userID,
			msg.Data(),
		); sendErr == nil {
			if err := s.repo.MarkAsDelivered(msgCtx, eventID); err != nil {
				logger.Warn("Failed to mark notification as delivered",
					"error", err,
					"event_id", eventID,
				)
			}

			observability.NotificationsDeliveredTotal.
				WithLabelValues("delivered").Inc()

			if err := msg.Ack(); err != nil {
				logger.Error("Failed to Ack locally delivered message",
					"error", err,
					"event_id", eventID,
				)
			}

			return
		}
	}

	if err := s.redisClient.AddUnread(
		msgCtx,
		userID,
		msg.Data(),
	); err != nil {
		logger.Error("Failed to save notification to Redis",
			"error", err,
			"user_id", userID,
			"event_id", eventID,
		)

		if markErr := s.repo.MarkAsFailed(msgCtx, eventID); markErr != nil {
			logger.Error("Failed to mark notification as failed",
				"error", markErr,
				"event_id", eventID,
			)
		}

		if nakErr := msg.NakWithDelay(2 * time.Second); nakErr != nil {
			logger.Error("Failed to Nak NATS message",
				"error", nakErr,
				"event_id", eventID,
			)
		}

		return
	}

	ttl := s.redisClient.DefaultTTL
	if priority == "high" {
		ttl = s.redisClient.HighTTL
	}

	if err := s.repo.MarkAsWaiting(msgCtx, eventID, ttl); err != nil {
		logger.Error("Failed to mark notification as waiting",
			"error", err,
			"event_id", eventID,
		)
	}

	if err := s.redisClient.PublishBroadcast(
		msgCtx,
		s.redisClient.BroadcastChannel,
		msg.Data(),
	); err != nil {
		logger.Error("Failed to publish notification to Redis broadcast",
			"error", err,
			"event_id", eventID,
		)
	}

	if err := msg.Ack(); err != nil {
		logger.Error("Failed to Ack persisted NATS message",
			"error", err,
			"event_id", eventID,
		)
	}
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

func (s *Subscriber) Close() {
	if s.cancel != nil {
		s.cancel()
	}

	if s.cons != nil {
		s.cons.Stop()
		s.cons = nil
	}
	if s.nc != nil {
		s.nc.Close()
	}
	observability.Info(s.rootCtx, "NATS JetStream Subscriber closed")
}
