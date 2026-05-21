package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/moshdealer/notification-platform/pkg/config"
	"github.com/moshdealer/notification-platform/pkg/model"
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
			log.Printf("Failed to unmarshal: %v", err)
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
					fmt.Printf("Failed to mark as delivered %d: %v\n", eventID, err)
				}
				sentLocally = true
				fmt.Printf("Sent Message %v to online local user %v\n", eventID, userID)
			}
		}

		// 2. Сохраняем в Redis ТОЛЬКО если не отправили локально
		if !sentLocally {
			if addErr := s.redisClient.AddUnread(context.Background(), userID, msg.Data()); addErr != nil {
				fmt.Printf("Failed to save to Redis for user %s: %v\n", userID, addErr)
				if err := s.repo.MarkAsFailed(context.Background(), eventID); err != nil {
					fmt.Printf("Failed to mark as failed %d: %v\n", eventID, err)
				}
				msg.NakWithDelay(time.Second * 15)
				return
			}
			fmt.Printf("Save Message %v to Redis Cache\n", eventID)

			if err := s.repo.MarkAsWaiting(context.Background(), eventID, ttl); err != nil {
				fmt.Printf("Failed to mark as waiting %d: %v\n", eventID, err)
			}

			// 3. Публикуем в broadcast — чтобы другие ноды попробовали отправить
			s.redisClient.PublishBroadcast(context.Background(), s.redisClient.BroadcastChannel, msg.Data())
			fmt.Printf("Publish Message %v to broadcast\n", eventID)
		}

		msg.Ack()
	})

	if err != nil {
		return fmt.Errorf("failed to start consumer: %w", err)
	}

	s.cons = consumeCtx
	fmt.Println("NATS JetStream Subscriber started with Redis Broadcast")
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
					fmt.Printf("Failed to mark as delivered from broadcast %d: %v\n", natsMessage.EventID, markErr)
				}
				s.redisClient.RemoveUnread(context.Background(), userID, eventID)
				fmt.Println("Sent Message to User from Redis broadcast")
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
	fmt.Println("NATS JetStream Subscriber closed")
}
