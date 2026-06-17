package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/moshdealer/notification-platform/pkg/config"
	"github.com/moshdealer/notification-platform/pkg/model"
	"github.com/moshdealer/notification-platform/pkg/observability"
	"github.com/redis/go-redis/v9"
)

type Client struct {
	rdb                *redis.Client
	UnreadSetKeyPrefix string
	BroadcastChannel   string
	TestTTL            time.Duration
	DefaultTTL         time.Duration
	HighTTL            time.Duration
}

func NewClient(cfg *config.RedisCfg) *Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       0,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		panic("Redis connection failed: " + err.Error())
	}

	return &Client{
		rdb:                rdb,
		DefaultTTL:         time.Duration(cfg.DefaultTTL) * time.Second,
		HighTTL:            time.Duration(cfg.HighTTL) * time.Second,
		TestTTL:            20 * time.Second,
		UnreadSetKeyPrefix: cfg.UnreadKeyPrefix,
		BroadcastChannel:   cfg.BroadcastChannel,
	}
}

// AddUnread
func (c *Client) AddUnread(ctx context.Context, userID string, data []byte) error {
	if len(data) == 0 {
		return nil
	}

	notificationPayload := model.NatsMessage{}

	if err := json.Unmarshal(data, &notificationPayload); err != nil || notificationPayload.EventID == 0 {
		return c.rdb.Set(ctx, fmt.Sprintf("user:%s:unread:fallback:%d", userID, time.Now().UnixNano()), data, c.TestTTL).Err()
	}

	setKey := fmt.Sprintf(c.UnreadSetKeyPrefix, userID)
	msgKey := fmt.Sprintf("%s:%d", setKey, notificationPayload.EventID) // user:123:unread:456
	pipe := c.rdb.TxPipeline()

	// 1. Добавляем event_id в Sorted Set (для сортировки по времени)
	pipe.ZAdd(ctx, setKey, redis.Z{
		Score:  float64(time.Now().UnixMilli()),
		Member: notificationPayload.EventID,
	})

	var TTL time.Duration
	// 2. Сохраняем полное сообщение с индивидуальным TTL
	if notificationPayload.Payload.Priority == "high" {
		TTL = c.HighTTL
		pipe.Set(ctx, msgKey, data, TTL)
	} else {
		TTL = c.DefaultTTL
		pipe.Set(ctx, msgKey, data, TTL)
	}

	logger := observability.FromContext(ctx)

	_, err := pipe.Exec(ctx)
	if err == nil {
		logger.Info("Redis Saved unread event_id for user",
			"event_id", notificationPayload.EventID, "user_id", userID, "TTL", TTL)
	}
	return err
}

// GetUnread - возвращает в правильном порядке
func (c *Client) GetUnread(ctx context.Context, userID string) ([][]byte, error) {
	setKey := fmt.Sprintf(c.UnreadSetKeyPrefix, userID)

	eventIDs, err := c.rdb.ZRange(ctx, setKey, 0, -1).Result()
	if err != nil || len(eventIDs) == 0 {
		return nil, err
	}

	var result [][]byte
	for _, eventID := range eventIDs {
		msgKey := fmt.Sprintf("%s:%s", setKey, eventID)
		data, err := c.rdb.Get(ctx, msgKey).Result()
		if err == nil {
			result = append(result, []byte(data))
		} else if err == redis.Nil {
			c.rdb.ZRem(ctx, setKey, eventID)
		}
	}

	// Чистим после отправки
	go c.ClearAllUnread(ctx, userID)

	return result, nil
}

func (c *Client) ClearAllUnread(ctx context.Context, userID string) error {
	pattern := fmt.Sprintf("user:%s:unread*", userID)
	keys, _ := c.rdb.Keys(ctx, pattern).Result()
	if len(keys) > 0 {
		c.rdb.Del(ctx, keys...)
	}
	return nil
}

func (c *Client) RemoveUnread(ctx context.Context, userID string, eventID uint) error {
	if eventID == 0 {
		return nil
	}

	setKey := fmt.Sprintf(c.UnreadSetKeyPrefix, userID)
	msgKey := fmt.Sprintf("%s:%d", setKey, eventID)

	pipe := c.rdb.TxPipeline()

	pipe.ZRem(ctx, setKey, eventID) // удаляем из Sorted Set
	pipe.Del(ctx, msgKey)           // удаляем само сообщение

	logger := observability.FromContext(ctx)
	_, err := pipe.Exec(ctx)
	if err == nil {
		logger.Info("Redis removed delivered event_id for user",
			"event_id", eventID, "user_id", userID)
	}
	return err
}

func (c *Client) Close() error {
	if c.rdb == nil {
		return nil
	}
	err := c.rdb.Close()
	if err != nil {
		observability.Error(context.Background(), "Redis client closed", "error", err)
	} else {
		observability.Info(context.Background(), "Redis client closed")
	}
	return err
}

// PublishBroadcast публикует сообщение во broadcast-канал (для других нод)
func (c *Client) PublishBroadcast(ctx context.Context, channel string, data []byte) error {
	return c.rdb.Publish(ctx, channel, data).Err()
}

// SubscribeBroadcast подписывается на broadcast-канал
// handler будет вызван на каждой ноде при получении сообщения
func (c *Client) SubscribeBroadcast(ctx context.Context, channel string, handler func([]byte)) {
	pubsub := c.rdb.Subscribe(ctx, channel)
	ch := pubsub.Channel()

	go func() {
		for msg := range ch {
			handler([]byte(msg.Payload))
		}
	}()
}
