package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/moshdealer/notification-service/internal/config"
	"github.com/redis/go-redis/v9"
)

const (
	UnreadSetKeyPrefix = "user:%s:unread"
	TestTTL            = 30 * time.Second
)

type Client struct {
	rdb *redis.Client
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

	return &Client{rdb: rdb}
}

// AddUnread
func (c *Client) AddUnread(ctx context.Context, userID string, data []byte) error {
	if len(data) == 0 {
		return nil
	}

	var payload struct {
		EventID int `json:"event_id"`
	}
	if err := json.Unmarshal(data, &payload); err != nil || payload.EventID == 0 {

		return c.rdb.Set(ctx, fmt.Sprintf("user:%s:unread:fallback:%d", userID, time.Now().UnixNano()), data, TestTTL).Err()
	}

	eventID := fmt.Sprintf("%d", payload.EventID)
	setKey := fmt.Sprintf(UnreadSetKeyPrefix, userID)
	msgKey := fmt.Sprintf("%s:%s", setKey, eventID) // user:123:unread:456

	pipe := c.rdb.TxPipeline()

	// 1. Добавляем event_id в Sorted Set (для сортировки по времени)
	pipe.ZAdd(ctx, setKey, redis.Z{
		Score:  float64(time.Now().UnixMilli()),
		Member: eventID,
	})

	// 2. Сохраняем полное сообщение с индивидуальным TTL
	pipe.Set(ctx, msgKey, data, TestTTL)

	_, err := pipe.Exec(ctx)
	if err == nil {
		fmt.Printf("[Redis] Saved unread event_id=%s for user=%s (TTL=%v)\n", eventID, userID, TestTTL)
	}
	return err
}

// GetUnread — возвращает в правильном порядке
func (c *Client) GetUnread(ctx context.Context, userID string) ([][]byte, error) {
	setKey := fmt.Sprintf(UnreadSetKeyPrefix, userID)

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
	go c.ClearUnread(context.Background(), userID)

	return result, nil
}

func (c *Client) ClearUnread(ctx context.Context, userID string) error {
	pattern := fmt.Sprintf("user:%s:unread*", userID)
	keys, _ := c.rdb.Keys(ctx, pattern).Result()
	if len(keys) > 0 {
		c.rdb.Del(ctx, keys...)
	}
	return nil
}
