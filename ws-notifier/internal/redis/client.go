package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/moshdealer/notification-platform/pkg/config"
	"github.com/redis/go-redis/v9"
)

var (
	TestTTL            = 20 * time.Second
	UnreadSetKeyPrefix string
	DefaultTTL         time.Duration
	HighTTL            time.Duration
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

	DefaultTTL = time.Duration(cfg.DefaultTTL) * time.Second
	HighTTL = time.Duration(cfg.HighTTL) * time.Second
	UnreadSetKeyPrefix = cfg.UnreadKeyPrefix

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

	var notificationPayload struct {
		EventID int `json:"event_id"`
		Payload struct {
			Priority string `json:"priority"`
		} `json:"payload"`
	}

	if err := json.Unmarshal(data, &notificationPayload); err != nil || notificationPayload.EventID == 0 {
		return c.rdb.Set(ctx, fmt.Sprintf("user:%s:unread:fallback:%d", userID, time.Now().UnixNano()), data, TestTTL).Err()
	}

	setKey := fmt.Sprintf(UnreadSetKeyPrefix, userID)
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
		TTL = HighTTL
		pipe.Set(ctx, msgKey, data, TTL)
	} else {
		TTL = DefaultTTL
		pipe.Set(ctx, msgKey, data, TTL)
	}

	_, err := pipe.Exec(ctx)
	if err == nil {
		fmt.Printf("[Redis] Saved unread event_id=%d for user=%s (TTL=%v)\n", notificationPayload.EventID, userID, TTL)
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

func (c *Client) Close() error {
	if c.rdb == nil {
		return nil
	}
	err := c.rdb.Close()
	if err != nil {
		fmt.Printf("Redis close error: %v\n", err)
	} else {
		fmt.Println("Redis client closed")
	}
	return err
}
