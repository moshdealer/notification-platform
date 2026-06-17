package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

func LoadNotificationService() (*ConfigNotificationService, error) {
	v := viper.New()

	// Пути для конфига
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath("./configs")

	v.SetEnvPrefix("NS")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Связали структуру с переменными окружениями
	_ = v.BindEnv("database.dsn", "POSTGRES_DSN")
	_ = v.BindEnv("nats.addr", "NATS_ADDR")
	_ = v.BindEnv("outbox_dispatcher_enabled", "OUTBOX_DISPATCHER_ENABLED")

	// Считали yaml-конфиг
	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("read config error: %w", err)
	}
	var cfg ConfigNotificationService
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal error: %w", err)
	}

	// Считали env-переменные
	v.GetString("database.dsn")
	v.GetString("nats.addr")
	v.GetString("outbox_dispatcher_enabled")

	return &cfg, nil
}

func LoadWSNotifier() (*ConfigWSNotifier, error) {
	v := viper.New()

	// Пути для конфига
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath("./configs")

	v.SetEnvPrefix("NS")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Связали структуру с переменными окружениями
	_ = v.BindEnv("database.dsn", "POSTGRES_DSN")
	_ = v.BindEnv("redis.addr", "REDIS_ADDR")
	_ = v.BindEnv("redis.password", "REDIS_PASSWORD")
	_ = v.BindEnv("nats.addr", "NATS_ADDR")
	_ = v.BindEnv("nats.node_name", "NATS_NODE_NAME")

	// Считали yaml-конфиг
	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("read config error: %w", err)
	}
	var cfg ConfigWSNotifier
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal error: %w", err)
	}

	// Считали env-переменные
	v.GetString("database.dsn")
	v.GetString("redis.addr")
	v.GetString("redis.password")
	v.GetString("nats.addr")

	return &cfg, nil
}
