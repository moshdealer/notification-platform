package config

import "time"

type ConfigNotificationService struct {
	Server                  ServerCfg   `mapstructure:"server"`
	Database                DatabaseCfg `mapstructure:"database"`
	NATS                    NATSCfg     `mapstructure:"nats"`
	LogsCfg                 LogCfg      `mapstructure:"logs"`
	OutboxDispatcherEnabled bool        `mapstructure:"outbox_dispatcher_enabled"`
}

type ConfigWSNotifier struct {
	Server   ServerCfg   `mapstructure:"server"`
	Database DatabaseCfg `mapstructure:"database"`
	Redis    RedisCfg    `mapstructure:"redis"`
	NATS     NATSCfg     `mapstructure:"nats"`
	LogsCfg  LogCfg      `mapstructure:"logs"`
}
type ServerCfg struct {
	Port         string        `mapstructure:"port"`
	ReadTimeout  time.Duration `mapstructure:"read_timeout"`
	WriteTimeout time.Duration `mapstructure:"write_timeout"`
}

type DatabaseCfg struct {
	DatabaseDSN     string        `mapstructure:"dsn"`
	MaxOpenConns    int           `mapstructure:"max_open_conns"`
	MaxIdleConns    int           `mapstructure:"max_idle_conns"`
	ConnMaxLifetime time.Duration `mapstructure:"conn_max_lifetime"`
}

type RedisCfg struct {
	RedisAddr        string `mapstructure:"addr"`
	RedisPassword    string `mapstructure:"password"`
	RedisDB          int    `mapstructure:"db"`
	PoolSize         int    `mapstructure:"pool_size"`
	DefaultTTL       int    `mapstructure:"default_ttl"`
	HighTTL          int    `mapstructure:"high_ttl"`
	UnreadKeyPrefix  string `mapstructure:"unread_key_prefix"`
	BroadcastChannel string `mapstructure:"broadcast_channel"`
}

type NATSCfg struct {
	NATSAddr    string `mapstructure:"addr"`
	SubjectNew  string `mapstructure:"subject_new"`
	SubjectRead string `mapstructure:"subject_read"`
	NodeName    string `mapstructure:"node_name"`
}

type LogCfg struct {
	LogLevel string `mapstructure:"logs_level"`
}
