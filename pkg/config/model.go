package config

import "time"

type Config struct {
	Server        ServerCfg        `mapstructure:"server"`
	Database      DatabaseCfg      `mapstructure:"database"`
	Redis         RedisCfg         `mapstructure:"redis"`
	NATS          NATSCfg          `mapstructure:"nats"`
	Notifications NotificationsCfg `mapstructure:"notifications"`
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
	RedisAddr       string `mapstructure:"addr"`
	RedisPassword   string `mapstructure:"password"`
	RedisDB         int    `mapstructure:"db"`
	PoolSize        int    `mapstructure:"pool_size"`
	DefaultTTL      int    `mapstructure:"default_ttl"`
	HighTTL         int    `mapstructure:"high_ttl"`
	UnreadKeyPrefix string `mapstructure:"unread_key_prefix"`
}

type NATSCfg struct {
	NATSAddr    string `mapstructure:"addr"`
	SubjectNew  string `mapstructure:"subject_new"`
	SubjectRead string `mapstructure:"subject_read"`
}
type NotificationsCfg struct {
	DefaultPriority string `mapstructure:"default_priority"`
}
