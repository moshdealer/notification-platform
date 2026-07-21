package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/moshdealer/notification-platform/pkg/config"
	"github.com/moshdealer/notification-platform/pkg/database/db"
	"github.com/moshdealer/notification-platform/pkg/messaging"
	"github.com/moshdealer/notification-platform/pkg/observability"
	"github.com/moshdealer/notification-platform/ws-notifier/internal/kafka"
	"github.com/moshdealer/notification-platform/ws-notifier/internal/nats"
	"github.com/moshdealer/notification-platform/ws-notifier/internal/redis"
	"github.com/moshdealer/notification-platform/ws-notifier/internal/repository"
	"github.com/moshdealer/notification-platform/ws-notifier/internal/router"
	"github.com/moshdealer/notification-platform/ws-notifier/internal/websocket"
)

/*
WS-Notifier - сервис для обработки WebSocket соединений и доставки уведомлений в реальном времени.
Подписывается на NATS и рассылает сообщения подключённым клиентам.
*/

//TODO
// - Вынести рассылку в горутины (в целом оптимизировать процесс)
// - Добавить идемпотентность в брокеры
// - Докер оптимизировать

type App struct {
	Config         *config.ConfigWSNotifier
	RedisClient    *redis.Client
	WSManager      *websocket.Manager
	Subscriber     messaging.Subscriber
	NATSSubscriber *nats.Subscriber
	OutBoxRepo     repository.OutBoxRepository
}

func main() {
	// 1. Загружаем конфиг
	cfg, err := config.LoadWSNotifier()
	if err != nil {
		observability.Error(context.Background(), "config load error", "error", err)
		os.Exit(1)
	}

	app := &App{
		Config: cfg,
	}

	observability.Init(cfg.LogsCfg)

	appCtx := observability.WithLogger(context.Background(), observability.FromContext(context.Background()))

	// TODO Логирую конфиг специально для удобства отладки, для боя убрать
	observability.Debug(appCtx, "Config loaded", "config", cfg)

	rootCtx, stop := signal.NotifyContext(appCtx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	dbConn, err := db.Connect(&cfg.Database)
	if err != nil {
		observability.Error(rootCtx, "DB connect error", "error", err)
		os.Exit(1)
	}

	app.OutBoxRepo = repository.NewOutBoxRepository(dbConn) // твой репозиторий

	// 2. Redis
	app.RedisClient = redis.NewClient(&cfg.Redis)

	// 3. WebSocket Manager
	app.WSManager = websocket.NewManagerWithContext(rootCtx)

	// 4. Message Brokers (слушает уведомления и раздаёт по WS)
	var subscriber messaging.Subscriber

	switch cfg.BrokerType {
	case "kafka":
		kafkaSub, err := kafka.NewSubscriber(&cfg.Kafka, app.WSManager, app.RedisClient, app.OutBoxRepo, rootCtx)
		if err != nil {
			observability.Error(rootCtx, "Kafka Subscriber create error", "error", err)
			os.Exit(1)
		}
		subscriber = kafkaSub

	default:
		natsSub, err := nats.NewSubscriber(&cfg.NATS, app.WSManager, app.RedisClient, app.OutBoxRepo, rootCtx)
		if err != nil {
			observability.Error(rootCtx, "NATS Subscriber create error", "error", err)
			os.Exit(1)
		}
		subscriber = natsSub
	}
	app.Subscriber = subscriber

	// Запускаем subscriber
	if err := app.Subscriber.Start(); err != nil {
		observability.Error(rootCtx, "Subscriber start error", "error", err)
		os.Exit(1)
	}
	defer subscriber.Close()

	// 5. HTTP сервер (только WebSocket)
	wsHandler := websocket.NewHandler(app.WSManager, app.RedisClient, app.OutBoxRepo)

	r := router.New(wsHandler)
	engine := r.Setup()

	srv := &http.Server{
		Addr:    ":" + app.Config.Server.Port,
		Handler: engine,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			observability.Error(rootCtx, "Server error", "error", err)
		}
	}()
	observability.Info(rootCtx, "WS-Notifier started on http://localhost",
		"port", app.Config.Server.Port)
	observability.Info(rootCtx, "WebSocket endpoint: ws://localhost",
		"port", app.Config.Server.Port)

	<-rootCtx.Done()
	observability.Info(context.Background(), "Shutting down WS-Notifier")

	// Graceful shutdown
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		observability.Error(shutdownCtx, "Server shutdown error", "error", err)
	}

	if app.Subscriber != nil {
		app.Subscriber.Close()
	}

	if app.RedisClient != nil {
		app.RedisClient.Close()
	}

	if app.WSManager != nil {
		app.WSManager.CloseAll()
	}

	observability.Info(shutdownCtx, "WS-Notifier stopped gracefully")
}
