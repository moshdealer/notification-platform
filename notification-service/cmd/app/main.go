package main

import (
	"context"
	"fmt"
	"gorm.io/gorm"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/moshdealer/notification-platform/notification-service/internal/handler"
	"github.com/moshdealer/notification-platform/notification-service/internal/kafka"
	"github.com/moshdealer/notification-platform/notification-service/internal/nats"
	"github.com/moshdealer/notification-platform/notification-service/internal/repository"
	"github.com/moshdealer/notification-platform/notification-service/internal/router"
	"github.com/moshdealer/notification-platform/notification-service/internal/service"
	outbox "github.com/moshdealer/notification-platform/notification-service/internal/worker"
	"github.com/moshdealer/notification-platform/pkg/config"
	"github.com/moshdealer/notification-platform/pkg/database/db"
	"github.com/moshdealer/notification-platform/pkg/messaging"
	"github.com/moshdealer/notification-platform/pkg/observability"
)

/*
Notification-service - сервис, который принимает по REST запросы на отправку нотификаций пользователям
Запросы обрабатываются пачками n-количеством worker'ов и отправляются в Nats
*/

//TODO
// - докер оптимизировать
// - Сделать поддержку смены outbox dispatcher'ов

type App struct {
	Config              *config.ConfigNotificationService
	DB                  *gorm.DB
	NotificationRepo    repository.NotificationRepository
	Publisher           messaging.Publisher
	NotificationService *service.NotificationService
	OutboxSyncer        *outbox.Syncer
}

func main() {
	// 1. Загружаем конфиг
	cfg, err := config.LoadNotificationService()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Load cfg error: %v\n", err)
		os.Exit(1)
	}

	observability.Init(cfg.LogsCfg)

	appCtx := observability.WithLogger(context.Background(), observability.FromContext(context.Background()))

	// TODO Логирую конфиг специально для удобства отладки, для боя убрать
	observability.Debug(appCtx, "Config has been loaded:", "config", cfg)

	// 2. Подключаем БД + миграции
	dbConn, err := db.Connect(&cfg.Database)
	if err != nil {
		observability.Error(appCtx, "DB connect error", "error", err)
		os.Exit(1)
	}
	err = db.Migrate(&cfg.Database)
	if err != nil {
		observability.Error(appCtx, "Migrate error", "error", err)
		os.Exit(1)
	}

	// 3. Создаём все компоненты
	app := &App{
		Config: cfg,
		DB:     dbConn,
	}

	// 4. Репозитории
	app.NotificationRepo = repository.NewNotificationRepository(app.DB)

	rootCtx, stop := signal.NotifyContext(appCtx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// 5. Message Brokers
	if app.Config.OutboxDispatcherEnabled {
		var publisher messaging.Publisher

		switch cfg.BrokerType {
		case "kafka":
			kafkaPub, err := kafka.NewPublisher(&cfg.Kafka)
			if err != nil {
				observability.Error(rootCtx, "Kafka Publisher connect error", "error", err)
				os.Exit(1)
			}
			if err := kafka.EnsureTopic(&cfg.Kafka); err != nil {
				observability.Error(rootCtx, "Failed to create Kafka topic:", "error", err)
				os.Exit(1)
			}
			publisher = kafkaPub
			defer kafkaPub.Close()

		default:
			natsPub, err := nats.NewPublisher(&cfg.NATS)
			if err != nil {
				observability.Error(rootCtx, "NATS Publisher connect error", "error", err)
				os.Exit(1)
			}
			if err := nats.CreateNotificationsStream(natsPub.GetJetStream(), cfg.NATS); err != nil {
				observability.Error(rootCtx, "Failed to create JetStream stream:", "error", err)
				os.Exit(1)
			}
			publisher = natsPub
			defer natsPub.Close()
		}

		app.Publisher = publisher
	} else {
		observability.Info(rootCtx, "[Outbox] Running in API-only mode (background workers disabled)")
	}

	app.NotificationService = service.NewNotificationService(app.NotificationRepo, app.Publisher)

	// 6. Worker для синхронизации outbox
	app.OutboxSyncer = outbox.NewSyncer(
		app.NotificationRepo,
		10*time.Second,
		100,
	)

	// 7. Запуск worker
	if app.Config.OutboxDispatcherEnabled {
		// Запускаем outbox dispatcher (отправка событий в broker)
		observability.Info(rootCtx, "[Outbox] Starting background workers",
			"workers", []string{"OutboxDispatcher", "OutboxSyncer"},
		)
		go app.NotificationService.StartOutboxDispatcher(rootCtx, 4)
		go app.OutboxSyncer.Run(rootCtx)
	}

	// 7. Запуск HTTP сервер
	notificationHandler := handler.NewNotificationHandler(app.NotificationService)
	r := router.New(notificationHandler)
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

	observability.Info(rootCtx, "Notification service started",
		"port", app.Config.Server.Port,
	)

	<-rootCtx.Done()

	// Shutdown
	observability.Info(appCtx, "Shutting down Notification service...")

	// Закрытие Message broker
	if app.Publisher != nil {
		app.Publisher.Close()
	}

	// Закрытие HTTP
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		observability.Error(appCtx, "Shutdown error", "error", err)
	}

	// Закрытие БД
	if app.DB != nil {
		if sqlDB, err := app.DB.DB(); err == nil {
			if closeErr := sqlDB.Close(); closeErr != nil {
				observability.Error(appCtx, "Failed to close database connection", "error", closeErr)
			} else {
				observability.Info(appCtx, "Database connection closed")
			}
		}
	}

	observability.Info(appCtx, "Service stopped gracefully")
}
