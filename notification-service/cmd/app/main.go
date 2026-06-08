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
	"github.com/moshdealer/notification-platform/notification-service/internal/nats"
	"github.com/moshdealer/notification-platform/notification-service/internal/repository"
	"github.com/moshdealer/notification-platform/notification-service/internal/router"
	"github.com/moshdealer/notification-platform/notification-service/internal/service"
	outbox "github.com/moshdealer/notification-platform/notification-service/internal/worker"
	"github.com/moshdealer/notification-platform/pkg/config"
	"github.com/moshdealer/notification-platform/pkg/database/db"
	"github.com/moshdealer/notification-platform/pkg/observability"
)

//TODO убрать ненужные комментарии
// Контексты и graceful shutdown
// докер оптимизировать
// рефакторинг всего как будет работать

type App struct {
	Config              *config.ConfigNotificationService
	DB                  *gorm.DB
	NotificationRepo    repository.NotificationRepository
	NATSPublisher       *nats.Publisher
	NotificationService *service.NotificationService
	OutboxSyncer        *outbox.Syncer
}

func main() {
	// 1. Загружаем конфиг
	cfg, err := config.LoadNotificationService()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Load load error: %v\n", err)
		os.Exit(1)
	}

	observability.Init(cfg.LogsCfg)
	// TODO Логирую конфиг специально для удобства отладки, для боя убрать
	observability.Debug(context.Background(), "Config has been loaded:", "config", cfg)

	// 2. Подключаем БД + миграции
	dbConn, err := db.Connect(&cfg.Database)
	if err != nil {
		observability.Error(context.Background(), "DB connect error", "error", err)
		os.Exit(1)
	}
	err = db.Migrate(&cfg.Database)
	if err != nil {
		observability.Error(context.Background(), "Migrate error", "error", err)
		os.Exit(1)
	}

	// 3. Создаём все компоненты
	app := &App{
		Config: cfg,
		DB:     dbConn,
	}

	// 4. Репозитории
	app.NotificationRepo = repository.NewNotificationRepository(app.DB)

	// 5. NATS Publisher
	app.NATSPublisher, err = nats.NewPublisher(&app.Config.NATS)
	if err != nil {
		observability.Error(context.Background(), "NATS Publisher connect error", "error", err)
		os.Exit(1)
	}

	if err := nats.CreateNotificationsStream(app.NATSPublisher.GetJetStream(), cfg.NATS); err != nil {
		observability.Error(context.Background(), "Failed to create JetStream stream:", "error", err)
		os.Exit(1)
	}

	// 6. Сервис
	app.NotificationService = service.NewNotificationService(app.NotificationRepo, app.NATSPublisher)

	// 7. Worker для синхронизации outbox
	app.OutboxSyncer = outbox.NewSyncer(
		app.NotificationRepo,
		10*time.Second,
		100,
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Запускаем outbox dispatcher (отправка событий в NATS)

	dispatcherCtx := observability.WithLogger(ctx, observability.FromContext(ctx))
	if app.Config.OutboxDispatcherEnabled {
		observability.Info(dispatcherCtx, "[Outbox] Starting background workers",
			"workers", []string{"OutboxDispatcher", "OutboxSyncer"},
		)
		go app.NotificationService.StartOutboxDispatcher(dispatcherCtx)
		go app.OutboxSyncer.Run(dispatcherCtx)
	} else {
		observability.Info(context.Background(), "[Outbox] Running in API-only mode (background workers disabled)")
	}

	// HTTP сервер
	notificationHandler := handler.NewNotificationHandler(app.NotificationService)

	r := router.New(notificationHandler)
	engine := r.Setup()

	srv := &http.Server{
		Addr:    ":" + app.Config.Server.Port,
		Handler: engine,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			observability.Error(context.Background(), "Server error", "error", err)
		}
	}()

	observability.Info(context.Background(), "Notification service started",
		"port", app.Config.Server.Port,
	)

	<-ctx.Done()
	observability.Info(context.Background(), "Shutting down Notification service...")

	if app.NATSPublisher != nil {
		app.NATSPublisher.Close()
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		observability.Error(context.Background(), "Shutdown error", "error", err)
	}

	observability.Info(context.Background(), "Service stopped gracefully")
}
