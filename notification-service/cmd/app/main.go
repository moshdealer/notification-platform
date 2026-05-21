package main

import (
	"context"
	"fmt"
	"gorm.io/gorm"
	"log"
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
)

//TODO убрать ненужные комментарии
// Auth токены
// Логирование
// докер оптимизировать
// рефакторинг всего как будет работать
// Просмотр ссылок и переменных

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
		_, _ = fmt.Fprintf(os.Stderr, "config load error: %v\n", err)
		os.Exit(1)
	}

	// 2. Подключаем БД + миграции
	dbConn, err := db.Connect(&cfg.Database)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "DB connect error: %v\n", err)
		os.Exit(1)
	}
	err = db.Migrate(&cfg.Database)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Migrate error: %v\n", err)
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
		_, _ = fmt.Fprintf(os.Stderr, "NATS Publisher connect error: %v\n", err)
		os.Exit(1)
	}

	if err := nats.CreateNotificationsStream(app.NATSPublisher.GetJetStream(), cfg.NATS); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Failed to create JetStream stream: %v\n", err)
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

	if app.Config.OutboxDispatcherEnabled {
		fmt.Println("[Outbox] Starting background workers:")
		fmt.Println("OutboxDispatcher (pending events to NATS)")
		fmt.Println("OutboxSyncer (sync status back to notifications)")
		go app.NotificationService.StartOutboxDispatcher(ctx)
		go app.OutboxSyncer.Run(ctx)
	} else {
		fmt.Println("[Outbox] Running in API-only mode (background workers disabled)")
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
			log.Printf("Server error: %v", err)
		}
	}()

	fmt.Printf("Notification service started on http://localhost:%s\n", app.Config.Server.Port)

	<-ctx.Done()
	fmt.Println("Shutting down...")

	if app.NATSPublisher != nil {
		app.NATSPublisher.Close()
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("Shutdown error: %v", err)
	}

	fmt.Println("Service stopped gracefully")
}
