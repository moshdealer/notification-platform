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
	"github.com/moshdealer/notification-platform/notification-service/internal/redis"
	"github.com/moshdealer/notification-platform/notification-service/internal/repository"
	"github.com/moshdealer/notification-platform/notification-service/internal/router"
	"github.com/moshdealer/notification-platform/notification-service/internal/service"
	"github.com/moshdealer/notification-platform/notification-service/internal/websocket"
	"github.com/moshdealer/notification-platform/pkg/config"
	"github.com/moshdealer/notification-platform/pkg/database/db"
)

/*
На текущий момент реализовано:
1) Дергая Post /notifications, создаются записи в БД (само сообщение и event)
2) NatsPublisher каждые 5 сек дергает таблицу Event и отправляет в Nats
*/

//TODO убрать ненужные комментарии
// рефакторинг всего как будет работать
// Redis конфиги
// Redis тестирование
// Redis разобраться с ключем (сейчас один ключ на все сообщения для юзера)
// Разбор каждого модуля
// Разбор того как мы шлем сообщение в итоге (отказаться от payload)
// Логирование
// Auth токены

type App struct {
	Config              *config.Config
	DB                  *gorm.DB
	NotificationRepo    repository.NotificationRepository
	NATSPublisher       *nats.Publisher
	NotificationService *service.NotificationService
	WSManager           *websocket.Manager
	RedisClient         *redis.Client
	NATSSubscriber      *nats.Subscriber
}

func main() {
	// 1. Загружаем конфиг
	cfg, err := config.Load()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "config load error: %v\n", err)
		os.Exit(1)
	}
	// 2. Подключаем БД + миграции
	dbConn, err := db.ConnectAndMigrate(&cfg.Database)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "DB connect error: %v\n", err)
		os.Exit(1)
	}

	// 3. Создаём все компоненты
	app := &App{
		Config: cfg,
		DB:     dbConn,
	}

	fmt.Println(app)

	// 4. Репозитории
	app.NotificationRepo = repository.NewNotificationRepository(app.DB)

	app.NATSPublisher, err = nats.NewPublisher(&app.Config.NATS)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "NATS Publisher connect error: %v\n", err)
		os.Exit(1)
	}

	app.NotificationService = service.NewNotificationService(app.NotificationRepo, app.NATSPublisher)

	app.RedisClient = redis.NewClient(&cfg.Redis)
	app.WSManager = websocket.NewManager()

	app.NATSSubscriber = nats.NewSubscriber(&app.Config.NATS, app.WSManager, app.RedisClient)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go app.NotificationService.StartOutboxDispatcher(ctx)

	if err := app.NATSSubscriber.Start(); err != nil {
		log.Printf("NATS Subscriber start error: %v", err)
	}
	fmt.Println("NATS WebSocket Subscriber запущен")

	// HTTP сервер
	notificationHandler := handler.NewNotificationHandler(app.NotificationService)
	wsHandler := websocket.NewHandler(app.WSManager, app.RedisClient)

	r := router.New(notificationHandler, wsHandler)
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
	fmt.Println("WebSocket: ws://localhost:" + app.Config.Server.Port + "/ws?user_id=123&token=xxx")

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
