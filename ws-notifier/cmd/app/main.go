package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/moshdealer/notification-platform/pkg/config"
	"github.com/moshdealer/notification-platform/pkg/database/db"
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
// - Auth
// - Graceful shutdown для всех
// - Prometheus metrics попробовать накинуть
// - Логирование
// - Докер оптимизировать

type App struct {
	Config         *config.ConfigWSNotifier
	RedisClient    *redis.Client
	WSManager      *websocket.Manager
	NATSSubscriber *nats.Subscriber
	OutBoxRepo     repository.OutBoxRepository
}

func main() {
	// 1. Загружаем конфиг
	cfg, err := config.LoadWSNotifier()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "config load error: %v\n", err)
		os.Exit(1)
	}

	app := &App{
		Config: cfg,
	}

	dbConn, err := db.Connect(&cfg.Database)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "DB connect error: %v\n", err)
		os.Exit(1)
	}

	app.OutBoxRepo = repository.NewOutBoxRepository(dbConn) // твой репозиторий

	// 2. Redis
	app.RedisClient = redis.NewClient(&cfg.Redis)

	// 3. WebSocket Manager
	app.WSManager = websocket.NewManager()

	// 4. NATS Subscriber (слушает уведомления и раздаёт по WS)
	app.NATSSubscriber, err = nats.NewSubscriber(&app.Config.NATS, app.WSManager, app.RedisClient, app.OutBoxRepo)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Запускаем NATS subscriber
	if err := app.NATSSubscriber.Start(); err != nil {
		log.Printf("NATS Subscriber start error: %v", err)
		os.Exit(1)
	}
	fmt.Println("NATS WebSocket Subscriber запущен")

	// 5. HTTP сервер (только WebSocket)

	wsHandler := websocket.NewHandler(app.WSManager, app.RedisClient, app.OutBoxRepo)

	// Используем router из ws-notifier
	r := router.New(wsHandler)
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
	fmt.Printf("WS-Notifier started on http://localhost:%s\n", app.Config.Server.Port)
	fmt.Printf("WebSocket endpoint: ws://localhost:%s/ws\n", app.Config.Server.Port)

	<-ctx.Done()
	fmt.Println("Shutting down WS-Notifier...")

	// Graceful shutdown
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("Server shutdown error: %v", err)
	}

	if app.NATSSubscriber != nil {
		app.NATSSubscriber.Close()
	}

	if app.RedisClient != nil {
		app.RedisClient.Close()
	}

	if app.WSManager != nil {
		app.WSManager.CloseAll()
	}

	fmt.Println("WS-Notifier stopped gracefully")
}
