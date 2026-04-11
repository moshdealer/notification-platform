package main

import (
	"context"
	"fmt"
	"gorm.io/gorm"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/moshdealer/notification-service/internal/config"
	"github.com/moshdealer/notification-service/internal/database/db"
)

type App struct {
	Config *config.Config
	DB     *gorm.DB
	//NotificationRepo    repository.NotificationRepository
	//NotificationService *service.NotificationService
	//WSManager           *websocket.Manager
	//NATSPublisher       *broker.Publisher
	//NATSWorker          *broker.Worker // будет запускать subscriber'ы
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

	/*
		// 4. Репозитории
		app.NotificationRepo = repository.NewNotificationRepository(app.DB)

		// 5. WebSocket менеджер
		app.WSManager = websocket.NewManager()

		// 6. NATS (publisher + worker)
		app.NATSPublisher = broker.NewPublisher(cfg.NATS.URL)
		app.NATSWorker = broker.NewWorker(cfg.NATS.URL, app.NotificationService) // позже передадим service

		// 7. Сервисы (зависит от repo + broker + ws)
		app.NotificationService = service.NewNotificationService(
			app.NotificationRepo,
			app.NATSPublisher,
			app.WSManager,
		)

		// 8. Запускаем worker'ы в фоне
		go app.NATSWorker.Run() // здесь будут оба subscriber'а (new + read)

		// 9. HTTP + WebSocket сервер
		h := handler.NewHandler(app.NotificationService, app.WSManager)
	*/
	// 10. Graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	/*
		server := h.SetupRouter() // Gin router
		go func() {
			if err := server.Run(":" + cfg.Server.Port); err != nil {
				log.Fatalf("server failed: %v", err)
			}
		}()
	*/
	<-ctx.Done()
	log.Println("Shutting down...")
	// здесь shutdown NATS, WS, DB
}
