package router

import (
	"github.com/gin-gonic/gin"

	"github.com/moshdealer/notification-platform/notification-service/internal/handler"   // твой существующий handler
	"github.com/moshdealer/notification-platform/notification-service/internal/websocket" // новый WS handler
)

type Router struct {
	notificationHandler *handler.NotificationHandler
	wsHandler           *websocket.Handler // ← добавили
}

func New(
	notificationHandler *handler.NotificationHandler,
	wsHandler *websocket.Handler,
) *Router {
	return &Router{
		notificationHandler: notificationHandler,
		wsHandler:           wsHandler,
	}
}

func (r *Router) Setup() *gin.Engine {
	engine := gin.Default()

	// Healthcheck
	engine.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})

	// Notification routes
	notifications := engine.Group("/notifications")
	{
		notifications.POST("", r.notificationHandler.Create)
		notifications.POST("/read", r.notificationHandler.MarkAsRead)
	}

	// WS routes
	engine.GET("/ws", r.wsHandler.WebSocket)

	return engine
}
