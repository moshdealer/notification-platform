package router

import (
	"github.com/gin-gonic/gin"

	"github.com/moshdealer/notification-platform/notification-service/internal/handler"
	"github.com/moshdealer/notification-platform/pkg/observability"
)

type Router struct {
	notificationHandler *handler.NotificationHandler
}

func New(
	notificationHandler *handler.NotificationHandler,
) *Router {
	return &Router{
		notificationHandler: notificationHandler,
	}
}

func (r *Router) Setup() *gin.Engine {
	engine := gin.New()
	engine.Use(observability.LoggingMiddleware())
	engine.Use(observability.MetricsMiddleware())
	engine.Use(gin.Recovery())

	// Healthcheck
	engine.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})

	// Metrics
	engine.GET("/metrics/ns", observability.PrometheusHandler())

	// Notification routes
	notifications := engine.Group("/notifications")
	{
		notifications.POST("", r.notificationHandler.Create)
		notifications.POST("/read", r.notificationHandler.MarkAsRead)
	}

	return engine
}
