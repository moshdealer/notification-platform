package router

import (
	"github.com/gin-gonic/gin"
	"github.com/moshdealer/notification-platform/pkg/observability"
	"github.com/moshdealer/notification-platform/ws-notifier/internal/websocket"
)

type Router struct {
	wsHandler *websocket.Handler
}

func New(wsHandler *websocket.Handler) *Router {
	return &Router{
		wsHandler: wsHandler,
	}
}

func (r *Router) Setup() *gin.Engine {
	engine := gin.New()
	engine.Use(observability.LoggingMiddleware())
	engine.Use(gin.Recovery())

	// Healthcheck
	engine.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"status":  "ok",
			"service": "ws-notifier",
		})
	})

	// WebSocket endpoint
	engine.GET("/ws", r.wsHandler.WebSocket)

	return engine
}
