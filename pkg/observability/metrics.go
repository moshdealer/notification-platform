package observability

import (
	"fmt"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// HTTP метрики
var (
	HTTPRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total", // Всего http запросов
			Help: "Total number of HTTP requests",
		},
		[]string{"method", "path", "status"},
	)

	HTTPRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds", // Длительность обработки запросов
			Help:    "HTTP request duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "path", "status"},
	)
)

// Notification-service
var (
	NotificationsCreatedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "notifications_created_total", // Всего уведомлений
			Help: "Total number of notifications created",
		},
		[]string{"type", "priority"},
	)

	OutboxEventsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "outbox_events_total", //Всего событий отправки уведомлений
			Help: "Total number of outbox events",
		},
		[]string{"priority"},
	)

	NATSPublishedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nats_published_total", // Сколько ушло в Nats
			Help: "Total number of messages published to NATS",
		},
		[]string{"priority"},
	)

	NatsPublishDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "notification_nats_publish_duration_seconds", // Время публикации одного события в NATS (включая ожидание ack)
		Help:    "Duration of JetStream Publish call (with server ack)",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 12), // от 1мс до ~4 сек
	}, []string{"worker_id"})

	OutboxClaimDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "notification_outbox_claim_duration_seconds", // Время выполнения Claim
		Help:    "Duration of ClaimPendingOutboxEvents query",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 12),
	}, []string{"worker_id"})

	OutboxPendingGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "notification_outbox_pending_events", // Текущее количество pending событий в outbox
		Help: "Current number of pending events in outbox_events table",
	})
)

// Ws-notifier
var (
	WebSocketConnectionsActive = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "websocket_connections_active", // Сколько всего активных подключений
			Help: "Current number of active WebSocket connections",
		},
	)

	NATSConsumedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nats_consumed_total", // Сколько обработано из Nats
			Help: "Total number of messages consumed from NATS",
		},
		[]string{"priority"},
	)

	NotificationsDeliveredTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "notifications_delivered_total", // Сколько доставлено уведомлений
			Help: "Total number of notifications delivered to clients",
		},
		[]string{"status"},
	)
)

// PrometheusHandler - хендлер для /metrics
func PrometheusHandler() gin.HandlerFunc {
	h := promhttp.Handler()
	return func(c *gin.Context) {
		h.ServeHTTP(c.Writer, c.Request)
	}
}

// MetricsMiddleware - middleware для автоматического сбора HTTP-метрик
func MetricsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()

		c.Next()

		status := fmt.Sprintf("%d", c.Writer.Status())
		method := c.Request.Method
		path := c.FullPath()
		if path == "" {
			path = c.Request.URL.Path
		}

		HTTPRequestsTotal.WithLabelValues(method, path, status).Inc()
		HTTPRequestDuration.WithLabelValues(method, path, status).Observe(time.Since(start).Seconds())
	}
}
