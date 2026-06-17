package observability

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/moshdealer/notification-platform/pkg/config"
)

// Пакет работает через глобальный логгер и использованием контекста
// Это сделано для удобства автоматического обогащения логов в HTTP-запросах через Gin middleware
// В будущем возможно сделаю логгер также частью app

const (
	loggerKey string = "logger"
)

var defaultLogger = slog.Default()

// Init инициализирует глобальный структурированный json логгер
func Init(logsLevel config.LogCfg) {
	var logLevel slog.Level
	switch strings.ToLower(logsLevel.LogLevel) {
	case "debug":
		logLevel = slog.LevelDebug
	case "warn", "warning":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	default:
		logLevel = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		Level:     logLevel,
		AddSource: false,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey && a.Value.Kind() == slog.KindTime {
				return slog.Attr{
					Key:   "timestamp",
					Value: slog.StringValue(a.Value.Time().Format(time.RFC3339Nano)),
				}
			}
			return a
		},
	}

	handler := slog.NewJSONHandler(os.Stdout, opts)
	logger := slog.New(handler)

	slog.SetDefault(logger)
	defaultLogger = logger
}

// FromContext возвращает логгер из контекста
// Если логгера в контексте нет, то возвращает дефолтный
func FromContext(ctx context.Context) *slog.Logger {
	if ctx == nil {
		return defaultLogger
	}
	if l, ok := ctx.Value(loggerKey).(*slog.Logger); ok && l != nil {
		return l
	}
	return defaultLogger
}

// WithLogger кладёт логгер в контекст
func WithLogger(ctx context.Context, logger *slog.Logger) context.Context {
	if logger == nil {
		return ctx
	}
	return context.WithValue(ctx, loggerKey, logger)
}

// LoggingMiddleware - Gin middleware
// Добавляет request_id, обогащает контекст логгером и логирует запросы
func LoggingMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()

		// Берём request_id из заголовка или генерируем
		requestID := c.GetHeader("X-Request-ID")
		if requestID == "" {
			requestID = generateRequestID()
		}

		c.Set("request_id", requestID)
		c.Header("X-Request-ID", requestID)

		// Создаём обогащённый логгер для этого запроса
		reqLogger := defaultLogger.With(slog.String("request_id", requestID))

		// Кладём логгер в контекст
		ctx := WithLogger(c.Request.Context(), reqLogger)
		c.Request = c.Request.WithContext(ctx)

		// Логируем начало запроса
		reqLogger.Info("request started",
			slog.String("method", c.Request.Method),
			slog.String("path", c.Request.URL.Path),
			slog.String("client_ip", c.ClientIP()),
			slog.String("user_agent", c.Request.UserAgent()),
		)

		c.Next()

		// Логируем завершение запроса
		duration := time.Since(start)
		status := c.Writer.Status()

		level := slog.LevelInfo
		if status >= 500 {
			level = slog.LevelError
		} else if status >= 400 {
			level = slog.LevelWarn
		}

		reqLogger.Log(c.Request.Context(), level, "request completed",
			slog.Int("status", status),
			slog.Duration("duration", duration),
			slog.Int("response_size", c.Writer.Size()),
		)
	}
}

// generateRequestID генерирует короткий уникальный идентификатор запроса
func generateRequestID() string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return time.Now().Format("20060102150405") + "-" + hex.EncodeToString(b)
}

func Debug(ctx context.Context, msg string, args ...any) {
	FromContext(ctx).Debug(msg, args...)
}

func Info(ctx context.Context, msg string, args ...any) {
	FromContext(ctx).Info(msg, args...)
}

func Warn(ctx context.Context, msg string, args ...any) {
	FromContext(ctx).Warn(msg, args...)
}

func Error(ctx context.Context, msg string, args ...any) {
	FromContext(ctx).Error(msg, args...)
}
