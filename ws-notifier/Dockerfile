# -------- Stage 1: build --------
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Устанавливаем git (если нужны приватные модули)
RUN apk add --no-cache git

# Копируем весь проект (docker-compose запускается из корня)
#COPY go.work .
COPY pkg ./pkg
COPY ws-notifier ./ws-notifier

# Переходим в папку сервиса и скачиваем зависимости
WORKDIR /app/ws-notifier
RUN go mod download

# Собираем именно ws-notifier
RUN go build -o /app/bin/ws-notifier ./cmd/app

# -------- Stage 2: runtime --------
FROM alpine:3.20

WORKDIR /app

RUN apk add --no-cache ca-certificates tzdata

# Копируем бинарник
COPY --from=builder /app/bin/ws-notifier ./ws-notifier

# Копируем конфиги и другие нужные файлы (если есть)
COPY --from=builder /app/ws-notifier/configs ./configs
# COPY --from=builder /app/ws-notifier/migrations ./migrations   # раскомментируй, если появятся

EXPOSE 8081

# Запускаем
CMD ["./ws-notifier"]