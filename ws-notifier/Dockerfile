# -------- Stage 1: build --------
FROM golang:1.25-alpine AS builder

WORKDIR /app

RUN apk add --no-cache git

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

# Копируем конфиги
COPY --from=builder /app/ws-notifier/configs ./configs

EXPOSE 8081

# Запускаем
CMD ["./ws-notifier"]