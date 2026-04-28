# -------- Stage 1: build --------
FROM golang:1.25-alpine AS builder

WORKDIR /app

RUN apk add --no-cache git

# Копируем зависимости
COPY go.mod go.sum ./
RUN go mod download

# Копируем весь код сервиса
COPY . .

# Собираем
RUN go build -o /app/notification-service ./cmd/app

# -------- Stage 2: runtime --------
FROM alpine:3.20

WORKDIR /app

RUN apk add --no-cache ca-certificates

# Копируем собранный бинарник
COPY --from=builder /app/notification-service .

# Копируем миграции (если они есть)
COPY migrations ./migrations
COPY configs ./configs

EXPOSE 8080

CMD ["./notification-service"]