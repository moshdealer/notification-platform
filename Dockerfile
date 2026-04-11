# -------- Stage 1: build --------
FROM golang:1.25-alpine AS builder

WORKDIR /app

# зависимости для сборки
RUN apk add --no-cache git

# сначала зависимости (кэшируется)
COPY go.mod go.sum ./
RUN go mod download

# потом весь код
COPY . .

# собираем бинарник
RUN go build -o app ./cmd/app/main.go


# -------- Stage 2: runtime --------
FROM alpine:3.20

WORKDIR /app

# добавим сертификаты (важно для прод)
RUN apk add --no-cache ca-certificates

# копируем только бинарь
COPY --from=builder /app/app .

# копируем миграции
COPY migrations ./migrations

# порт (если будет HTTP позже)
EXPOSE 8080

# запуск
CMD ["./app"]