# -------- Stage 1: build --------
FROM golang:1.25-alpine AS builder

WORKDIR /app

RUN apk add --no-cache git

#COPY go.work .
COPY pkg ./pkg
COPY notification-service ./notification-service

WORKDIR /app/notification-service

RUN go mod download

RUN go build -o /app/bin/notification-service ./cmd/app

# -------- Stage 2: runtime --------
FROM alpine:3.20

WORKDIR /app

RUN apk add --no-cache ca-certificates

COPY --from=builder /app/bin/notification-service ./notification-service

COPY --from=builder /app/notification-service/migrations ./migrations
COPY --from=builder /app/notification-service/configs ./configs

EXPOSE 8080

CMD ["./notification-service"]