# -------- Stage 1: build --------
FROM golang:1.25-alpine AS builder

WORKDIR /app

RUN apk add --no-cache git

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o app ./cmd/app/main.go


# -------- Stage 2: runtime --------
FROM alpine:3.20

WORKDIR /app

RUN apk add --no-cache ca-certificates

COPY --from=builder /app/app .

COPY migrations ./migrations

EXPOSE 8080

CMD ["./app"]