package messaging

// Subscriber - общий интерфейс для получения сообщений из брокера.
// Реализуют: NATS Subscriber и Kafka Subscriber.
type Subscriber interface {
	// Start запускает подписку на сообщения
	Start() error

	// Close останавливает подписку и закрывает соединение
	Close()
}
