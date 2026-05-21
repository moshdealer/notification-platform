# Notification Platform

**Микросервисная платформа уведомлений на Golang**

Два независимых сервиса:
- `notification-service` - управление уведомлениями (создание, хранение, отправка в Nats)
- `ws-notifier` - доставка уведомлений в реальном времени (получение из Nats, работа с WebSocket соединениями)

Проект демонстрирует чистую архитектуру, асинхронное взаимодействие через NATS JetStream, работу с PostgreSQL, Redis и Docker.

## Основные возможности

- Создание уведомлений через REST API
- Доставка в реальном времени через WebSocket
- Асинхронная обработка через NATS (Pub/Sub)
- Хранение статусов доставки в PostgreSQL
- Оффлайн пользователям сообщения доставляются по факту подключения с помощью Redis
- Полная контейнеризация (Docker + docker-compose)
- Миграции базы данных

## Запуск
`docker-compose up --build` - развернуть сервисы

Создать уведомление\
POST /notifications
```json
{
  "user_id": "12345",
  "title": "System update Notifice",
  "body": "Your system will be updated tonight at 02:00",
  "type": "message",
  "priority": "low",
  "data": {
    "source": "system",
    "version": "1.0.3",
    "action": "maintenance"
  }
}
```

После отправки открыть один из файлов test-ws.html (в зависимости от id юзера) - сообщение придет туда

## Архитектура
TBD
