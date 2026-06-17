# Notification Platform

**Микросервисная платформа уведомлений на Golang**

Два независимых сервиса:
- `notification-service` - управление уведомлениями (создание, хранение, отправка в Nats)
- `ws-notifier` - доставка уведомлений в реальном времени (получение из Nats, работа с WebSocket соединениями)

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

Также можно запустить нагрузочный тест k6\
Для этого необходимо воспользоваться командой
```shell
k6 run tests/highload/normal-test.js \
--out json=tests/highload/results/normal_test.json \
--summary-export=tests/highload/results/normal_test_summary.json
```

Результаты теста будут выведены после его окончания в терминал + файлы, указанные в команде\
Также я дополнительно использую sql-запрос, чтобы отслеживать в реальном времени через базу обработку сообщений
```sql
SELECT
COUNT(*) AS total_messages,
COUNT(*) FILTER (WHERE status = 'delivered') AS delivered,
COUNT(*) FILTER (WHERE status = 'sent') AS sent,
COUNT(*) FILTER (WHERE status = 'pending') AS pending,
COUNT(*) FILTER (WHERE status = 'waiting') AS waiting,
COUNT(*) FILTER (WHERE status = 'failed') AS failed
FROM outbox_events;
```

## Архитектура

Платформа состоит из двух независимых микросервисов, работающих через **NATS JetStream** + **PostgreSQL** + **Redis**.

### Компоненты

| Сервис                | Роль                                      | Масштабирование                                        | Ключевые технологии                       |
|-----------------------|-------------------------------------------|--------------------------------------------------------|-------------------------------------------|
| **notification-service** | Приём уведомлений по REST, надёжная публикация в NATS | Горизонтальное (только 1 инстанс запускает dispatcher) | Gin, GORM, NATS JetStream, Outbox Pattern |
| **ws-notifier**       | Реал-тайм доставка по WebSocket + offline storage | Горизонтальное (N инстансов)                           | Gorilla WebSocket, Redis, NATS JetStream  |
| **NATS JetStream**    | Надёжная асинхронная шина сообщений       | -                                                      | JetStream                                 |
| **PostgreSQL**        | Хранение уведомлений и outbox-событий     | -                                                      | GORM + Транзакции + Триггеры              |
| **Redis**             | Offline-сообщения + cross-node broadcast  | -                                                      | ZSET + TTL + Pub/Sub                      |
| **Nginx**             | Reverse proxy / Load balancer             | -                                                      | -                                         |

### Ключевые паттерны и решения

- **Outbox Pattern** (в `notification-service`)
    - При создании уведомления атомарно сохраняются `Notification` + `OutboxEvent` в одной транзакции.
    - Фоновые воркеры забирают события через `ClaimPendingOutboxEvents` (Postgres `UPDATE ... SKIP LOCKED`).
    - Это гарантирует доставку **хотя бы раз** даже при падении сервиса.

- **Горизонтальное масштабирование WebSocket**
    - Несколько инстансов `ws-notifier` держат соединения.
    - При получении сообщения из NATS:
        - Если пользователь онлайн **на этом инстансе** -> отправляем по WS + `MarkAsDelivered`.
        - Иначе -> сохраняем в Redis (`AddUnread`) + публикуем в Redis Pub/Sub broadcast.
    - Другие инстансы получают broadcast и пытаются доставить, если у них есть соединение пользователя. Если отправили, то удаляем из Redis сообщение
    - При подключении пользователя (`/ws?user_id=...`) догружаются все непрочитанные сообщения из Redis.

- **Приоритизация и TTL**
    - Высокоприоритетные сообщения имеют больший TTL в Redis.
    - Статусы: `pending -> sent -> delivered / waiting / failed / read / expired`.

- **Синхронизация статусов**
    - `OutboxSyncer` периодически синхронизирует состояния обратно в основную таблицу уведомлений.

### Упрощенный Data-flow системы
![Архитектура платформы](docs/images/DataFlow-2026-06-17-103318.png)