# Notification Platform

Распределённая платформа доставки уведомлений на Go

Основной транспорт проекта - **NATS JetStream**. Поддержка **Apache Kafka добавлена экспериментально**: она реализует тот же общий контракт и используется для сравнительного нагрузочного тестирования, но пока не считается основным production-путём.

Платформа принимает уведомления через REST API, атомарно сохраняет их в PostgreSQL вместе с outbox-событием, публикует в выбранный брокер и доставляет пользователю через WebSocket. Для offline-доставки и взаимодействия между экземплярами `ws-notifier` используется Redis.

Подробное описание устройства системы находится в [ARCHITECTURE.md](ARCHITECTURE.md).

## Возможности

- REST API для создания уведомлений.
- Transactional outbox: уведомление и событие создаются в одной транзакции PostgreSQL.
- Общий интерфейс публикации и подписки для брокеров.
- NATS JetStream pull consumer с explicit ACK, NakWithDelay и Term.
- Экспериментальный Kafka consumer group с ручным commit offset после обработки.
- Пять экземпляров `ws-notifier` и горизонтальное масштабирование WebSocket.
- Четыре API-реплики `notification-service` за Nginx.
- Отдельный экземпляр `notification-service` с четырьмя outbox-воркерами.
- Offline-хранилище уведомлений в Redis.
- Redis Pub/Sub для доставки между экземплярами `ws-notifier`.
- Graceful shutdown, структурированные логи и Prometheus-метрики.
- k6-сценарии для end-to-end проверки HTTP -> outbox -> broker -> WebSocket.

## Технологии

- Go 1.25;
- Gin и GORM;
- PostgreSQL 15;
- NATS JetStream 2.10;
- Apache Kafka 3.7 и franz-go;
- Redis 7;
- Gorilla WebSocket;
- Nginx;
- Docker Compose;
- Prometheus;
- k6.

## Основной поток данных

```text
Client
  -> Nginx
  -> notification-service
  -> PostgreSQL: notification + outbox event
  -> Outbox Dispatcher
  -> NATS JetStream или экспериментальный Kafka adapter
  -> ws-notifier
  -> WebSocket

Если пользователь находится на другой ноде:
  ws-notifier
    -> Redis unread
    -> Redis Pub/Sub broadcast
    -> нода с WebSocket пользователя
```

## Выбор брокера

Один и тот же тип брокера должен быть указан в конфигурации обоих сервисов:

- `notification-service/configs/config.yaml`;
- `ws-notifier/configs/config.yaml`.

Пример:
```yaml
broker_type: "nats"
```
либо
```yaml
broker_type: "kafka"
```

Docker Compose запускает оба брокера, но приложение использует только выбранный адаптер.

Kafka topic создаётся с пятью partitions только при первом запуске. Существующий topic автоматически не расширяется.

## Запуск

Создаем .env из env.example

Затем
```shell
docker compose up --build
```

REST API и WebSocket доступны через Nginx:

- REST API: `http://localhost:8080`;
- WebSocket: `ws://localhost:8080/ws`;
- healthcheck: `http://localhost:8080/health`.

## Создание уведомления

```shell
curl -X POST http://localhost:8080/notifications \
  -H 'Content-Type: application/json' \
  -d '{
    "user_id": "12345",
    "title": "System update",
    "body": "Your system will be updated tonight",
    "type": "message",
    "priority": "medium",
    "data": {
      "source": "system"
    }
  }'
```

WebSocket-подключение:

```text
ws://localhost:8080/ws?user_id=12345&token=testtoken
```

Текущая проверка token является заглушкой и предназначена только для локального стенда.

## Нагрузочные тесты

Тесты используют 1500 постоянных WebSocket-подключений и отдельный `ramping-arrival-rate` сценарий для REST API.

| Сценарий | Профиль | Target RPS | Удержание target | Плановый объём |
|---|---|---:|---:|---:|
| `normal-test.js` | штатная нагрузка | 800 | 120 секунд | 183 000 |
| `stress-test.js` | стресс-нагрузка | 2000 | 120 секунд | 397 500 |

Каждый запуск требует уникальный `TEST_RUN_ID`, чтобы сообщения предыдущих прогонов не попадали в метрики.

NATS:

```shell
k6 run -e TEST_RUN_ID=nats-normal-800-001 tests/highload/normal-test.js

k6 run -e TEST_RUN_ID=nats-stress-2000-001 tests/highload/stress-test.js
```

Kafka:

```shell
k6 run -e TEST_RUN_ID=kafka-normal-800-001 tests/highload/normal-test.js

k6 run -e TEST_RUN_ID=kafka-stress-2000-001 tests/highload/stress-test.js
```

## Результаты нагрузочного тестирования

Локальный Docker Compose, 21 июля 2026 года, 1500 WebSocket-клиентов.

| Профиль | Брокер | Создано | Уникально доставлено | Delivery p95 | HTTP p95 | Дубли | Dropped k6 |
|---|---|---:|---:|---:|---:|---:|---:|
| Normal 800 | NATS | 182 999 | 182 999 | 331 ms | 1.71 ms | 0 | 0 |
| Normal 800 | Kafka, experimental | 182 999 | 182 999 | 334 ms | 1.75 ms | 0 | 0 |
| Stress 2000 | NATS | 397 394 | 397 394 | 249 ms | 2.21 ms | 0 | 105 |
| Stress 2000 | Kafka, experimental | 397 499 | 397 499 | 240 ms | 2.50 ms | 0 | 0 |

Во всех четырёх прогонах каждое созданное уведомление было получено через WebSocket ровно один раз.

`dropped_iterations` в NATS stress означает, что k6 не успел запустить 105 HTTP-итераций. Эти запросы не поступали в приложение и не являются потерей сообщений брокером.

Target RPS удерживается только на отдельной стадии профиля. Средняя скорость, которую k6 выводит за полное время теста, включает ramp-up, ramp-down, задержку старта и delivery drain, поэтому она ниже target RPS.

Результаты являются локальным сравнением реализаций, а не production capacity benchmark.

## Проверяемые свойства

k6 отдельно контролирует:

- успешное создание уведомлений;
- количество уникально полученных событий;
- end-to-end latency от создания payload до WebSocket;
- дубли;
- некорректные WebSocket-сообщения;
- окончательные ошибки WebSocket-подключений;
- диагностическую долю сообщений, полученных не по возрастанию `event_id`.

## Статусы уведомления

Используемые состояния:

```text
pending
  -> sent
  -> delivered
  -> read

sent
  -> waiting
  -> delivered

sent
  -> failed / expired
```

`OutboxSyncer` переносит изменения статуса из `outbox_events` в основную таблицу `notifications`.

## Известные проблемы

Ниже перечислены осознанные ограничения текущей версии. Они не скрываются и запланированы к устранению на следующих этапах развития проекта.

### WebSocket ACK означает enqueue, а не подтверждённую доставку

`SendToUser` возвращает успех после помещения сообщения во внутренний буфер WebSocket writer. Фактическая запись в socket происходит позже и может завершиться ошибкой. Статус `delivered` и broker ACK/commit сейчас выставляются после enqueue.

Планируемое исправление: определить точную границу доставки и добавить подтверждение от writer либо application-level ACK клиента. До этого момента `delivered` следует понимать как «принято внутренним WebSocket pipeline».

### NATS callback последовательный внутри одной ноды

`Consume` в используемой версии `nats.go` вызывает обработчик последовательно. `PullMaxMessages(64)` задаёт prefetch, а не 64 параллельных обработчика. В текущем Docker Compose системный параллелизм всё же есть: пять `ws-notifier` совместно используют один durable pull consumer.

Планируемое исправление при необходимости дальнейшего роста: ограниченный worker pool с шардированием по `userID`, чтобы увеличить параллелизм и не нарушить порядок одного пользователя.

### Redis broadcast создаёт N-кратный трафик

Сообщение для пользователя, подключённого к другой ноде, публикуется в общий Redis Pub/Sub channel. Его получают и разбирают все экземпляры `ws-notifier`.

Планируемое исправление: маппинг `userID -> nodeID`, адресные каналы или очереди и fallback в unread storage.

### Авторизация WebSocket является заглушкой

Любой непустой token принимается, а `userID` берётся из query-параметра. Это допустимо только для локального тестового стенда.

Планируемое исправление: полноценная проверка JWT/session token

### Outbox crash gap

Claim переводит событие в `sent` до вызова broker publisher. Обычная ошибка публикации возвращает запись в `pending`, но авария процесса между claim и публикацией может оставить событие в `sent`.

Планируемое исправление: статус `processing` с lease/timeout и автоматическим возвратом зависших событий в `pending`.

### Порядок сообщений

Строгий порядок уведомлений одного пользователя пока не гарантируется. В последних тестах диагностическая доля reorder составила 1.10–1.31%.

Планируемое исправление: per-user sequence и bounded processing с шардированием по `userID`.

### Offline cleanup

GetUnread асинхронно запускает очистку Redis сразу после чтения сообщений, до подтверждённого enqueue и доставки. Cleanup также использует поиск Redis-ключей по шаблону

Планируемое исправление: удаление конкретного `eventID` только после подтверждённой доставки и отказ от `KEYS` на delivery-path.

### Testing и инфраструктура

- Автоматические Go unit/integration tests пока не добавлены; положительный путь проверяется end-to-end сценариями k6.
- Локальный Docker Compose использует одиночные экземпляры PostgreSQL, Redis, NATS и Kafka без production-grade replication и failover.
