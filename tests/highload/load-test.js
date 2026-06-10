import http from 'k6/http';
import ws from 'k6/ws';
import { Trend, Counter, Rate } from 'k6/metrics';
import { check } from 'k6';

const deliveryLatency = new Trend('delivery_latency_ms', true);
const messagesReceived = new Counter('messages_received');
const httpSuccessRate = new Rate('http_success');

const NUM_USERS = 1000;           // 1000 пользователей
const TEST_DURATION = '12m';

export const options = {
    scenarios: {
        // === Сценарий отправки (находим max throughput + создаём нагрузку) ===
        send_load: {
            executor: 'ramping-arrival-rate',
            startRate: 100,
            timeUnit: '1s',
            preAllocatedVUs: 30,
            maxVUs: 300,
            stages: [
                { duration: '2m', target: 300 },    // прогрев
                { duration: '2m', target: 600 },
                { duration: '2m', target: 1000 },   // цель — 1000 msg/s
                { duration: '4m', target: 1000 },   // удержание пика (здесь будет расти очередь)
                { duration: '2m', target: 0 },
            ],
            exec: 'sendNotifications',
        },

        // === Сценарий держания WebSocket соединений ===
        ws_listeners: {
            executor: 'constant-vus',
            vus: NUM_USERS,
            duration: TEST_DURATION,
            exec: 'listenWS',
            gracefulStop: '30s',
        },
    },

    thresholds: {
        http_req_duration: ['p(95)<800'],           // HTTP должен отвечать быстро
        delivery_latency_ms: ['p(99)<2000'],        // цель < 2с (можно ужесточить)
        http_success: ['rate>0.98'],
        messages_received: ['count>400000'],        // для 500k теста
    },
};

export function sendNotifications() {
    const userIndex = Math.floor(Math.random() * NUM_USERS);
    const userId = `load-user-${userIndex}`;
    const createdAt = Date.now(); // миллисекунды

    const payload = JSON.stringify({
        user_id: userId,
        title: 'Load test notification',
        body: `Message at ${createdAt}`,
        type: 'message',
        priority: 'low',
        data: {
            created_at: createdAt,
            seq: __ITER,
        },
    });

    const res = http.post('http://localhost:8080/notifications', payload, {
        headers: { 'Content-Type': 'application/json' },
    });

    httpSuccessRate.add(res.status === 200 || res.status === 201);

    check(res, {
        'status 2xx': (r) => r.status >= 200 && r.status < 300,
    });
}

export function listenWS() {
    const userId = `load-user-${__VU - 1}`;
    const url = `ws://localhost:8080/ws?user_id=${userId}&token=testtoken`;

    ws.connect(url, {}, function (socket) {
        socket.on('open', () => {
            // console.log(`WS connected: ${userId}`);
        });

        socket.on('message', (rawData) => {
            messagesReceived.add(1);

            try {
                const msg = JSON.parse(rawData);
                // Подстрой путь под реальную структуру сообщения из ws-notifier
                const createdAt = msg.data?.created_at || msg.created_at;

                if (createdAt) {
                    const latency = Date.now() - createdAt;
                    deliveryLatency.add(latency);
                }
            } catch (e) {
                // если сообщение не JSON или без timestamp — игнорируем
            }
        });

        socket.on('close', () => {});
        socket.on('error', (e) => console.error(`WS error ${userId}:`, e));

        // держим соединение до конца теста
        socket.setTimeout(() => socket.close(), 720000);
    });
}