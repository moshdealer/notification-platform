import http from 'k6/http';
import ws from 'k6/ws';
import { Trend, Counter, Rate } from 'k6/metrics';
import { check, sleep } from 'k6';

// ==================== НАСТРОЙКИ ====================
const NUM_USERS = 200;           // для отправки сообщений
const WS_VUS    = 201;           // для WebSocket подключений (на 1 больше)

const TARGET_RPS = 50;
const RAMP_UP_TIME = '30s';
const HOLD_TIME = '1m';
const RAMP_DOWN_TIME = '30s';

const WS_URL = 'ws://localhost:8080/ws';
const HTTP_URL = 'http://localhost:8080/notifications';

// ==================== МЕТРИКИ ====================
const deliveryLatency = new Trend('delivery_latency_ms', true);
const messagesReceived = new Counter('messages_received');
const messagesSent = new Counter('messages_sent');
const httpSuccessRate = new Rate('http_success');
const duplicateMessages = new Counter('duplicate_messages');

export const options = {
    scenarios: {
        ws_listeners: {
            executor: 'constant-vus',
            vus: WS_VUS,                    // ← используем 201
            duration: '5m',
            exec: 'listenWS',
            gracefulStop: '20s',
        },

        send_load: {
            executor: 'ramping-arrival-rate',
            startRate: 20,
            timeUnit: '1s',
            preAllocatedVUs: 10,
            maxVUs: 50,
            startTime: '12s',
            stages: [
                { duration: RAMP_UP_TIME, target: TARGET_RPS / 2 },
                { duration: RAMP_UP_TIME, target: TARGET_RPS },
                { duration: HOLD_TIME, target: TARGET_RPS },
                { duration: RAMP_DOWN_TIME, target: 0 },
            ],
            exec: 'sendNotifications',
            gracefulStop: '20s',
        },
    },

    thresholds: {
        http_req_duration: ['p(95)<600'],
        delivery_latency_ms: ['p(95)<2000'],
        http_success: ['rate>0.95'],
    },
};

// ==================== ОТПРАВКА ====================

export function sendNotifications() {
    const userIndex = Math.floor(Math.random() * NUM_USERS); // ← остаётся 200
    const userId = `load-user-${userIndex}`;
    const createdAt = Date.now();

    const payload = JSON.stringify({
        user_id: userId,
        title: 'Load test notification',
        body: `Message at ${createdAt}`,
        type: 'message',
        priority: 'medium',
        data: {
            created_at: createdAt,
            seq: __ITER,
        },
    });

    const res = http.post(HTTP_URL, payload, {
        headers: { 'Content-Type': 'application/json' },
    });

    messagesSent.add(1);
    httpSuccessRate.add(res.status === 201 || res.status === 200);

    check(res, {
        'notification created': (r) => r.status === 201 || r.status === 200,
    });
}

// ==================== WEBSOCKET СЛУШАТЕЛИ ====================
export function listenWS() {
    const userId = `load-user-${__VU - 1}`;
    const url = `${WS_URL}?user_id=${userId}&token=testtoken`;

    const receivedSeqs = new Set();
    let attempt = 0;
    const maxAttempts = 5;

    // Небольшая случайная задержка, чтобы не все подключались одновременно
    const initialDelay = Math.random() * 8;
    sleep(initialDelay);

    if (__VU % 20 === 0) {
        console.log(`[DEBUG] VU ${__VU} started for ${userId} (initial delay: ${initialDelay.toFixed(1)}s)`);
    }

    function tryConnect() {
        attempt++;

        if (attempt > 1) {
            console.log(`[WS] Reconnecting to ${userId} (attempt ${attempt}/${maxAttempts})`);
        } else {
            console.log(`[WS] Trying to connect: ${userId}`);
        }

        ws.connect(url, {}, function (socket) {
            socket.on('open', () => {
                console.log(`[WS] Connected successfully: ${userId} (attempt ${attempt})`);
                attempt = 0; // сбрасываем счётчик при успешном подключении
            });

            socket.on('message', (rawData) => {
                let msg;
                try {
                    msg = JSON.parse(rawData);
                } catch (e) {
                    return;
                }

                if (msg.type === 'connected') {
                    return;
                }

                messagesReceived.add(1);

                const createdAt = msg.data?.created_at || msg.created_at;
                if (createdAt) {
                    deliveryLatency.add(Date.now() - createdAt);
                }

                if (msg.data?.seq !== undefined && !receivedSeqs.has(msg.data.seq)) {
                    receivedSeqs.add(msg.data.seq);
                }
            });

            socket.on('close', () => {
                if (attempt < maxAttempts) {
                    const delay = 1000 * attempt; // увеличиваем задержку при каждой попытке
                    console.warn(`[WS] Connection closed for ${userId}. Reconnecting in ${delay}ms...`);
                    setTimeout(tryConnect, delay);
                } else {
                    console.error(`[WS] Failed to connect after ${maxAttempts} attempts: ${userId}`);
                }
            });

            socket.on('error', (e) => {
                console.error(`[WS Error] ${userId} (attempt ${attempt}):`, e);

                if (attempt < maxAttempts) {
                    const delay = 1000 * attempt;
                    setTimeout(tryConnect, delay);
                }
            });

            // Закрываем соединение в конце теста
            socket.setTimeout(() => {
                socket.close();
            }, 1000 * 60 * 25);
        });
    }

    tryConnect();
}