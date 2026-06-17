import http from 'k6/http';
import ws from 'k6/ws';
import { Trend, Counter, Rate } from 'k6/metrics';
import { check, sleep } from 'k6';

// ==================== ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ДЛЯ ДУБЛИКАТОВ ====================
const duplicatesByUser = {};

// ==================== НАСТРОЙКИ ====================
const NUM_USERS = 1500;
const WS_VUS    = 1550;

const TARGET_RPS = 250;
const RAMP_UP_TIME = '1m';
const HOLD_TIME = '2m';
const RAMP_DOWN_TIME = '1m30s';

const WS_URL = 'ws://localhost:8080/ws';
const HTTP_URL = 'http://localhost:8080/notifications';

// ==================== МЕТРИКИ ====================
const deliveryLatency = new Trend('delivery_latency_ms', true);
const messagesReceived = new Counter('messages_received');
const messagesSent = new Counter('messages_sent');
const httpSuccessRate = new Rate('http_success');
const wsReconnects = new Counter('ws_reconnects');
const wsConnectionFailures = new Counter('ws_connection_failures');

export const options = {
    scenarios: {
        ws_listeners: {
            executor: 'constant-vus',
            vus: WS_VUS,
            duration: '5m30s',
            exec: 'listenWS',
            gracefulStop: '30s',
        },
        send_load: {
            executor: 'ramping-arrival-rate',
            startRate: 100,
            timeUnit: '1s',
            preAllocatedVUs: 50,
            maxVUs: 800,
            startTime: '20s',
            stages: [
                { duration: RAMP_UP_TIME, target: TARGET_RPS / 2 },
                { duration: RAMP_UP_TIME, target: TARGET_RPS },
                { duration: HOLD_TIME, target: TARGET_RPS },
                { duration: RAMP_DOWN_TIME, target: 0 },
            ],
            exec: 'sendNotifications',
            gracefulStop: '30s',
        },
    },
    thresholds: {
        http_req_duration: ['p(95)<800'],
        delivery_latency_ms: ['p(95)<3000'],
        http_success: ['rate>0.90'],
        messages_received: ['count>58000'],
    },
};

// ==================== ОТПРАВКА ====================
export function sendNotifications() {
    const userIndex = Math.floor(Math.random() * (NUM_USERS+1));
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

    let attempt = 0;
    const maxAttempts = 4;

    // Инициализируем статистику по пользователю
    if (!duplicatesByUser[userId]) {
        duplicatesByUser[userId] = {
            received: new Set(),
            duplicatesCount: 0,
        };
    }

    const userStats = duplicatesByUser[userId];

    sleep(Math.random() * 12);

    function tryConnect() {
        attempt++;

        ws.connect(url, {}, function (socket) {
            socket.on('open', () => {
                if (attempt > 1) wsReconnects.add(1);
                attempt = 0;
            });

            socket.on('message', (rawData) => {
                let msg;
                try {
                    msg = JSON.parse(rawData);
                } catch (e) {
                    return;
                }

                if (msg.type === 'connected') return;

                messagesReceived.add(1);

                const createdAt = msg.data?.created_at || msg.created_at;
                if (createdAt) {
                    deliveryLatency.add(Date.now() - createdAt);
                }

                // === Отслеживание дублей ===
                if (msg.data?.seq !== undefined) {
                    const seq = msg.data.seq;

                    if (userStats.received.has(seq)) {
                        userStats.duplicatesCount++;
                    } else {
                        userStats.received.add(seq);
                    }
                }
            });

            socket.on('close', () => {
                if (attempt < maxAttempts) {
                    setTimeout(tryConnect, 800 * attempt);
                } else {
                    wsConnectionFailures.add(1);
                }
            });

            socket.on('error', (e) => {
                if (attempt < maxAttempts) {
                    setTimeout(tryConnect, 800 * attempt);
                } else {
                    wsConnectionFailures.add(1);
                }
            });

            socket.setTimeout(() => socket.close(), 1000 * 60 * 30);
        });
    }

    tryConnect();
}

// ==================== ОТЧЁТ ПО ДУБЛИКАТАМ ====================
export function teardown() {
    console.log('\n========== DUPLICATES REPORT ==========');

    let totalDuplicates = 0;
    let usersWithDuplicates = 0;
    const usersWithDupsList = [];

    for (const [userId, stats] of Object.entries(duplicatesByUser)) {
        if (stats.duplicatesCount > 0) {
            usersWithDuplicates++;
            totalDuplicates += stats.duplicatesCount;

            usersWithDupsList.push({
                userId,
                duplicates: stats.duplicatesCount,
            });
        }
    }

    console.log(`Total users with duplicates : ${usersWithDuplicates}`);
    console.log(`Total duplicate deliveries  : ${totalDuplicates}`);

    if (usersWithDupsList.length > 0) {
        console.log('\nTop users with duplicates:');
        usersWithDupsList
            .sort((a, b) => b.duplicates - a.duplicates)
            .slice(0, 15)
            .forEach((u, i) => {
                console.log(`${i + 1}. ${u.userId} - ${u.duplicates} duplicates`);
            });
    } else {
        console.log('\nNo duplicates were detected during the test!');
    }

    console.log('=======================================\n');
}