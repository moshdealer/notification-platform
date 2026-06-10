import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
    scenarios: {
        send_500k_messages: {
            executor: 'ramping-arrival-rate',
            startRate: 100,
            timeUnit: '1s',
            preAllocatedVUs: 50,
            maxVUs: 200,
            stages: [
                { duration: '1m',   target: 300 },   // медленный разгон
                { duration: '2m',   target: 600 },
                { duration: '6m',   target: 900 },
                { duration: '30s',  target: 0 },
            ],
        },
    },
    thresholds: {
        http_req_duration: ['p(95)<500'],
        http_req_failed: ['rate<0.01'],
        checks: ['rate>0.99'],
    },
};

const TOTAL_USERS = 1000;

export default function () {
    // Равномерное распределение по пользователям
    const userIndex = Math.floor(Math.random() * TOTAL_USERS);
    const userId = `load-user-${userIndex}`;

    const payload = JSON.stringify({
        user_id: userId,
        title: 'Load test message',
        body: `Message for ${userId}`,
        type: 'message',
        priority: 'low',
        data: {
            test: true,
            sent_at: Date.now(),
        },
    });

    const res = http.post('http://localhost:8080/notifications', payload, {
        headers: { 'Content-Type': 'application/json' },
    });

    check(res, {
        'status is 200/201': (r) => r.status === 200 || r.status === 201,
    });

    // Небольшая пауза, чтобы не убивать CPU
    sleep(0.005);
}