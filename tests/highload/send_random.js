import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
    scenarios: {
        send_random_notifications: {
            executor: 'constant-arrival-rate',
            rate: 40,                    // ~40 уведомлений в секунду
            timeUnit: '1s',
            duration: '30s',             // всего отправим ~1200 уведомлений
            preAllocatedVUs: 20,
            maxVUs: 50,
        },
    },
    thresholds: {
        http_req_duration: ['p(95)<350'],
        http_req_failed: ['rate<0.02'],
    },
};

const TOTAL_USERS = 100;

export default function () {
    // Выбираем случайного пользователя от 0 до 99
    const userIndex = Math.floor(Math.random() * TOTAL_USERS);
    const userId = `load-user-${userIndex}`;

    const payload = JSON.stringify({
        user_id: userId,
        title: 'Random order test',
        body: `Random message for ${userId}`,
        type: 'message',
        priority: 'low',
        data: {
            random_test: true,
            sent_at: Date.now(),
        },
    });

    const res = http.post('http://localhost:8080/notifications', payload, {
        headers: { 'Content-Type': 'application/json' },
    });

    check(res, {
        'created': (r) => r.status === 200 || r.status === 201,
    });

    // Небольшая пауза, чтобы не было слишком жёсткого всплеска
    sleep(0.02);
}