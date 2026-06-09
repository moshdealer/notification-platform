import ws from 'k6/ws';
import { Counter } from 'k6/metrics';

const wsConnected = new Counter('ws_connected');
const messagesReceived = new Counter('messages_received');

export const options = {
    scenarios: {
        hold_100_users: {
            executor: 'constant-vus',
            vus: 100,
            duration: '90s',
            gracefulStop: '10s',
        },
    },
    thresholds: {
        ws_connected: ['count >= 95'],           // минимум 95 успешных подключений
        messages_received: ['count >= 450'],     // минимум 450 сообщений дошло (90%)
    },
};

export default function () {
    const userId = `load-user-${__VU - 1}`; // load-user-0 ... load-user-99
    const url = `ws://localhost:8080/ws?user_id=${userId}&token=testtoken`;

    const res = ws.connect(url, {}, function (socket) {
        wsConnected.add(1);

        socket.on('open', () => {
            console.log(`[${userId}] connected`);
        });

        socket.on('message', (data) => {
            messagesReceived.add(1);
            // Можно раскомментировать для отладки:
            // console.log(`[${userId}] received: ${data}`);
        });

        socket.on('close', () => console.log(`[${userId}] disconnected`));
        socket.on('error', (e) => console.error(`[${userId}] error:`, e));

        // Держим соединение открытым до конца теста
        socket.setTimeout(() => {
            socket.close();
        }, 85000);
    });

    // Проверка, что подключение успешно установлено
    if (res && res.status === 101) {
        // соединение живое
    }
}