import ws from 'k6/ws';
import { Counter } from 'k6/metrics';

const wsConnected = new Counter('ws_connected');
const messagesReceived = new Counter('messages_received');
const wsErrors = new Counter('ws_errors');

export const options = {
    scenarios: {
        hold_1000_users: {
            executor: 'constant-vus',
            vus: 1000,
            duration: '12m',
            gracefulStop: '30s',
        },
    },
    thresholds: {
        ws_connected: ['count >= 980'],
        messages_received: ['count > 400000'], // будет зависеть от отправки
        ws_errors: ['count < 50'],
    },
};

export default function () {
    const userId = `load-user-${__VU - 1}`; // от load-user-0 до load-user-999
    const url = `ws://localhost:8080/ws?user_id=${userId}&token=testtoken`;

    const res = ws.connect(url, {}, function (socket) {
        wsConnected.add(1);

        socket.on('open', () => {
            console.log(JSON.stringify({
                event: 'connected',
                userId: userId,
                time: new Date().toISOString()
            }));
        });

        socket.on('message', (data) => {
            messagesReceived.add(1);

            // Можно раскомментировать при отладке, но при 500k лучше отключить
            // console.log(JSON.stringify({ event: 'message_received', userId, time: new Date().toISOString() }));
        });

        socket.on('close', () => {
            console.log(JSON.stringify({
                event: 'disconnected',
                userId: userId,
                time: new Date().toISOString()
            }));
        });

        socket.on('error', (e) => {
            wsErrors.add(1);
            console.error(JSON.stringify({
                event: 'error',
                userId: userId,
                time: new Date().toISOString(),
                error: String(e)
            }));
        });

        // Держим соединение 11 минут
        socket.setTimeout(() => socket.close(), 11 * 60 * 1000);
    });
}