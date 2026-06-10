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
        ws_connected: ['count >= 95'],
        messages_received: ['count >= 450'],
    },
};

export default function () {
    const userId = `load-user-${__VU - 1}`;
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

            // === Главное: структурированный вывод в лог ===
            console.log(JSON.stringify({
                event: 'message_received',
                userId: userId,
                time: new Date().toISOString(),
                message: data
            }));
        });

        socket.on('close', () => {
            console.log(JSON.stringify({
                event: 'disconnected',
                userId: userId,
                time: new Date().toISOString()
            }));
        });

        socket.on('error', (e) => {
            console.error(JSON.stringify({
                event: 'error',
                userId: userId,
                time: new Date().toISOString(),
                error: String(e)
            }));
        });

        socket.setTimeout(() => socket.close(), 85000);
    });
}