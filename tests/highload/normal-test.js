import http from 'k6/http';
import ws from 'k6/ws';
import exec from 'k6/execution';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

const PROFILE = {
    startRps: 100,
    targetRps: 800,
    rampUpSeconds: 60,
    holdSeconds: 120,
    rampDownSeconds: 90,
    httpP95: 800,
    deliveryP95: 3000,
};

const NUM_USERS = 1500;
const WS_VUS = NUM_USERS;

const SEND_START_SECONDS = 20;
const DELIVERY_DRAIN_SECONDS = 30;
const MIN_SUCCESS_RATIO = 0.99;
const MAX_RECONNECT_ATTEMPTS = 4;

const WS_URL = 'ws://localhost:8080/ws';
const HTTP_URL = 'http://localhost:8080/notifications';

const TEST_RUN_ID = __ENV.TEST_RUN_ID;

if (!TEST_RUN_ID) {
    throw new Error(
        'TEST_RUN_ID is required, for example: TEST_RUN_ID=normal-001',
    );
}

const EXPECTED_NOTIFICATIONS = Math.round(
    ((PROFILE.startRps + PROFILE.targetRps / 2) / 2)
        * PROFILE.rampUpSeconds
    + ((PROFILE.targetRps / 2 + PROFILE.targetRps) / 2)
        * PROFILE.rampUpSeconds
    + PROFILE.targetRps * PROFILE.holdSeconds
    + (PROFILE.targetRps / 2) * PROFILE.rampDownSeconds,
);

const MIN_REQUIRED_NOTIFICATIONS = Math.floor(
    EXPECTED_NOTIFICATIONS * MIN_SUCCESS_RATIO,
);

const SEND_DURATION_SECONDS =
    PROFILE.rampUpSeconds * 2
    + PROFILE.holdSeconds
    + PROFILE.rampDownSeconds;

const WS_DURATION_SECONDS =
    SEND_START_SECONDS
    + SEND_DURATION_SECONDS
    + DELIVERY_DRAIN_SECONDS;

const deliveryLatency = new Trend('delivery_latency_ms', true);
const notificationsCreated = new Counter('notifications_created');
const messagesReceived = new Counter('messages_received');
const uniqueMessagesReceived = new Counter('unique_messages_received');
const duplicateMessages = new Counter('duplicate_messages');
const outOfOrderMessages = new Counter('out_of_order_messages');
const outOfOrderRate = new Rate('out_of_order_rate');
const invalidWsMessages = new Counter('invalid_ws_messages');
const foreignRunMessages = new Counter('foreign_run_messages');
const httpSuccessRate = new Rate('http_success');
const wsReconnects = new Counter('ws_reconnects');
const wsConnectionFailures = new Counter('ws_connection_failures');

const THRESHOLDS = {
    'http_req_duration{scenario:send_load}': [
        `p(95)<${PROFILE.httpP95}`,
    ],
    delivery_latency_ms: [
        `p(95)<${PROFILE.deliveryP95}`,
    ],
    http_success: ['rate>0.99'],
    notifications_created: [
        `count>=${MIN_REQUIRED_NOTIFICATIONS}`,
    ],
    unique_messages_received: [
        `count>=${MIN_REQUIRED_NOTIFICATIONS}`,
    ],
    duplicate_messages: ['count==0'],
    invalid_ws_messages: ['count==0'],
    ws_connection_failures: ['count==0'],
};

export const options = {
    scenarios: {
        ws_listeners: {
            executor: 'constant-vus',
            vus: WS_VUS,
            duration: `${WS_DURATION_SECONDS}s`,
            exec: 'listenWS',
            gracefulStop: '30s',
        },
        send_load: {
            executor: 'ramping-arrival-rate',
            startRate: PROFILE.startRps,
            timeUnit: '1s',
            preAllocatedVUs: 200,
            maxVUs: 800,
            startTime: `${SEND_START_SECONDS}s`,
            stages: [
                {
                    duration: `${PROFILE.rampUpSeconds}s`,
                    target: PROFILE.targetRps / 2,
                },
                {
                    duration: `${PROFILE.rampUpSeconds}s`,
                    target: PROFILE.targetRps,
                },
                {
                    duration: `${PROFILE.holdSeconds}s`,
                    target: PROFILE.targetRps,
                },
                {
                    duration: `${PROFILE.rampDownSeconds}s`,
                    target: 0,
                },
            ],
            exec: 'sendNotifications',
            gracefulStop: '30s',
        },
    },
    thresholds: THRESHOLDS,
};

export function sendNotifications() {
    const userIndex = Math.floor(Math.random() * NUM_USERS);
    const userId = `load-user-${userIndex}`;
    const createdAt = Date.now();

    const payload = JSON.stringify({
        user_id: userId,
        title: 'Load test notification',
        body: `Message at ${createdAt}`,
        type: 'message',
        priority: 'medium',
        data: {
            test_run_id: TEST_RUN_ID,
            created_at: createdAt,
        },
    });

    const response = http.post(HTTP_URL, payload, {
        headers: { 'Content-Type': 'application/json' },
    });

    const created = response.status === 201 || response.status === 200;

    httpSuccessRate.add(created);

    if (created) {
        notificationsCreated.add(1);
    }

    check(response, {
        'notification created': () => created,
    });
}

export function listenWS() {
    const userIndex = exec.scenario.iterationInTest % NUM_USERS;
    const userId = `load-user-${userIndex}`;
    const url = `${WS_URL}?user_id=${encodeURIComponent(userId)}&token=testtoken`;

    const receivedEventIDs = new Set();

    let lastEventID = 0;
    let reconnectAttempts = 0;
    let reconnectScheduled = false;
    let connectionFailureReported = false;
    let hasConnected = false;
    let stopping = false;
    let activeSocket = null;

    sleep(Math.random() * 12);

    const stopDelay =
        WS_DURATION_SECONDS * 1000
        - exec.instance.currentTestRunDuration
        - 250;

    if (stopDelay <= 0) {
        return;
    }

    setTimeout(() => {
        stopping = true;

        if (activeSocket) {
            activeSocket.close();
        }
    }, stopDelay);

    function handleMessage(rawData) {
        let message;

        try {
            message = JSON.parse(rawData);
        } catch {
            invalidWsMessages.add(1);
            return;
        }

        if (message.type === 'connected') {
            return;
        }

        const eventID = message.event_id;
        const messageData = message.payload?.data;

        if (messageData?.test_run_id !== TEST_RUN_ID) {
            foreignRunMessages.add(1);
            return;
        }

        if (!Number.isInteger(eventID) || !messageData) {
            invalidWsMessages.add(1);
            return;
        }

        invalidWsMessages.add(0);
        messagesReceived.add(1);

        const duplicate = receivedEventIDs.has(eventID);
        duplicateMessages.add(duplicate ? 1 : 0);

        if (duplicate) {
            return;
        }

        receivedEventIDs.add(eventID);
        uniqueMessagesReceived.add(1);

        const outOfOrder = lastEventID !== 0 && eventID < lastEventID;
        outOfOrderMessages.add(outOfOrder ? 1 : 0);
        outOfOrderRate.add(outOfOrder);

        if (eventID > lastEventID) {
            lastEventID = eventID;
        }

        const createdAt = messageData.created_at;
        if (Number.isFinite(createdAt)) {
            deliveryLatency.add(Date.now() - createdAt);
        }
    }

    function scheduleReconnect() {
        if (stopping || reconnectScheduled || connectionFailureReported) {
            return;
        }

        if (reconnectAttempts >= MAX_RECONNECT_ATTEMPTS) {
            connectionFailureReported = true;
            wsConnectionFailures.add(1);
            return;
        }

        const delay = 800 * (reconnectAttempts + 1);
        const reconnectDeadline =
            exec.instance.currentTestRunDuration + delay;

        if (reconnectDeadline >= WS_DURATION_SECONDS * 1000) {
            return;
        }

        reconnectAttempts++;
        reconnectScheduled = true;

        setTimeout(() => {
            reconnectScheduled = false;
            connect();
        }, delay);
    }

    function connect() {
        const response = ws.connect(url, {}, (socket) => {
            activeSocket = socket;

            socket.on('open', () => {
                if (hasConnected) {
                    wsReconnects.add(1);
                }

                hasConnected = true;
                reconnectAttempts = 0;
                connectionFailureReported = false;
                wsConnectionFailures.add(0);
            });

            socket.on('message', handleMessage);
            socket.on('close', () => {
                if (activeSocket === socket) {
                    activeSocket = null;
                }

                scheduleReconnect();
            });

            socket.on('error', () => {
                socket.close();
                scheduleReconnect();
            });
        });

        if (!response || response.status !== 101) {
            scheduleReconnect();
        }
    }

    connect();
}
