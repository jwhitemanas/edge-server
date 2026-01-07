import net from 'net';
import dotenv from 'dotenv';
import { createClient } from '@supabase/supabase-js';

dotenv.config();
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

let ws = null;
let connecting = false;
const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

function addClientEvent(eventData, callback) {
    sb.from('client_events').insert([{
        event: eventData.event,
        context: eventData.context,
        timestamp: eventData.timestamp,
        source: eventData.source, // must be 'web', 'android', or 'firmware'
        user_id: eventData.user_id,
        device_id: eventData.device_id
    }]).then(({ data, clientEventError }) => {
        switch (eventData.event) {
            case "em-raw": {
                const emData = eventData.data;
                sb.from('client_events_em_raw').insert([{
                    id: emData.id,
                    channel: emData.channel,
                    peak: emData.peak,
                    crossing: emData.crossing,
                    frequency: emData.frequency,
                    error: emData.error,
                    timestamp: emData.timestamp
                }]).then(({ data, emRawError }) => {
                    if (emRawError) {
                        callback(emRawError);
                    } else {
                        callback(null);
                    }
                }).catch((emRawError) => {
                    callback(emRawError);
                });
                break;
            } case "em-classic": {
                const emData = eventData.data;
                sb.from('client_events_em_classic').insert([{
                    id: emData.id,
                    peak: emData.peak,
                    null: emData.null,
                    compass: emData.compass,
                    depth: emData.depth,
                    timestamp: emData.timestamp
                }]).then(({ data, emClassicError }) => {
                    if (emClassicError) {
                        callback(emClassicError);
                    } else {
                        callback(null);
                    }
                }).catch((emClassicError) => {
                    callback(emClassicError);
                });
                break;
            } default: {
                if (clientEventError) {
                    callback(clientEventError);
                } else {
                    callback(null);
                }
                break;
            }
        }
    }).catch((clientEventError) => {
        callback(clientEventError);
    });
}

function connect(callback) {
    connecting = true;
    ws = net.createConnection({ host: '127.0.0.1', port: 3001 }, () => {
        connecting = false;
        console.log("Connection to websocket server established");
        callback();
    });
    ws.on('error', (err) => {
        console.error("IPC connection error:", err.message);
        connecting = false;
        ws.destroy();
        ws = null;
    });
    ws.on('close', () => {
        console.log("IPC connection closed");
        ws = null;
    });
}

const listener = net.createServer((socket) => {
    console.log("IPC connect");
    socket.setEncoding("utf8");
    socket.on("data", (data) => {
        const cleanData = data.toString().trim();
        const client = cleanData.slice(0, 36);
        const body = cleanData.slice(36);
        let msg;
        try {
            msg = JSON.parse(body);
        } catch (err) {
            console.error("Invalid JSON from local process:", err.message);
            return;
        }
        if (!msg.type || typeof msg.type !== "string") {
            console.error("Missing or invalid 'type' field");
            return;
        }
        if (typeof msg.data !== "object" || msg.data === null || Array.isArray(msg.data)) {
            console.error("Missing or invalid 'data' field");
            return;
        }
        switch (msg.type) {
            case "track-event": {
                addClientEvent(msg.data, (err) => {
                    if (!err) {
                        const response = {"client": client, "data": {"type": "response", "data": {"request": msg.id, "status": 200}}};
                        attemptSend(response);
                    } else {
                        console.error("Supabase insert failed: ", err.message);
                    }
                });
                break;
            }
        }
    });
    socket.on("close", () => {
        console.log("IPC closed");
    });
    socket.on("error", (err) => {
        console.error("IPC error: ", err);
    });
});

const port = 4000;
listener.listen(port, "127.0.0.1", () => {
    console.log(`IPC started on ${port}`);
});

function attemptSend(response) {
    if (!ws || ws.destroyed) {
        connect(() => {
            send(response);
        });
        return;
    }
    send(response);
}

function send(response) {
    const text = JSON.stringify(response) + "\n"
    try {
        ws.write(text);
    } catch (err) {
        console.error("Send failed: ", err.message);
        ws.destroy();
        ws = null;
    }
}

