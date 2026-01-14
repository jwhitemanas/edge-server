import net from 'net';
import dotenv from 'dotenv';
import { createClient } from '@supabase/supabase-js';
import { v4 as uuidv4 } from 'uuid';

dotenv.config();
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

let ws = null;
let connecting = false;
const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

function addEmRaw(eventId, emData, callback) {
    sb.from('client_events_em_raw').insert([{
        event_id: eventId,
        channel: emData.channel,
        peak: emData.peak,
        crossing: emData.crossing,
        frequency: emData.frequency,
        error: emData.error,
        timestamp: emData.timestamp
    }]).then(({ data, error }) => {
        if (error) {
            callback(error);
        } else {
            callback(null);
        }
    }).catch((err) => {
        callback(err);
    });
}

function addEmClassic(eventId, emData, callback) {
    sb.from('client_events_em_classic').insert([{
        event_id: eventId,
        peak: emData.peak,
        "null": emData["null"],
        compass: emData.compass,
        depth: emData.depth,
        timestamp: emData.timestamp
    }]).then(({ data, error }) => {
        if (error) {
            callback(error);
        } else {
            callback(null);
        }
    }).catch((err) => {
        callback(err);
    });
}

function addClientEventData(eventId, eventData, callback) {
    switch (eventData.event) {
        case "em-raw": {
            addEmRaw(eventId, eventData.data, callback);
            break;
        } case "em-classic": {
            addEmClassic(eventId, eventData.data, callback);
            break;
        } default: {
            callback(null);
            break;
        }
    }
}

function checkFingerprint(fingerprint, done, forward) {
    sb.from('client_events')
        .select('*', { count: 'exact', head: true })
        .eq('fingerprint', fingerprint)
        .then(({ count, error }) => {
            const exists = count > 0
            if (error) {
                console.error("Fingerprint DB failure: ", error.message);
            }
            if (exists) {
                console.log("Fingerprint found, no duplicate entry for client event made.");
                done();
            } else {
                forward();
            }
        }).catch(({ err }) => {
            console.error("Fingerprint request failure: ", err.message);
        });
}

function addClientEvent(eventData, callback) {
    const eventId = uuidv4();
    console.log("New event with fingerprint ", eventData.fingerprint);
    sb.from('client_events').insert([{
        id: eventId,
        fingerprint: eventData.fingerprint,
        event: eventData.event,
        context: eventData.context,
        timestamp: eventData.timestamp,
        source: eventData.source, // must be 'web', 'android', or 'firmware'
        user_id: eventData.user_id,
        device_id: eventData.device_id
    }]).then(({ data, error }) => {
        if (error) {
            callback(error);
        } else {
            addClientEventData(eventData.fingerprint, eventData, callback);
        }
    }).catch((err) => {
        callback(err);
    });
}

function upsertClientEvent(eventData, callback) {
    checkFingerprint(eventData.fingerprint, callback, () => {
        addClientEvent(eventData, callback);
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

function processMessage(id, client, type, item) {
    switch (type) {
        case "client_event": {
            upsertClientEvent(item, (err) => {
                if (!err) {
                    const response = {"client": client, "data": {"type": "response", "data": {"request": id, "status": 200}}};
                    attemptSend(response);
                } else {
                    console.error("Supabase insert failed: ", err.message);
                }
            });
            break;
        }
    }
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
        if (msg.data === null || (typeof msg.data !== "object" && !Array.isArray(msg.data))) {
            console.error("Missing or invalid 'data' field");
            return;
        }
        if (Array.isArray(msg.data)) {
            msg.data.forEach((item, index) => {
                processMessage(msg.id, client, msg.type, item);
            });
        } else {
            processMessage(msg.id, client, msg.type, msg.data);
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

