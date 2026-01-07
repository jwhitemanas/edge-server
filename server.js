const net = require('net');
let ws = null;
let connecting = false;

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
    const text = data.toString().trim();
    let msg;
    try {
      msg = JSON.parse(text);
    } catch (err) {
      console.error("Invalid JSON from local process:", err.message);
      return;
    }
    if (!msg.id || typeof msg.id !== "string") {
      console.error("Missing or invalid 'id' field");
      return;
    }
    if (!msg.timestamp || typeof msg.timestamp !== "number") {
      console.error("Missing or invalid 'timestamp' field");
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
		case "track-event":
			const response = {"type": "response", "data": {"request": msg.id, "status": 200}};
			attemptSend(response);
			break;
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

