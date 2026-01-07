const net = require('net');
const ws = net.createConnection({ host: '127.0.0.1', port: 3001 });

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
			send(response);
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

function send(response) {
	const text = JSON.stringify(response) + "\n"
	ws.write(text);
}

