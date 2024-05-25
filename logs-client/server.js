const express = require("express");
const http = require("http");
const socketIo = require("socket.io");
const mongoose = require("mongoose");
const cors = require("cors");

mongoose
  .connect("mongodb://127.0.0.1:27017/logs", {
    socketTimeoutMS: 60000,
    connectTimeoutMS: 60000,
  })
  .then(() => console.log("Connected to MongoDB"))
  .catch((err) => console.error("Failed to connect to MongoDB:", err));

const logSchema = new mongoose.Schema(
  {
    timestamp: String,
    logLevel: String,
    message: String,
  },
  { collection: "logsCollection" }
);

const Log = mongoose.model("Log", logSchema);

const app = express();
const server = http.createServer(app);

app.use(
  cors({
    origin: "http://localhost:3000",
    methods: ["GET", "POST"],
    allowedHeaders: ["Content-Type"],
  })
);

const io = socketIo(server, {
  cors: {
    origin: "http://localhost:3000",
    methods: ["GET", "POST"],
  },
});

io.on("connection", async (socket) => {
  console.log("New client connected");

  try {
    const logs = await Log.find().sort({ timestamp: -1 }).limit(100).exec();
    socket.emit("initialLogs", logs);
  } catch (err) {
    console.error("Error fetching logs:", err);
  }

  // Polling for new logs
  const pollingInterval = setInterval(async () => {
    try {
      const latestLogs = await Log.find()
        .sort({ timestamp: -1 })
        .limit(1)
        .exec();
      const latestLog = latestLogs[0];
      if (latestLog) {
        socket.emit("newLog", latestLog);
      }
    } catch (err) {
      console.error("Error fetching new logs:", err);
    }
  }, 10000);

  socket.on("disconnect", () => {
    console.log("Client disconnected");
    clearInterval(pollingInterval);
  });
});

const PORT = process.env.PORT || 4001;
server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
