const express = require('express');
const { faker } = require('@faker-js/faker');
const fs = require('fs');
const path = require('path');
const http = require('http');
const socketIo = require('socket.io');
const kafka = require('kafka-node');

const app = express();
const server = http.createServer(app);
const io = socketIo(server);

app.use(express.json());

const logFilePath = path.join(__dirname, 'logs.txt');
const logLevels = ['INFO', 'WARN', 'ERROR'];

function generateLog() {
  const timestamp = new Date().toISOString();
  const logLevel = logLevels[Math.floor(Math.random() * logLevels.length)];
  const message = faker.lorem.sentence();
  return { timestamp, logLevel, message };
}

function saveLogToFile(log) {
  const logEntry = `${log.timestamp} [${log.logLevel}] ${log.message}\n`;
  fs.appendFileSync(logFilePath, logEntry);
}

const client = new kafka.KafkaClient({ kafkaHost: 'localhost:9092' });
const producer = new kafka.Producer(client);

producer.on('ready', function() {
  console.log('Kafka Producer is connected and ready.');
});

producer.on('error', function(error) {
  console.error('Kafka Producer error:', error);
});

function produceLogToKafka(log) {
  const payloads = [
    { topic: 'logs', messages: JSON.stringify(log), partition: 0 }
  ];
  producer.send(payloads, function(err, data) {
    if (err) {
      console.error('Failed to send log to Kafka:', err);
    } else {
      console.log('Log sent to Kafka:', data);
    }
  });
}

function generateLogsAutomatically(interval) {
  setInterval(() => {
    const log = generateLog();
    saveLogToFile(log);
    io.emit('log', log); 
    produceLogToKafka(log);
    console.log('Generated log:', log);
  }, interval);
}

app.post('/logs', (req, res) => {
  const log = generateLog();
  saveLogToFile(log);
  io.emit('log', log); 
  produceLogToKafka(log);
  console.log('Generated log:', log);
  res.status(200).send('Log generated');
});

generateLogsAutomatically(10000);

const PORT = process.env.PORT || 4000;
server.listen(PORT, () => {
  console.log(`Log generator server is running on port ${PORT}`);
});

// const express = require('express');
// const { faker } = require('@faker-js/faker');
// const fs = require('fs');
// const path = require('path');
// const http = require('http');
// const socketIo = require('socket.io');
// const kafka = require('kafka-node');
// const { MongoClient } = require('mongodb');

// const app = express();
// const server = http.createServer(app);
// const io = socketIo(server);

// const uri = 'mongodb://localhost:27017';
// const client = new MongoClient(uri, { useNewUrlParser: true, useUnifiedTopology: true });

// const logFilePath = path.join(__dirname, 'logs.txt');
// const logLevels = ['INFO', 'WARN', 'ERROR'];

// function generateLog() {
//   const timestamp = new Date().toISOString();
//   const logLevel = logLevels[Math.floor(Math.random() * logLevels.length)];
//   const message = faker.lorem.sentence();
//   return { timestamp, logLevel, message };
// }

// function saveLogToFile(log) {
//   const logEntry = `${log.timestamp} [${log.logLevel}] ${log.message}\n`;
//   fs.appendFileSync(logFilePath, logEntry);
// }

// // Kafka Producer Setup
// const clientKafka = new kafka.KafkaClient({ kafkaHost: 'localhost:9092' });
// const producer = new kafka.Producer(clientKafka);

// producer.on('ready', function() {
//   console.log('Kafka Producer is connected and ready.');
// });

// producer.on('error', function(error) {
//   console.error('Kafka Producer error:', error);
// });

// function produceLogToKafka(log) {
//   const payloads = [
//     { topic: 'logs', messages: JSON.stringify(log), partition: 0 }
//   ];
//   producer.send(payloads, function(err, data) {
//     if (err) {
//       console.error('Failed to send log to Kafka:', err);
//     } else {
//       console.log('Log sent to Kafka:', data);
//     }
//   });
// }

// async function saveLogToMongoDB(log) {
//   try {
//     await client.connect();
//     const database = client.db('logDB');
//     const collection = database.collection('logs');
//     await collection.insertOne(log);
//   } finally {
//     await client.close();
//   }
// }

// function generateLogsAutomatically(interval) {
//   setInterval(async () => {
//     const log = generateLog();
//     saveLogToFile(log);
//     io.emit('log', log); // Send log to all connected clients
//     produceLogToKafka(log); // Send log to Kafka
//     await saveLogToMongoDB(log); // Save log to MongoDB
//     console.log('Generated log:', log);
//   }, interval);
// }

// app.post('/logs', async (req, res) => {
//   const log = generateLog();
//   saveLogToFile(log);
//   io.emit('log', log); // Send log to all connected clients
//   produceLogToKafka(log); // Send log to Kafka
//   await saveLogToMongoDB(log); // Save log to MongoDB
//   console.log('Generated log:', log);
//   res.status(200).send('Log generated');
// });

// // Fetch historical logs
// app.get('/logs', async (req, res) => {
//   try {
//     await client.connect();
//     const database = client.db('logDB');
//     const collection = database.collection('logs');
//     const logs = await collection.find({}).toArray();
//     res.status(200).json(logs);
//   } finally {
//     await client.close();
//   }
// });

// // Start generating logs automatically every second (1000 milliseconds)
// generateLogsAutomatically(10000);

// const PORT = process.env.PORT || 4000;
// server.listen(PORT, () => {
//   console.log(`Log generator server is running on port ${PORT}`);
// });
