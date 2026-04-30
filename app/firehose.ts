import { WebSocket } from "ws";
import { Kafka, Producer } from "kafkajs";
import * as fs from "fs";
import * as path from "path";
import * as dotenv from "dotenv";

dotenv.config({ path: path.resolve(__dirname, "../.env") });

const BASE_DATA_DIR = process.env.BLUESKY_DATA_DIR 
  ? path.resolve(__dirname, "..", process.env.BLUESKY_DATA_DIR)
  : path.resolve(__dirname, "../Bluesky_data");

const BRONZE_DIR = path.join(BASE_DATA_DIR, "initial_firehose");
const STREAMING_DIR = path.join(BASE_DATA_DIR, "streaming/firehose");

[BRONZE_DIR, STREAMING_DIR].forEach((dir) => {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
});

function getCurrentFilename(baseDir: string): string {
  const now = new Date();
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, "0");
  const day = String(now.getDate()).padStart(2, "0");
  const hourStr = String(now.getHours()).padStart(2, "0");
  const dateStr = `${year}-${month}-${day}`;
  return path.join(baseDir, `bluesky_${dateStr}_${hourStr}.jsonl`);
}

const JETSTREAM_URL =
  "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post&wantedCollections=app.bsky.feed.like&wantedCollections=app.bsky.graph.follow";
const KAFKA_ENABLED = process.env.KAFKA_ENABLED === "true";
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || "localhost:9092")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);
const KAFKA_TOPIC = process.env.KAFKA_FIREHOSE_TOPIC || "bluesky.firehose.raw";

let ws: WebSocket | null = null;
let postCount = 0;
let interactionCount = 0;
let isConnecting = false;
let lastMessageTime = Date.now();
let kafkaProducer: Producer | null = null;

async function initKafkaProducer() {
  if (!KAFKA_ENABLED) return;
  const kafka = new Kafka({
    clientId: "bluesky-firehose-producer",
    brokers: KAFKA_BROKERS,
  });
  kafkaProducer = kafka.producer();
  await kafkaProducer.connect();
  console.log(
    `[System] Kafka producer connected. brokers=${KAFKA_BROKERS.join(",")} topic=${KAFKA_TOPIC}`
  );
}

async function publishToKafka(rawString: string) {
  if (!kafkaProducer) return;
  try {
    await kafkaProducer.send({
      topic: KAFKA_TOPIC,
      messages: [{ value: rawString }],
    });
  } catch (err) {
    console.error("[Kafka] Publish failed:", err);
  }
}

function persistRecord(rawString: string) {
  fs.appendFile(getCurrentFilename(BRONZE_DIR), rawString + "\n", (err) => {
    if (err) console.error("Bronze write failed:", err);
  });
  fs.appendFile(getCurrentFilename(STREAMING_DIR), rawString + "\n", (err) => {
    if (err) console.error("Streaming write failed:", err);
  });
}

function connect() {
  if (isConnecting) return;
  isConnecting = true;
  console.log("\n[System] Connecting to Bluesky Firehose...");
  ws = new WebSocket(JETSTREAM_URL, { family: 4 });

  ws.on("open", () => {
    console.log(
      `[System] Connected. Writing bronze + streaming feeds${KAFKA_ENABLED ? " + Kafka" : ""}.`
    );
    isConnecting = false;
    lastMessageTime = Date.now();
  });

  ws.on("message", (data: Buffer) => {
    lastMessageTime = Date.now();
    const rawString = data.toString();
    try {
      const payload = JSON.parse(rawString);
      if (payload.commit?.collection === "app.bsky.feed.post") postCount++;
      else interactionCount++;
    } catch {
      // ignore
    }
    persistRecord(rawString);
    void publishToKafka(rawString);
  });

  ws.on("error", (err) => console.error("[WebSocket Error]:", err.message));
  ws.on("close", () => {
    console.log("[System] Firehose closed.");
    isConnecting = false;
    ws = null;
  });
}

setInterval(() => {
  const isSocketClosed = !ws || ws.readyState !== WebSocket.OPEN;
  const isGhostConnection = Date.now() - lastMessageTime > 60000;
  if (isSocketClosed || isGhostConnection) {
    console.log("[Watchdog] Reconnecting Firehose stream...");
    if (ws) ws.terminate();
    ws = null;
    connect();
  }
}, 60000);

setInterval(() => {
  if (ws && ws.readyState === WebSocket.OPEN) {
    console.log(`[Stats] Posts: ${postCount} | Interactions: ${interactionCount}`);
  }
  postCount = 0;
  interactionCount = 0;
}, 10000);

void (async () => {
  try {
    await initKafkaProducer();
  } catch (err) {
    console.error("[Kafka] Producer initialization failed:", err);
  }
  connect();
})();

async function shutdown() {
  if (ws) ws.terminate();
  if (kafkaProducer) {
    try {
      await kafkaProducer.disconnect();
    } catch {
      // ignore shutdown errors
    }
  }
  process.exit(0);
}

process.on("SIGINT", () => {
  void shutdown();
});
process.on("SIGTERM", () => {
  void shutdown();
});

