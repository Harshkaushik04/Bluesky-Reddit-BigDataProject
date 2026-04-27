import { WebSocket } from "ws";
import * as fs from "fs";
import * as path from "path";

const BRONZE_DIR = "/mnt/d/Bluesky-Reddit-BigDataProject/Bluesky_data/initial_firehose";
const STREAMING_DIR = "mnt/d/Bluesky-Reddit-BigDataProject/Bluesky_data/streaming/firehose";

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

let ws: WebSocket | null = null;
let postCount = 0;
let interactionCount = 0;
let isConnecting = false;
let lastMessageTime = Date.now();

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
    console.log("[System] Connected. Writing bronze + streaming feeds.");
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
  });

  ws.on("error", (err) => console.error("[WebSocket Error]:", err.message));
  ws.on("close", () => {
    console.log("[System] Firehose closed.");
    isConnecting = false;
    ws = null;
  });
}

connect();

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

