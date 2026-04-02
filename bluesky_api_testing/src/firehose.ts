import { WebSocket } from "ws";

// @ts-ignore
import { HttpsProxyAgent } from "https-proxy-agent";
import * as fs from "fs";
import * as path from "path";

// 1. Setup the Absolute Storage Directory
const BRONZE_DIR = "D:\\Bluesky-Reddit-BigDataProject\\Bluesky_data\\initial_firehose";

if (!fs.existsSync(BRONZE_DIR)) {
    fs.mkdirSync(BRONZE_DIR, { recursive: true });
}

// 2. Helper to generate the hourly filename (FIXED FOR LOCAL TIME)
function getCurrentFilename(): string {
    const now = new Date();
    // Force JavaScript to use your local system timezone for everything
    const year = now.getFullYear();
    const month = String(now.getMonth() + 1).padStart(2, '0'); // Months are 0-indexed
    const day = String(now.getDate()).padStart(2, '0');
    const hourStr = String(now.getHours()).padStart(2, '0');
    const dateStr = `${year}-${month}-${day}`;
    return path.join(BRONZE_DIR, `bluesky_${dateStr}_${hourStr}.jsonl`);
}

const JETSTREAM_URL = 'wss://jetstream2.us-east.bsky.network/subscribe' + 
                      '?wantedCollections=app.bsky.feed.post' + 
                      '&wantedCollections=app.bsky.feed.like' + 
                      '&wantedCollections=app.bsky.graph.follow';

// 4. Connect through the Psiphon local proxy
const agent = new HttpsProxyAgent('http://127.0.0.1:64257');

// Global state variables
let ws: WebSocket | null = null;
let postCount = 0;
let interactionCount = 0;
let isConnecting = false;
let lastMessageTime = Date.now(); // Used to detect silent firewall drops

// 2. Wrap the connection logic in a reusable function
function connect() {
    if (isConnecting) return;
    isConnecting = true;

    console.log("\n[System] Attempting to connect to Neo4j Firehose through Psiphon tunnel...");
    
    ws = new WebSocket(JETSTREAM_URL, { agent });

    ws.on('open', () => {
        console.log("[System] Connected successfully! Saving data to:", BRONZE_DIR);
        isConnecting = false;
        lastMessageTime = Date.now(); // Reset the timer on fresh connection
    });

    ws.on('message', (data: Buffer) => {
        lastMessageTime = Date.now(); // Update timer every time data arrives
        const rawString = data.toString();
        
        try {
            const payload = JSON.parse(rawString);
            if (payload.commit && payload.commit.collection === 'app.bsky.feed.post') {
                postCount++;
            } else {
                interactionCount++;
            }
        } catch (e) {
            // Ignore parse errors
        }

        const filename = getCurrentFilename();
        fs.appendFile(filename, rawString + '\n', (err) => {
            if (err) console.error("Disk Write Error:", err);
        });
    });

    ws.on('error', (err) => {
        console.error("[WebSocket Error]:", err.message);
        // We let the watchdog handle the actual reconnection to prevent instant retry loops
    });

    ws.on('close', () => {
        console.log("[System] Connection closed by server.");
        isConnecting = false;
        ws = null;
    });
}

// 3. Start the initial connection
connect();

// 4. The 1-Minute Watchdog
setInterval(() => {
    const now = Date.now();
    const timeSinceLastMessage = now - lastMessageTime;
    
    // Check if socket is null, closed, OR if we haven't seen data in 60 seconds
    const isSocketClosed = !ws || ws.readyState !== WebSocket.OPEN;
    const isGhostConnection = timeSinceLastMessage > 60000;

    if (isSocketClosed || isGhostConnection) {
        console.log(`\n[Watchdog] Connection dead. (Socket Closed: ${isSocketClosed}, Ghost Connection: ${isGhostConnection})`);
        console.log("[Watchdog] Forcing reconnect...");
        
        if (ws) {
            ws.terminate(); // Force kill the hanging socket to prevent memory leaks
            ws = null;
        }
        
        connect();
    } else {
        // Optional: comment this out if you don't want it cluttering your logs
        // console.log("[Watchdog] Connection is healthy."); 
    }
}, 60000);

// 5. Print stats to the terminal every 10 seconds
setInterval(() => {
    // Only print if we are actively connected
    if (ws && ws.readyState === WebSocket.OPEN) {
        console.log(`[Stats] New Posts: ${postCount} | New Likes/Follows: ${interactionCount}`);
    }
    // Reset counters
    postCount = 0;
    interactionCount = 0;
}, 10000);
