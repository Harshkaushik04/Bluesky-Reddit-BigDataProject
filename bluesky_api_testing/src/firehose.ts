import { WebSocket } from "ws";

// @ts-ignore
import { HttpsProxyAgent } from "https-proxy-agent";
import * as fs from "fs";
import * as path from "path";

// 1. Setup the Absolute Storage Directory
const BRONZE_DIR = "D:\\Bluesky-Reddit-BigDataProject\\Bluesky_data\\initial_firehose";

// Create the directory if it doesn't exist yet
if (!fs.existsSync(BRONZE_DIR)) {
    fs.mkdirSync(BRONZE_DIR, { recursive: true });
}

// 2. Helper to generate the hourly filename
function getCurrentFilename(): string {
    const now = new Date();
    // Creates files like: bluesky_2026-04-02_00.jsonl
    const dateStr = now.toISOString().split('T')[0];
    const hourStr = now.getHours().toString().padStart(2, '0');
    return path.join(BRONZE_DIR, `bluesky_${dateStr}_${hourStr}.jsonl`);
}

// 3. The Expanded Neo4j Jetstream URL (Posts, Likes, Follows)
const JETSTREAM_URL = 'wss://jetstream2.us-east.bsky.network/subscribe' + 
                      '?wantedCollections=app.bsky.feed.post' + 
                      '&wantedCollections=app.bsky.feed.like' + 
                      '&wantedCollections=app.bsky.graph.follow';

// 4. Connect through the Psiphon local proxy
const agent = new HttpsProxyAgent('http://127.0.0.1:64257');
const ws = new WebSocket(JETSTREAM_URL, { agent });

console.log("Starting Neo4j Firehose through Psiphon tunnel...");
console.log(`Saving data directly to: ${BRONZE_DIR}`);

// Keep track of counts for the terminal display
let postCount = 0;
let interactionCount = 0;

ws.on('message', (data: Buffer) => {
    const rawString = data.toString();
    
    // Parse it quickly just to update our terminal stats
    try {
        const payload = JSON.parse(rawString);
        if (payload.commit && payload.commit.collection === 'app.bsky.feed.post') {
            postCount++;
        } else {
            interactionCount++;
        }
    } catch (e) {
        // Ignore parse errors on the fly
    }

    // Write the raw string directly to the disk (newline delimited)
    const filename = getCurrentFilename();
    fs.appendFile(filename, rawString + '\n', (err) => {
        if (err) console.error("Disk Write Error:", err);
    });
});

// Print stats to the terminal every 10 seconds
setInterval(() => {
    console.log(`[Stats] New Posts: ${postCount} | New Likes/Follows: ${interactionCount}`);
    // Reset counters
    postCount = 0;
    interactionCount = 0;
}, 10000);

ws.on('error', (err) => console.error("WebSocket Error:", err));

ws.on('close', () => {
    console.log("Connection closed by server. If Fortinet dropped it, restart the script!");
});
