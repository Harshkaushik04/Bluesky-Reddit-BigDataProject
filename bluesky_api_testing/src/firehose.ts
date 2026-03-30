import { WebSocket } from "ws";

// Tell the strict TS compiler to ignore the missing type definitions for this one package
// @ts-ignore
import { HttpsProxyAgent } from "https-proxy-agent";

// Point the agent to the exact port Psiphon opened (8081)
const agent = new HttpsProxyAgent('http://127.0.0.1:8081');

// Pass the agent into the WebSocket connection
const ws = new WebSocket('wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post', { agent });

console.log("Sneaking through Fortinet via Psiphon...");

ws.on('message', (data) => {
    // Printing the first 200 characters so your terminal doesn't crash from the firehose volume
    console.log(JSON.stringify(JSON.parse(data.toString()),null,4)); 
    console.log("\n\n")
});

ws.on('error', (err) => console.error("WebSocket Error:", err));