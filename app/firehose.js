"use strict";
exports.__esModule = true;
var ws_1 = require("ws");
var fs = require("fs");
var path = require("path");
var BRONZE_DIR = "/mnt/d/Bluesky-Reddit-BigDataProject/Bluesky_data/initial_firehose";
var STREAMING_DIR = "mnt/d/Bluesky-Reddit-BigDataProject/Bluesky_data/streaming/firehose";
[BRONZE_DIR, STREAMING_DIR].forEach(function (dir) {
    if (!fs.existsSync(dir))
        fs.mkdirSync(dir, { recursive: true });
});
function getCurrentFilename(baseDir) {
    var now = new Date();
    var year = now.getFullYear();
    var month = String(now.getMonth() + 1).padStart(2, "0");
    var day = String(now.getDate()).padStart(2, "0");
    var hourStr = String(now.getHours()).padStart(2, "0");
    var dateStr = "".concat(year, "-").concat(month, "-").concat(day);
    return path.join(baseDir, "bluesky_".concat(dateStr, "_").concat(hourStr, ".jsonl"));
}
var JETSTREAM_URL = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post&wantedCollections=app.bsky.feed.like&wantedCollections=app.bsky.graph.follow";
var ws = null;
var postCount = 0;
var interactionCount = 0;
var isConnecting = false;
var lastMessageTime = Date.now();
function persistRecord(rawString) {
    fs.appendFile(getCurrentFilename(BRONZE_DIR), rawString + "\n", function (err) {
        if (err)
            console.error("Bronze write failed:", err);
    });
    fs.appendFile(getCurrentFilename(STREAMING_DIR), rawString + "\n", function (err) {
        if (err)
            console.error("Streaming write failed:", err);
    });
}
function connect() {
    if (isConnecting)
        return;
    isConnecting = true;
    console.log("\n[System] Connecting to Bluesky Firehose...");
    ws = new ws_1.WebSocket(JETSTREAM_URL, { family: 4 });
    ws.on("open", function () {
        console.log("[System] Connected. Writing bronze + streaming feeds.");
        isConnecting = false;
        lastMessageTime = Date.now();
    });
    ws.on("message", function (data) {
        var _a;
        lastMessageTime = Date.now();
        var rawString = data.toString();
        try {
            var payload = JSON.parse(rawString);
            if (((_a = payload.commit) === null || _a === void 0 ? void 0 : _a.collection) === "app.bsky.feed.post")
                postCount++;
            else
                interactionCount++;
        }
        catch (_b) {
            // ignore
        }
        persistRecord(rawString);
    });
    ws.on("error", function (err) { return console.error("[WebSocket Error]:", err.message); });
    ws.on("close", function () {
        console.log("[System] Firehose closed.");
        isConnecting = false;
        ws = null;
    });
}
connect();
setInterval(function () {
    var isSocketClosed = !ws || ws.readyState !== ws_1.WebSocket.OPEN;
    var isGhostConnection = Date.now() - lastMessageTime > 60000;
    if (isSocketClosed || isGhostConnection) {
        console.log("[Watchdog] Reconnecting Firehose stream...");
        if (ws)
            ws.terminate();
        ws = null;
        connect();
    }
}, 60000);
setInterval(function () {
    if (ws && ws.readyState === ws_1.WebSocket.OPEN) {
        console.log("[Stats] Posts: ".concat(postCount, " | Interactions: ").concat(interactionCount));
    }
    postCount = 0;
    interactionCount = 0;
}, 10000);
