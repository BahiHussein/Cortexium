const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const Cortexium = require('../../index');

const PORT = 3000;
const PUBLIC_CHAT_TOPIC = 'public-chat-room';

// --- State Management for Robust Long-Polling ---
// We will store each client, their queue, and a potential cleanup timer.
const clients = new Map();

// --- Cortexium Setup ---
const cortexNode = new Cortexium({
    prefix: 'chat-app',
    url: 'redis://127.0.0.1:6379',
    type: 'chat-server',
});

// A single, persistent listener for messages from Cortexium.
cortexNode.sub(PUBLIC_CHAT_TOPIC, (message) => {
    console.log(`Message from Cortexium [${message.clientMessageId}]. Distributing to ${clients.size} clients.`);
    
    // Distribute the message to EVERY client's personal queue.
    for (const client of clients.values()) {
        client.queue.push(message);
        // If the client is actively waiting, fulfill their request immediately.
        if (client.response) {
            const res = client.response;
            const queuedMessage = client.queue.shift();
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify(queuedMessage));
            client.response = null; // Mark as fulfilled
        }
    }
    return "Message distributed";
});


// --- HTTP Server Setup ---
const server = http.createServer(async (req, res) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, X-Client-Id');

    if (req.method === 'OPTIONS') {
        res.writeHead(204);
        res.end();
        return;
    }
    
    // Serve the HTML file
    if (req.url === '/' && req.method === 'GET') {
        fs.readFile(path.join(__dirname, 'index.html'), (err, data) => {
            if (err) { res.writeHead(500); res.end('Error loading index.html'); return; }
            res.writeHead(200, { 'Content-Type': 'text/html' });
            res.end(data);
        });
    } 
    // Handle sending a new message
    else if (req.url === '/message' && req.method === 'POST') {
        let body = '';
        req.on('data', chunk => { body += chunk.toString(); });
        req.on('end', () => {
            try {
                const message = JSON.parse(body);
                console.log(`Publishing message ${message.clientMessageId} to Cortexium.`);
                cortexNode.emit(PUBLIC_CHAT_TOPIC, message);
                res.writeHead(200, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ status: 'Message sent' }));
            } catch (e) {
                res.writeHead(400); res.end('Invalid JSON');
            }
        });
    } 
    // Handle the long-polling connection
    else if (req.url === '/events' && req.method === 'GET') {
        const clientId = req.headers['x-client-id'] || crypto.randomUUID();
        res.setHeader('X-Client-Id', clientId);

        // Register the client or update their state if they are reconnecting.
        if (!clients.has(clientId)) {
            clients.set(clientId, { queue: [], response: null, cleanupTimer: null });
            console.log(`New client connected: ${clientId}. Total clients: ${clients.size}`);
        }

        const client = clients.get(clientId);
        
        // If they were disconnected, cancel their cleanup timer.
        if (client.cleanupTimer) {
            clearTimeout(client.cleanupTimer);
            client.cleanupTimer = null;
        }

        // If there are already messages in their personal queue, send one immediately.
        if (client.queue.length > 0) {
            const message = client.queue.shift();
            console.log(`Immediately sending queued message ${message.clientMessageId} to client ${clientId}`);
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify(message));
            return;
        }

        // Otherwise, store the response object to be fulfilled later.
        client.response = res;
        console.log(`Client ${clientId} is now waiting for a message.`);

        // When the client disconnects, we no longer delete them.
        // We start a timer to clean them up if they don't reconnect.
        req.on('close', () => {
            if (clients.has(clientId)) {
                const clientState = clients.get(clientId);
                clientState.response = null; // No longer actively listening
                
                // Set a grace period. If they don't reconnect, they're gone.
                clientState.cleanupTimer = setTimeout(() => {
                    console.log(`Cleaning up inactive client: ${clientId}`);
                    clients.delete(clientId);
                }, 30000); // 30-second grace period
            }
        });
    } 
    else {
        res.writeHead(404);
        res.end('Not Found');
    }
});

cortexNode.ready().then(() => {
    server.listen(PORT, () => {
        console.log(`Cortexium Chat Server running on http://localhost:${PORT}`);
    });
});

