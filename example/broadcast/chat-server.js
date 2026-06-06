const Cortexium = require('../../index');

async function startChatServer(serverId) {
    const chatNode = new Cortexium({
        prefix: 'chat-app',
        url: 'nats://127.0.0.1:4222',
        type: 'chat-server',
    });

    await chatNode.ready();
    console.log(`[Chat Server ${serverId}] Node ${chatNode.nodeId} is online.`);

    // Subscribe to broadcast events.
    // ALL chat servers will receive EVERY message (not load-balanced).
    await chatNode.subscribe('chat.room.general', (message) => {
        console.log(`[Chat Server ${serverId}] Received broadcast: ${message.user}: ${message.text}`);
    });

    // Simulate publishing messages after a delay
    setTimeout(() => {
        console.log(`[Chat Server ${serverId}] Broadcasting a message...`);
        chatNode.publish('chat.room.general', {
            user: `server_${serverId}`,
            text: 'Hello from chat server!',
        });
    }, 2000 + serverId * 500);
}

// Start two chat servers to demonstrate broadcast
startChatServer(1).catch(console.error);
startChatServer(2).catch(console.error);
