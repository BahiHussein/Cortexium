const Cortexium = require('../../index');

async function startGateway() {
    const apiNode = new Cortexium({
        prefix: 'my-app',
        url: 'redis://127.0.0.1:6379',
        type: 'api-gateway',
    });

    await apiNode.ready();
    console.log('[API Gateway] is online and ready.');

    // After a short delay, emit a message to the 'add' topic.
    setTimeout(() => {
        console.log('[API Gateway] Sending request to add [15, 27]');
        apiNode.emit('add', [15, 27], (err, result) => {
            if (err) {
                console.error('[API Gateway] Received an error:', err);
                return;
            }
            console.log(`[API Gateway] SUCCESS! The result is: ${result}`); // Should be 42
        });
    }, 1000);
}

startGateway();
