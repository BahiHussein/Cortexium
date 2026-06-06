const Cortexium = require('../../index');

async function startGateway() {
    const apiNode = new Cortexium({
        prefix: 'my-app',
        url: 'nats://127.0.0.1:4222',
        type: 'api-gateway',
    });

    await apiNode.ready();
    console.log(`[API Gateway] Node ${apiNode.nodeId} is online and ready.`);

    // Example 1: RPC with callback (backward compatible)
    console.log('[API Gateway] Sending callback-based request to add [15, 27]');
    apiNode.emit('add', [15, 27], (err, result, duration) => {
        if (err) {
            console.error('[API Gateway] Error:', err.message);
            return;
        }
        console.log(`[API Gateway] SUCCESS! Result: ${result} (took ${duration}ms)`);
    });

    // Example 2: Promise-based RPC (modern API)
    setTimeout(async () => {
        try {
            console.log('[API Gateway] Sending Promise-based request to multiply [6, 7]');
            const result = await apiNode.request('multiply', [6, 7], { timeout: 5000 });
            console.log(`[API Gateway] SUCCESS! Multiply result: ${result}`);
        } catch (err) {
            console.error('[API Gateway] Request failed:', err.message);
        }
    }, 1000);

    // Example 3: Fire-and-forget (no reply expected, load-balanced)
    setTimeout(() => {
        console.log('[API Gateway] Sending fire-and-forget task');
        apiNode.emit('analytics.track', { event: 'page_view', path: '/home' });
    }, 2000);
}

startGateway().catch(console.error);
