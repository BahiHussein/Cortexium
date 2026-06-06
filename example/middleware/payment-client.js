const Cortexium = require('../../index');

async function startClient() {
    const node = new Cortexium({
        prefix: 'my-app',
        url: 'nats://127.0.0.1:4222',
        type: 'payment-client',
    });

    await node.ready();
    console.log(`[Payment Client] Node ${node.nodeId} is online.`);

    setTimeout(async () => {
        try {
            // Request WITH auth header
            const result = await node.request('payments.process', {
                amount: 100,
                currency: 'USD',
            });
            console.log('[Payment Client] Success:', result);
        } catch (err) {
            console.error('[Payment Client] Error:', err.code, err.message);
        }
    }, 1000);
}

startClient().catch(console.error);
