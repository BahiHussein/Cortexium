const Cortexium = require('../../index');

async function startService() {
    const node = new Cortexium({
        prefix: 'my-app',
        url: 'nats://127.0.0.1:4222',
        type: 'payment-service',
    });

    // Register middleware BEFORE subscribing.
    // This middleware runs for ALL topics matching the pattern.
    node.use('payments.*', async (ctx, next) => {
        const start = Date.now();
        console.log(`[Middleware] Request starting for topic`);
        await next();
        console.log(`[Middleware] Request completed in ${Date.now() - start}ms`);
    });

    // Auth middleware for sensitive topics
    node.use('payments.process', async (ctx, next) => {
        const apiKey = ctx.message.headers?.['x-api-key'];
        if (!apiKey) {
            throw Object.assign(new Error('Unauthorized'), { code: 'UNAUTHORIZED' });
        }
        console.log(`[Middleware] Auth passed for ${ctx.message.correlationId}`);
        await next();
    });

    await node.ready();
    console.log(`[Payment Service] Node ${node.nodeId} is online.`);

    // Subscribe to payment processing
    await node.sub('payments.process', async (payload, ctx) => {
        console.log(`[Payment Service] Processing payment:`, payload);
        // Simulate processing
        await new Promise(r => setTimeout(r, 100));
        return { status: 'approved', transactionId: 'txn_12345' };
    });
}

startService().catch(console.error);
