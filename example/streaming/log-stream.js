// Streaming RPC example: a service streams log lines to a client over time.
// Start a NATS server first:  nats-server -js
const Cortexium = require('../../index');

const URL = 'nats://127.0.0.1:4222';
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function main() {
    const service = new Cortexium({ prefix: 'demo', url: URL, type: 'log-service' });
    await service.ready();

    // Streaming handler. IMPORTANT: keep the handler awaiting until you're done
    // streaming — the stream stays open while the handler's promise is pending,
    // and ends when you call stream.end() (or when the handler returns).
    await service.sub('logs.tail', async (req, ctx) => {
        const stream = ctx.stream();
        const lines = req.lines || 5;
        for (let i = 0; i < lines; i++) {
            await sleep(300);
            stream.send({ line: i, msg: `log entry ${i}`, ts: Date.now() });
        }
        stream.end();
    });
    console.log('[service] log-service online.');

    const client = new Cortexium({ prefix: 'demo', url: URL, type: 'client' });
    await client.ready();

    console.log('[client] tailing logs...');
    for await (const entry of client.requestStream('logs.tail', { lines: 5 })) {
        console.log('[client] received:', entry);
    }
    console.log('[client] stream ended.');

    await Promise.all([service.shutdown(), client.shutdown()]);
    process.exit(0);
}

main().catch((e) => { console.error(e); process.exit(1); });
