// performance-test.js - NATS-based throughput test
const Cortexium = require('./index');

const BATCH_SIZE = 100;
const latencies = [];
let successfulReplies = 0;
let failedReplies = 0;

async function runTest() {
    console.log(`--- Starting Cortexium NATS Throughput Test (Batch Size: ${BATCH_SIZE}) ---`);
    console.log('This test includes a warm-up phase to ensure accurate measurements.\n');

    const responderNode = new Cortexium({
        prefix: 'perf-app',
        url: 'nats://127.0.0.1:4222',
        type: 'responder-service',
    });

    const requestorNode = new Cortexium({
        prefix: 'perf-app',
        url: 'nats://127.0.0.1:4222',
        type: 'requestor-service',
    });

    console.log('Waiting for nodes to connect to NATS...');
    await Promise.all([
        responderNode.ready(),
        requestorNode.ready()
    ]);

    await responderNode.sub('ping', (payload) => 'pong');
    console.log('All nodes are ready.');

    console.log('Warming up the consumer...');
    await requestorNode.request('ping', { warmUp: true });
    console.log('Consumer is warm. Starting benchmark...\n');

    const batchStartTime = process.hrtime.bigint();

    console.log('Sending request batch all at once...');
    const promises = [];
    for (let i = 0; i < BATCH_SIZE; i++) {
        const promise = requestorNode.request('ping', { count: i })
            .then((result) => {
                successfulReplies++;
            })
            .catch((err) => {
                failedReplies++;
            })
            .finally(() => {
                // We can't easily track per-request duration with Promise.all
                // So we just track success/failure here
            });
        promises.push(promise);
    }

    await Promise.all(promises);

    const batchEndTime = process.hrtime.bigint();
    const totalBatchDurationMs = Number(batchEndTime - batchStartTime) / 1_000_000;
    const operationsPerSecond = (successfulReplies / totalBatchDurationMs * 1000).toFixed(2);

    console.log('\n--- Benchmark Complete ---');
    console.log(`\n--- Overall Performance ---`);
    console.log(`Total Test Duration: ${totalBatchDurationMs.toFixed(2)}ms`);
    console.log(`Throughput:          ${operationsPerSecond} ops/sec`);
    console.log(`\n--- Results ---`);
    console.log(`Successful Replies: ${successfulReplies}/${BATCH_SIZE}`);
    if (failedReplies > 0) {
        console.log(`Failed Replies:     ${failedReplies}/${BATCH_SIZE}`);
    }
    console.log('--------------------------\n');

    await Promise.all([
        responderNode.shutdown(),
        requestorNode.shutdown(),
    ]);

    process.exit(0);
}

runTest().catch((err) => {
    console.error('Test failed:', err);
    process.exit(1);
});
