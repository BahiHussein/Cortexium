// performance-test.js
const Cortexium = require('./index');

const BATCH_SIZE = 100;
const latencies = [];
let successfulReplies = 0;
let failedReplies = 0;

async function runTest() {
    console.log(`--- Starting Cortexium THROUGHPUT Test (Batch Size: ${BATCH_SIZE}) ---`);
    console.log('This test includes a warm-up phase to ensure accurate measurements.\n');

    const responderNode = new Cortexium({
        prefix: 'perf-app',
        url: 'redis://127.0.0.1:6379',
        type: 'responder-service',
    });

    const requestorNode = new Cortexium({
        prefix: 'perf-app',
        url: 'redis://127.0.0.1:6379',
        type: 'requestor-service',
    });

    responderNode.sub('ping', (payload) => 'pong');

    console.log('Waiting for nodes to connect to Redis...');
    await Promise.all([
        responderNode.ready(),
        requestorNode.ready()
    ]);
    console.log('All nodes are ready.');

    console.log('Warming up the consumer...');
    await new Promise(resolve => {
        requestorNode.emit('ping', { warmUp: true }, () => {
            resolve();
        });
    });
    console.log('Consumer is warm. Starting benchmark...\n');

    const batchStartTime = process.hrtime.bigint();

    console.log('Sending request batch all at once...');
    const promises = [];
    for (let i = 0; i < BATCH_SIZE; i++) {
        const promise = new Promise((resolve) => {
            requestorNode.emit('ping', { count: i }, (err, result, totalDuration) => {
                if (err) {
                    failedReplies++;
                } else {
                    successfulReplies++;
                    latencies.push(parseFloat(totalDuration));
                }
                resolve();
            });
        });
        promises.push(promise);
    }

    await Promise.all(promises);

    const batchEndTime = process.hrtime.bigint();
    const totalBatchDurationMs = Number(batchEndTime - batchStartTime) / 1_000_000;
    const operationsPerSecond = (successfulReplies / totalBatchDurationMs * 1000).toFixed(2);

    console.log('\n--- Benchmark Complete ---');
    const sum = latencies.reduce((a, b) => a + b, 0);
    const avg = (sum / latencies.length || 0).toFixed(2);
    const min = (latencies.length > 0 ? Math.min(...latencies) : 0).toFixed(2);
    const max = (latencies.length > 0 ? Math.max(...latencies) : 0).toFixed(2);

    console.log(`\n--- Overall Performance ---`);
    console.log(`Total Test Duration: ${totalBatchDurationMs.toFixed(2)}ms`);
    console.log(`Throughput:          ${operationsPerSecond} ops/sec`);
    
    console.log(`\n--- Latency Statistics (per request) ---`);
    console.log(`Successful Replies: ${successfulReplies}/${BATCH_SIZE}`);
    if (failedReplies > 0) {
        console.log(`Failed Replies:     ${failedReplies}/${BATCH_SIZE}`);
    }
    console.log(`Average Latency:    ${avg}ms`);
    console.log(`Min Latency:        ${min}ms`);
    console.log(`Max Latency:        ${max}ms`);
    console.log('--------------------------\n');

    process.exit(0);
}

runTest();