// redis-latency-test.js
const Redis = require('ioredis');

const TEST_CONFIG = {
    url: 'redis://127.0.0.1:6379',
    pings: 100, // How many PING commands to send
};

const latencies = [];

async function runRedisLatencyTest() {
    console.log(`--- Starting Redis PING Latency Test ---`);
    console.log(`Connecting to Redis at ${TEST_CONFIG.url}...\n`);

    const redis = new Redis(TEST_CONFIG.url);

    // Wait for the client to be fully connected and ready.
    await new Promise(resolve => {
        redis.on('ready', resolve);
    });
    
    console.log('Redis client is ready. Starting benchmark...');
    
    // Run the test sequentially to measure individual command latency without queueing effects.
    for (let i = 0; i < TEST_CONFIG.pings; i++) {
        const startTime = process.hrtime.bigint();
        
        // PING is the standard command for measuring latency.
        // It should return 'PONG'.
        await redis.ping(); 
        
        const endTime = process.hrtime.bigint();
        const durationMs = Number(endTime - startTime) / 1_000_000;
        latencies.push(durationMs);
    }

    console.log('\n--- Benchmark Complete ---');
    const sum = latencies.reduce((a, b) => a + b, 0);
    const avg = (sum / latencies.length || 0).toFixed(2);
    const min = (latencies.length > 0 ? Math.min(...latencies) : 0).toFixed(2);
    const max = (latencies.length > 0 ? Math.max(...latencies) : 0).toFixed(2);

    console.log(`Commands Sent: ${TEST_CONFIG.pings}`);
    console.log(`Average Latency: ${avg}ms`);
    console.log(`Min Latency:     ${min}ms`);
    console.log(`Max Latency:     ${max}ms`);
    console.log('--------------------------\n');

    if (avg > 10) {
        console.log('⚠️  Warning: Average latency is high. This is likely due to the overhead of the Docker network bridge.');
    } else {
        console.log('✅  Result: Redis latency appears to be low and healthy.');
    }

    redis.quit();
}

runRedisLatencyTest();