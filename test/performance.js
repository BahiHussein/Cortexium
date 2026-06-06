// Aggressive performance suite for Cortexium over NATS.
// Requires a local nats-server -js on 127.0.0.1:4222.
//
// Measures: RPC throughput, latency distribution, concurrency scaling,
// sustained load, payload-size scaling, and broadcast fan-out throughput.
const Cortexium = require('../index');
const { connect } = require('nats');

const URL = 'nats://127.0.0.1:4222';
const PREFIX = 'perf-' + process.pid;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Destroy every KV bucket this run created (all live under PREFIX).
async function cleanupBuckets() {
    try {
        const nc = await connect({ servers: URL });
        const jsm = await nc.jetstreamManager();
        const toDelete = [];
        for await (const s of jsm.streams.list()) {
            const name = s.config.name;
            if (name === `KV_${PREFIX}` || name.startsWith(`KV_${PREFIX}_`)) toDelete.push(name);
        }
        for (const name of toDelete) { try { await jsm.streams.delete(name); } catch { /* ignore */ } }
        await nc.close();
        console.log(`(teardown: destroyed ${toDelete.length} test KV buckets)`);
    } catch (e) {
        console.log('(teardown skipped:', e.message + ')');
    }
}
const ms = (bigintNs) => Number(bigintNs) / 1e6;

function pct(sortedAsc, p) {
    if (sortedAsc.length === 0) return 0;
    const idx = Math.min(sortedAsc.length - 1, Math.floor((p / 100) * sortedAsc.length));
    return sortedAsc[idx];
}

function stats(latencies) {
    const s = latencies.slice().sort((a, b) => a - b);
    const sum = s.reduce((a, b) => a + b, 0);
    return {
        n: s.length,
        mean: sum / s.length,
        p50: pct(s, 50),
        p95: pct(s, 95),
        p99: pct(s, 99),
        max: s[s.length - 1],
        min: s[0],
    };
}

// Bounded-concurrency request pool. Returns per-op latencies (ms).
async function pool(total, concurrency, fn) {
    let next = 0;
    const lat = new Array(total);
    let errors = 0;
    async function worker() {
        for (;;) {
            const i = next++;
            if (i >= total) break;
            const t0 = process.hrtime.bigint();
            try {
                await fn(i);
                lat[i] = ms(process.hrtime.bigint() - t0);
            } catch (e) {
                errors++;
                lat[i] = ms(process.hrtime.bigint() - t0);
            }
        }
    }
    await Promise.all(Array.from({ length: concurrency }, worker));
    return { lat, errors };
}

function fmt(n) { return n.toLocaleString('en-US', { maximumFractionDigits: 2 }); }

async function main() {
    console.log('========================================================');
    console.log(' CORTEXIUM PERFORMANCE SUITE');
    console.log('========================================================\n');

    const responder = new Cortexium({ prefix: PREFIX, url: URL, type: 'responder' });
    const requestor = new Cortexium({ prefix: PREFIX, url: URL, type: 'requestor' });
    await Promise.all([responder.ready(), requestor.ready()]);

    await responder.sub('ping', () => 'pong');
    await responder.sub('echo', (payload) => payload);

    // Warm up (JIT + NATS subscription interest propagation).
    for (let i = 0; i < 200; i++) await requestor.request('ping', { i });
    console.log('Warm-up complete.\n');

    // ---- 1. Throughput: large batch, high concurrency -------------------
    {
        const TOTAL = 50000, CONC = 200;
        console.log(`--- 1. RPC Throughput  (${fmt(TOTAL)} requests, concurrency ${CONC}) ---`);
        const t0 = process.hrtime.bigint();
        const { lat, errors } = await pool(TOTAL, CONC, () => requestor.request('ping', { x: 1 }));
        const wallMs = ms(process.hrtime.bigint() - t0);
        const st = stats(lat);
        console.log(`  wall:        ${fmt(wallMs)} ms`);
        console.log(`  throughput:  ${fmt((TOTAL / wallMs) * 1000)} ops/sec`);
        console.log(`  latency ms:  mean ${st.mean.toFixed(3)} | p50 ${st.p50.toFixed(3)} | p95 ${st.p95.toFixed(3)} | p99 ${st.p99.toFixed(3)} | max ${st.max.toFixed(3)}`);
        console.log(`  errors:      ${errors}\n`);
    }

    // ---- 2. Baseline latency: serial (concurrency 1) -------------------
    {
        const TOTAL = 5000;
        console.log(`--- 2. Serial round-trip latency  (${fmt(TOTAL)} requests, concurrency 1) ---`);
        const { lat } = await pool(TOTAL, 1, () => requestor.request('ping', {}));
        const st = stats(lat);
        console.log(`  latency ms:  mean ${st.mean.toFixed(3)} | p50 ${st.p50.toFixed(3)} | p95 ${st.p95.toFixed(3)} | p99 ${st.p99.toFixed(3)} | max ${st.max.toFixed(3)}\n`);
    }

    // ---- 3. Concurrency scaling ---------------------------------------
    {
        console.log('--- 3. Concurrency scaling  (10,000 requests per level) ---');
        for (const CONC of [1, 10, 50, 100, 250, 500]) {
            const TOTAL = 10000;
            const t0 = process.hrtime.bigint();
            const { lat, errors } = await pool(TOTAL, CONC, () => requestor.request('ping', {}));
            const wallMs = ms(process.hrtime.bigint() - t0);
            const st = stats(lat);
            console.log(`  conc=${String(CONC).padStart(3)}  ${fmt((TOTAL / wallMs) * 1000).padStart(12)} ops/sec   p50 ${st.p50.toFixed(2).padStart(6)}  p99 ${st.p99.toFixed(2).padStart(7)}  errors ${errors}`);
        }
        console.log('');
    }

    // ---- 4. Sustained load --------------------------------------------
    {
        const DURATION = 10000, CONC = 200;
        console.log(`--- 4. Sustained load  (${DURATION / 1000}s, concurrency ${CONC}) ---`);
        let count = 0, errors = 0, running = true;
        const lat = [];
        const t0 = process.hrtime.bigint();
        const workers = Array.from({ length: CONC }, async () => {
            while (running) {
                const w0 = process.hrtime.bigint();
                try { await requestor.request('ping', {}); count++; lat.push(ms(process.hrtime.bigint() - w0)); }
                catch { errors++; }
            }
        });
        await sleep(DURATION);
        running = false;
        await Promise.all(workers);
        const wallMs = ms(process.hrtime.bigint() - t0);
        const st = stats(lat);
        console.log(`  completed:   ${fmt(count)} ops in ${fmt(wallMs)} ms`);
        console.log(`  throughput:  ${fmt((count / wallMs) * 1000)} ops/sec (steady-state)`);
        console.log(`  latency ms:  mean ${st.mean.toFixed(3)} | p50 ${st.p50.toFixed(3)} | p99 ${st.p99.toFixed(3)} | max ${st.max.toFixed(3)}`);
        console.log(`  errors:      ${errors}\n`);
    }

    // ---- 5. Payload-size scaling --------------------------------------
    {
        console.log('--- 5. Payload-size scaling  (5,000 echo requests per size, concurrency 100) ---');
        for (const size of [100, 1024, 10 * 1024, 100 * 1024]) {
            const payload = { blob: 'x'.repeat(size) };
            const TOTAL = 5000;
            const t0 = process.hrtime.bigint();
            const { lat, errors } = await pool(TOTAL, 100, () => requestor.request('echo', payload));
            const wallMs = ms(process.hrtime.bigint() - t0);
            const st = stats(lat);
            const label = size >= 1024 ? `${size / 1024}KB` : `${size}B`;
            console.log(`  ${label.padStart(6)}  ${fmt((TOTAL / wallMs) * 1000).padStart(12)} ops/sec   p50 ${st.p50.toFixed(2).padStart(6)}  p99 ${st.p99.toFixed(2).padStart(7)}  errors ${errors}`);
        }
        console.log('');
    }

    // ---- 6. Broadcast fan-out throughput ------------------------------
    {
        const N = 50000;
        console.log(`--- 6. Broadcast throughput  (${fmt(N)} events, 3 subscribers) ---`);
        const subs = [];
        const counts = [0, 0, 0];
        for (let i = 0; i < 3; i++) {
            const s = new Cortexium({ prefix: PREFIX, url: URL, type: `bsub-${i}` });
            await s.ready();
            const idx = i;
            await s.subscribe('events.stream', () => { counts[idx]++; });
            subs.push(s);
        }
        const pub = new Cortexium({ prefix: PREFIX, url: URL, type: 'bpub' });
        await pub.ready();
        await sleep(300); // let interest propagate

        const t0 = process.hrtime.bigint();
        for (let i = 0; i < N; i++) await pub.publish('events.stream', { i });
        const pubMs = ms(process.hrtime.bigint() - t0);
        // Wait for delivery to drain.
        const deadline = Date.now() + 10000;
        while (counts.some((c) => c < N) && Date.now() < deadline) await sleep(50);
        console.log(`  publish rate: ${fmt((N / pubMs) * 1000)} events/sec (${fmt(pubMs)} ms)`);
        console.log(`  delivered:    [${counts.join(', ')}] of ${fmt(N)} each (3x fan-out = ${fmt(N * 3)} total deliveries)`);
        console.log('');

        await Promise.all([...subs.map((s) => s.shutdown()), pub.shutdown()]);
    }

    await Promise.all([responder.shutdown(), requestor.shutdown()]);
    await cleanupBuckets();
    console.log('========================================================');
    console.log(' PERFORMANCE SUITE COMPLETE');
    console.log('========================================================');
    process.exit(0);
}

main().catch(async (e) => { console.error('PERF ERROR:', e); await cleanupBuckets(); process.exit(1); });
