// Aggressive reliability / correctness suite for Cortexium.
// Requires a local nats-server -js on 127.0.0.1:4222.
//
// Covers: RPC correctness under load, error propagation, timeouts,
// load-balancing fairness, broadcast fan-out, middleware ordering/short-circuit,
// and the multi-node scheduler (leader election, exactly-once, crash failover,
// and chaos under continuous scheduling load).
const Cortexium = require('../index');
const { connect } = require('nats');

const URL = 'nats://127.0.0.1:4222';
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Every prefix this run creates, so teardown can destroy the KV buckets it made.
const usedPrefixes = [];

async function cleanupBuckets() {
    try {
        const nc = await connect({ servers: URL });
        const jsm = await nc.jetstreamManager();
        const toDelete = [];
        for await (const s of jsm.streams.list()) {
            const name = s.config.name;
            if (usedPrefixes.some((p) => name === `KV_${p}` || name.startsWith(`KV_${p}_`))) {
                toDelete.push(name);
            }
        }
        for (const name of toDelete) { try { await jsm.streams.delete(name); } catch { /* ignore */ } }
        await nc.close();
        console.log(`\n(teardown: destroyed ${toDelete.length} test KV buckets)`);
    } catch (e) {
        console.log('\n(teardown skipped:', e.message + ')');
    }
}

let passCount = 0, failCount = 0;
function check(name, cond, detail = '') {
    if (cond) { passCount++; console.log(`  ✅ ${name}`); }
    else { failCount++; console.log(`  ❌ ${name}${detail ? ' — ' + detail : ''}`); }
}

let _seq = 0;
function uid(p) { const id = `${p}-${process.pid}-${_seq++}`; usedPrefixes.push(id); return id; }

// ---- 1. RPC correctness under high concurrency -------------------------
async function testRpcCorrectness() {
    console.log('\n--- 1. RPC correctness under concurrency (10,000 requests) ---');
    const prefix = uid('rel-rpc');
    const worker = new Cortexium({ prefix, url: URL, type: 'doubler' });
    const client = new Cortexium({ prefix, url: URL, type: 'client' });
    await Promise.all([worker.ready(), client.ready()]);
    await worker.sub('double', (n) => n * 2);

    const N = 10000;
    let wrong = 0, errs = 0;
    await Promise.all(Array.from({ length: N }, (_, i) =>
        client.request('double', i).then((r) => { if (r !== i * 2) wrong++; }).catch(() => errs++)
    ));
    check(`all ${N} replies correct`, wrong === 0 && errs === 0, `wrong=${wrong} errs=${errs}`);
    await Promise.all([worker.shutdown(), client.shutdown()]);
}

// ---- 2. Error propagation ---------------------------------------------
async function testErrorPropagation() {
    console.log('\n--- 2. Structured error propagation ---');
    const prefix = uid('rel-err');
    const worker = new Cortexium({ prefix, url: URL, type: 'risky' });
    const client = new Cortexium({ prefix, url: URL, type: 'client' });
    await Promise.all([worker.ready(), client.ready()]);
    await worker.sub('boom', () => { throw Object.assign(new Error('Insufficient funds'), { code: 'INSUFFICIENT_FUNDS' }); });

    let caught = null;
    try { await client.request('boom', {}); } catch (e) { caught = e; }
    check('error surfaced to caller', !!caught);
    check('error code preserved', caught?.code === 'INSUFFICIENT_FUNDS', `got ${caught?.code}`);
    check('error message preserved', caught?.message === 'Insufficient funds', `got ${caught?.message}`);
    await Promise.all([worker.shutdown(), client.shutdown()]);
}

// ---- 3. No-responders fast-fail + real timeout ------------------------
async function testTimeout() {
    console.log('\n--- 3. No-responders fast-fail and request timeout ---');
    const prefix = uid('rel-to');
    const worker = new Cortexium({ prefix, url: URL, type: 'slow' });
    const client = new Cortexium({ prefix, url: URL, type: 'client' });
    await Promise.all([worker.ready(), client.ready()]);

    // (a) No subscriber for the subject -> NATS fast-fails (no waiting).
    let nr = null;
    const t0 = Date.now();
    try { await client.request('nowhere', {}, { timeout: 2000 }); } catch (e) { nr = e; }
    const nrElapsed = Date.now() - t0;
    check('no-responders error raised', !!nr);
    check('mapped to NO_RESPONDERS code', nr?.code === 'NO_RESPONDERS', `got ${nr?.code}`);
    check('fast-failed (well under timeout)', nrElapsed < 500, `elapsed=${nrElapsed}ms`);

    // (b) Subscriber exists but is slower than the timeout -> TIMEOUT.
    await worker.sub('slowjob', async () => { await sleep(2000); return 'late'; });
    await sleep(150);
    let to = null;
    const t1 = Date.now();
    try { await client.request('slowjob', {}, { timeout: 500 }); } catch (e) { to = e; }
    const toElapsed = Date.now() - t1;
    check('timeout error raised', !!to);
    check('timeout code', to?.code === 'TIMEOUT', `got ${to?.code}`);
    check('timed out near 500ms', toElapsed >= 400 && toElapsed < 1500, `elapsed=${toElapsed}ms`);
    await Promise.all([worker.shutdown(), client.shutdown()]);
}

// ---- 4. Load-balancing fairness across queue group --------------------
async function testLoadBalancing() {
    console.log('\n--- 4. Load balancing across 4 workers (8,000 requests) ---');
    const prefix = uid('rel-lb');
    const workers = [];
    const hits = {};
    for (let i = 0; i < 4; i++) {
        const w = new Cortexium({ prefix, url: URL, type: 'worker' }); // same type => one queue group
        await w.ready();
        const id = w.nodeId; hits[id] = 0;
        await w.sub('task', () => { hits[id]++; return 'ok'; });
        workers.push(w);
    }
    const client = new Cortexium({ prefix, url: URL, type: 'client' });
    await client.ready();
    await sleep(200);

    const N = 8000;
    let ok = 0;
    await Promise.all(Array.from({ length: N }, () => client.request('task', {}).then(() => ok++).catch(() => {})));
    const dist = Object.values(hits);
    const total = dist.reduce((a, b) => a + b, 0);
    const min = Math.min(...dist), max = Math.max(...dist);
    check(`all ${N} requests served`, ok === N && total === N, `ok=${ok} total=${total}`);
    check('every worker got a share', min > 0, `dist=[${dist.join(', ')}]`);
    check('distribution roughly fair (max/min < 3)', max / min < 3, `dist=[${dist.join(', ')}]`);
    await Promise.all([...workers.map((w) => w.shutdown()), client.shutdown()]);
}

// ---- 5. Broadcast fan-out ---------------------------------------------
async function testBroadcast() {
    console.log('\n--- 5. Broadcast fan-out to all subscribers ---');
    const prefix = uid('rel-bc');
    const subs = []; const counts = [];
    for (let i = 0; i < 5; i++) {
        const s = new Cortexium({ prefix, url: URL, type: `sub-${i}` }); // distinct types => all receive
        await s.ready();
        const idx = i; counts[idx] = 0;
        await s.subscribe('news', () => { counts[idx]++; });
        subs.push(s);
    }
    const pub = new Cortexium({ prefix, url: URL, type: 'pub' });
    await pub.ready();
    await sleep(300);

    const N = 1000;
    for (let i = 0; i < N; i++) await pub.publish('news', { i });
    const deadline = Date.now() + 5000;
    while (counts.some((c) => c < N) && Date.now() < deadline) await sleep(50);
    check('every subscriber received all events', counts.every((c) => c === N), `counts=[${counts.join(', ')}] expected ${N}`);
    await Promise.all([...subs.map((s) => s.shutdown()), pub.shutdown()]);
}

// ---- 6. Middleware ordering + short-circuit ---------------------------
async function testMiddleware() {
    console.log('\n--- 6. Middleware ordering and short-circuit ---');
    const prefix = uid('rel-mw');
    const worker = new Cortexium({ prefix, url: URL, type: 'svc' });
    const client = new Cortexium({ prefix, url: URL, type: 'client' });
    await Promise.all([worker.ready(), client.ready()]);

    const order = [];
    worker.use('secure.*', async (ctx, next) => { order.push('mw1-before'); await next(); order.push('mw1-after'); });
    worker.use('secure.*', async (ctx, next) => {
        order.push('mw2');
        if (!ctx.message.payload?.token) throw Object.assign(new Error('Unauthorized'), { code: 'UNAUTHORIZED' });
        await next();
    });
    await worker.sub('secure.data', () => { order.push('handler'); return 'secret'; });

    const ok = await client.request('secure.data', { token: 'abc' });
    check('authorized request reaches handler', ok === 'secret');
    check('middleware ran in order (onion)', order.join(',') === 'mw1-before,mw2,handler,mw1-after', order.join(','));

    let blocked = null;
    try { await client.request('secure.data', {}); } catch (e) { blocked = e; }
    check('unauthorized request short-circuited', blocked?.code === 'UNAUTHORIZED', `got ${blocked?.code}`);
    await Promise.all([worker.shutdown(), client.shutdown()]);
}

// ---- 7. Streaming RPC (additive) --------------------------------------
async function testStreaming() {
    console.log('\n--- 7. Streaming RPC (additive feature) ---');
    const prefix = uid('rel-stream');
    const worker = new Cortexium({ prefix, url: URL, type: 'streamer' });
    const client = new Cortexium({ prefix, url: URL, type: 'client' });
    await Promise.all([worker.ready(), client.ready()]);

    await worker.sub('count', (n, ctx) => {
        const s = ctx.stream();
        for (let i = 0; i < 5; i++) s.send({ i });
        s.end();
    });
    await worker.sub('slowcount', async (n, ctx) => {
        const s = ctx.stream();
        for (let i = 0; i < 4; i++) { await sleep(30); s.send({ i }); } // emit over time
        s.end();
    });
    await worker.sub('single', () => ({ value: 'one' }));
    await worker.sub('boom', (p, ctx) => {
        const s = ctx.stream();
        s.send({ i: 0 });
        s.send({ i: 1 });
        throw Object.assign(new Error('stream broke'), { code: 'STREAM_FAIL' });
    });
    await worker.sub('stall', async (p, ctx) => {
        ctx.stream().send({ i: 0 });
        await new Promise(() => {}); // never ends -> client must idle-timeout
    });
    await sleep(150);

    // 7a: all chunks, in order
    const got = [];
    for await (const c of client.requestStream('count', {})) got.push(c.i);
    check('streamed all 5 chunks in order', got.join(',') === '0,1,2,3,4', `got=[${got.join(',')}]`);

    // 7a-async: chunks emitted over time (handler stays pending) all arrive
    const slow = [];
    for await (const c of client.requestStream('slowcount', {})) slow.push(c.i);
    check('async stream (chunks over time) delivers all', slow.join(',') === '0,1,2,3', `got=[${slow.join(',')}]`);

    // 7b: one-shot request() sees only the first chunk (this is WHY requestStream exists)
    const single = await client.request('count', {});
    check('request() sees only first chunk (existing behavior unchanged)', single.i === 0, `got=${JSON.stringify(single)}`);

    // 7c: requestStream against a normal handler yields exactly one value (compat)
    const compat = [];
    for await (const c of client.requestStream('single', {})) compat.push(c);
    check('requestStream on non-streaming handler yields exactly 1', compat.length === 1 && compat[0].value === 'one', `got=${JSON.stringify(compat)}`);

    // 7d: mid-stream error propagates, with chunks-before-error delivered
    const partial = []; let streamErr = null;
    try { for await (const c of client.requestStream('boom', {})) partial.push(c.i); }
    catch (e) { streamErr = e; }
    check('chunks before error were delivered', partial.join(',') === '0,1', `got=[${partial.join(',')}]`);
    check('mid-stream error propagated with code', streamErr?.code === 'STREAM_FAIL', `got ${streamErr?.code}`);

    // 7e: idle timeout when the handler stalls
    let toErr = null;
    try { for await (const _ of client.requestStream('stall', {}, { idleTimeout: 300 })) { /* consume */ } }
    catch (e) { toErr = e; }
    check('idle timeout fires when stream stalls', toErr?.code === 'TIMEOUT', `got ${toErr?.code}`);

    await Promise.all([worker.shutdown(), client.shutdown()]);
}

// ---- 8. Scheduler: single-node crash recovery -------------------------
async function testSchedulerRecovery() {
    console.log('\n--- 8. Scheduler single-node crash recovery ---');
    const prefix = uid('rel-sched1');
    const sched = new Cortexium({ prefix, url: URL, type: 'scheduler-service', services: [Cortexium.services.Scheduler] });
    const recv = new Cortexium({ prefix, url: URL, type: 'recv' });
    const client = new Cortexium({ prefix, url: URL, type: 'client' });
    await Promise.all([sched.ready(), recv.ready(), client.ready()]);
    const fired = [];
    await recv.sub('job', (p) => fired.push(p.id));

    await client.emit('scheduler:add', { id: 'R1', topic: 'job', payload: { id: 'R1' }, delay: 3000 });
    await sleep(400);
    await sched.transport.disconnect(); // hard kill before R1 fires
    sched.loadedServices[0].isShutdown = true;
    if (sched.loadedServices[0].electionTimer) clearInterval(sched.loadedServices[0].electionTimer);

    await sleep(800);
    const sched2 = new Cortexium({ prefix, url: URL, type: 'scheduler-service', services: [Cortexium.services.Scheduler] });
    await sched2.ready();
    // Hard-killed node never released its lease, so takeover waits out the lease TTL (~6s).
    // Once sched2 becomes leader it replays R1 from KV (already overdue) and fires it.
    await sleep(9000);
    check('task fired exactly once after recovery', fired.filter((x) => x === 'R1').length === 1, `fired=${fired.length}`);
    await Promise.all([sched2.shutdown(), recv.shutdown(), client.shutdown()]);
}

// ---- 8. Scheduler: multi-node exactly-once + chaos under load ----------
async function testSchedulerChaos() {
    console.log('\n--- 9. Scheduler multi-node chaos (kill leader under continuous load) ---');
    const prefix = uid('rel-sched2');
    const mk = () => new Cortexium({ prefix, url: URL, type: 'scheduler-service', services: [Cortexium.services.Scheduler] });
    const nodes = [mk(), mk(), mk()];
    for (const n of nodes) await n.ready();
    const recv = new Cortexium({ prefix, url: URL, type: 'recv' });
    const client = new Cortexium({ prefix, url: URL, type: 'client' });
    await Promise.all([recv.ready(), client.ready()]);

    const fired = {};
    await recv.sub('job', (p) => { fired[p.id] = (fired[p.id] || 0) + 1; });

    const leaderOf = () => nodes.filter((n) => n.loadedServices[0].isLeader && !n.loadedServices[0].isShutdown);
    await sleep(1500);
    check('exactly one leader elected', leaderOf().length === 1, `leaders=${leaderOf().length}`);

    // Continuously schedule tasks with short delays while we kill the leader.
    const TOTAL = 60;
    const ids = [];
    let scheduled = 0;
    const feeder = (async () => {
        for (let i = 0; i < TOTAL; i++) {
            const id = `C${i}`;
            ids.push(id);
            await client.emit('scheduler:add', { id, topic: 'job', payload: { id }, delay: 2000 });
            scheduled++;
            await sleep(50);
        }
    })();

    // Mid-stream, hard-kill the current leader.
    await sleep(700);
    const victim = leaderOf()[0];
    victim.transport.disconnect().catch(() => {});
    victim.loadedServices[0].isShutdown = true;
    if (victim.loadedServices[0].electionTimer) clearInterval(victim.loadedServices[0].electionTimer);
    console.log(`  (killed leader mid-stream after scheduling ~${scheduled} tasks)`);

    await feeder;
    // Wait out failover (lease TTL) + task delay + margin.
    await sleep(12000);

    const survivors = nodes.filter((n) => n !== victim);
    check('a survivor took over as leader', survivors.some((n) => n.loadedServices[0].isLeader));
    const firedIds = ids.filter((id) => fired[id]);
    const duplicates = ids.filter((id) => (fired[id] || 0) > 1);
    const missing = ids.filter((id) => !fired[id]);
    check(`all ${TOTAL} tasks fired (no loss)`, firedIds.length === TOTAL, `fired ${firedIds.length}/${TOTAL}, missing=[${missing.slice(0, 10).join(',')}]`);
    check('no task fired more than once', duplicates.length === 0, `dupes=[${duplicates.slice(0, 10).join(',')}]`);

    await Promise.all([...survivors.map((n) => n.shutdown()), recv.shutdown(), client.shutdown()]);
}

// ---- 10. Secondary API surface ----------------------------------------
async function testSecondaryApi() {
    console.log('\n--- 10. Secondary API surface ---');

    // 10a: constructor validation (no server needed)
    let e1 = null, e2 = null;
    try { new Cortexium({ url: URL }); } catch (e) { e1 = e; }
    try { new Cortexium({ type: 'x' }); } catch (e) { e2 = e; }
    check('constructor requires type', /type/i.test(e1?.message || ''), e1?.message);
    check('constructor requires url', /url/i.test(e2?.message || ''), e2?.message);

    const prefix = uid('rel-api');
    const worker = new Cortexium({ prefix, url: URL, type: 'api-worker' });
    const client = new Cortexium({ prefix, url: URL, type: 'api-client' });
    await Promise.all([worker.ready(), client.ready()]);

    // 10b: emit() with callback (RPC reply via callback)
    await worker.sub('echo', (p) => ({ got: p.n }));
    const cb = await new Promise((resolve) => {
        client.emit('echo', { n: 7 }, (err, result, duration) => resolve({ err, result, duration }));
    });
    check('emit() callback returns result', cb.result?.got === 7, JSON.stringify(cb.result));
    check('emit() callback has no error', cb.err === null, String(cb.err));
    check('emit() callback reports duration', cb.duration !== undefined && !isNaN(Number(cb.duration)), String(cb.duration));

    // 10c: emit() fire-and-forget (no callback)
    let ffGot = null;
    await worker.sub('ff', (p) => { ffGot = p.v; });
    await client.emit('ff', { v: 'fire' });
    await sleep(250);
    check('emit() fire-and-forget delivered', ffGot === 'fire', String(ffGot));

    // 10d: ctx.reply() manual reply
    await worker.sub('manual', (p, ctx) => { ctx.reply({ manual: true }); });
    const manual = await client.request('manual', {});
    check('ctx.reply() delivers a manual reply', manual?.manual === true, JSON.stringify(manual));

    // 10e: ctx.publish() from inside a handler
    let sideEffect = null;
    await worker.subscribe('side', (p) => { sideEffect = p.from; });
    await worker.sub('trigger', (p, ctx) => { ctx.publish('side', { from: 'handler' }); return 'ok'; });
    const trig = await client.request('trigger', {});
    await sleep(300);
    check('handler still returns its RPC result', trig === 'ok', String(trig));
    check('ctx.publish() emits a broadcast from a handler', sideEffect === 'handler', String(sideEffect));

    // 10f: unsub() stops delivery
    await worker.sub('temp', () => 'alive');
    const before = await client.request('temp', {});
    await worker.unsub('temp');
    let afterErr = null;
    try { await client.request('temp', {}, { timeout: 800 }); } catch (e) { afterErr = e; }
    check('request works before unsub', before === 'alive', String(before));
    check('unsub() stops the subscription', afterErr?.code === 'NO_RESPONDERS' || afterErr?.code === 'TIMEOUT', `got ${afterErr?.code}`);

    // 10g: shutdown() idempotency
    await worker.shutdown();
    let dblErr = null;
    try { await worker.shutdown(); } catch (e) { dblErr = e; }
    check('shutdown() is idempotent', dblErr === null, String(dblErr));

    // 10h: discover() finds active nodes of a type
    const d1 = new Cortexium({ prefix, url: URL, type: 'discoverable' });
    const d2 = new Cortexium({ prefix, url: URL, type: 'discoverable' });
    await Promise.all([d1.ready(), d2.ready()]);
    await sleep(900); // let heartbeats land in the discovery KV bucket
    const found = await client.discover('discoverable');
    check('discover() returns the active nodes', found.length === 2 && found.every((n) => n.nodeId), `found=${found.length}`);

    await Promise.all([d1.shutdown(), d2.shutdown(), client.shutdown()]);
}

async function main() {
    console.log('========================================================');
    console.log(' CORTEXIUM RELIABILITY SUITE');
    console.log('========================================================');
    await testRpcCorrectness();
    await testErrorPropagation();
    await testTimeout();
    await testLoadBalancing();
    await testBroadcast();
    await testMiddleware();
    await testStreaming();
    await testSchedulerRecovery();
    await testSchedulerChaos();
    await testSecondaryApi();

    await cleanupBuckets();

    console.log('\n========================================================');
    console.log(` RESULT: ${passCount} passed, ${failCount} failed`);
    console.log('========================================================');
    process.exit(failCount === 0 ? 0 : 1);
}

main().catch(async (e) => { console.error('RELIABILITY ERROR:', e); await cleanupBuckets(); process.exit(1); });
