# Cortexium

**A high-performance, reliable Node.js library for inter-service communication using NATS.**

Cortexium provides a simple and powerful API for building complex, distributed systems and microservices. It enables different services (called "nodes") to perform remote procedure calls (RPC) and publish events in a fast, scalable, and fault-tolerant way.

- **High Throughput:** Built on NATS for sub-millisecond RPC and event broadcasting.
- **True Pub/Sub + RPC:** Distinguishes between load-balanced RPC and broadcast events.
- **Native Request/Reply:** No hand-rolled correlation IDs or reply channels — NATS handles it.
- **Middleware Pipeline:** Express-style middleware for auth, logging, metrics, tracing.
- **Service Discovery:** Built-in node registry with heartbeats.
- **Streaming RPC:** Support for multi-chunk responses.
- **Error Propagation:** Structured errors sent back to callers, not silent timeouts.
- **Extensible:** Load service modules (scheduler, etc.) into a Cortexium node.

---

## Table of Contents

- [How Cortexium Works](#how-cortexium-works)
- [Getting Started](#getting-started)
- [Usage Patterns](#usage-patterns)
  - [RPC (Load-Balanced)](#1-rpc-load-balanced)
  - [Broadcast Events](#2-broadcast-events)
  - [Promise-Based RPC](#3-promise-based-rpc)
  - [Middleware](#4-middleware)
- [Extending with Services](#extending-with-services)
- [Performance & Diagnostics](#performance--diagnostics)
- [API Reference](#api-reference)
- [Architecture](#architecture)
- [Debugging](#debugging)

---

## How Cortexium Works

At its core, Cortexium is a communication framework for microservices. Your services (nodes) connect to a NATS server and communicate by publishing messages to topics and subscribing to topics to process them.

### The Core Components

#### 1. Nodes
A **node** is an instance of your service. You give it a `type`, like `'order-service'`. Multiple nodes can share the same type, and RPC work is automatically load-balanced across them.

#### 2. Topics
A **topic** is a category of work. Services send messages to topics (e.g., `payments.process`). NATS supports wildcards: `payments.*` matches `payments.process`, `payments.>` matches all nested topics.

#### 3. Two Message Patterns

**RPC (`sub` + `emit`/`request`)**: Load-balanced to **exactly one** worker. Ideal for operations that need a reply.

**Events (`subscribe` + `publish`)**: Broadcast to **all** active subscribers. Ideal for notifications, analytics, cache invalidation.

---

## Getting Started

### Prerequisites

You need a NATS server running. The easiest way:

```bash
# macOS
brew install nats-server
nats-server -js

# Or Docker
docker run -p 4222:4222 nats:latest -js
```

### Installation

```bash
npm install cortexium
```

### Running the RPC Example

**Step 1: Start the Calculator Service**

```bash
node example/rpc/calculator-service.js
# Output: [Calculator] Node i49... is online and ready.
```

**Step 2: Start the API Gateway**

```bash
node example/rpc/api-gateway.js
# Output: [API Gateway] SUCCESS! Result: 42 (took 1.98ms)
```

---

## Usage Patterns

### 1. RPC (Load-Balanced)

This is the most common pattern. A service sends a request to a topic, and **any one** of the available workers picks it up. This is ideal for scaling tasks horizontally.

**Service (worker):**
```javascript
const Cortexium = require('cortexium');

const node = new Cortexium({
    prefix: 'my-app',
    url: 'nats://127.0.0.1:4222',
    type: 'calculator-service',
});

await node.ready();

// Load-balanced: only ONE calculator-service receives each request
await node.sub('add', (numbers) => {
    return numbers[0] + numbers[1];
});
```

**Client (callback API):**
```javascript
node.emit('add', [15, 27], (err, result, duration) => {
    console.log(`Result: ${result}, took ${duration}ms`);
});
```

**Client (Promise API — recommended):**
```javascript
const result = await node.request('add', [15, 27]);
```

### 2. Broadcast Events

Send a message to **all** subscribers. No reply is expected.

**Subscriber:**
```javascript
await node.subscribe('user:login', (data) => {
    console.log(`User ${data.userId} logged in`);
    // ALL analytics, audit, and notification services receive this
});
```

**Publisher:**
```javascript
await node.publish('user:login', { userId: 123 });
```

### 3. Promise-Based RPC

The modern alternative to callbacks:

```javascript
try {
    const result = await node.request('payments.process', {
        amount: 100,
        currency: 'USD'
    }, { timeout: 10000 });
    console.log('Payment approved:', result);
} catch (err) {
    console.error('Payment failed:', err.code, err.message);
}
```

### 4. Middleware

Register cross-cutting concerns that run before your handlers:

```javascript
// Logging middleware for all topics
node.use('*', async (ctx, next) => {
    const start = Date.now();
    await next();
    console.log(`Handled ${ctx.message.topic} in ${Date.now() - start}ms`);
});

// Auth middleware for sensitive topics
node.use('payments.*', async (ctx, next) => {
    const apiKey = ctx.message.headers?.['x-api-key'];
    if (!apiKey) throw Object.assign(new Error('Unauthorized'), { code: 'UNAUTHORIZED' });
    await next();
});

await node.sub('payments.process', async (payload, ctx) => {
    // This runs AFTER middleware
    return { status: 'approved', transactionId: 'txn_123' };
});
```

### 5. Error Propagation

When a handler throws, the error is sent back to the caller as a structured response:

```javascript
await node.sub('risky.operation', () => {
    throw Object.assign(
        new Error('Insufficient funds'),
        { code: 'INSUFFICIENT_FUNDS' }
    );
});

try {
    await node.request('risky.operation', {});
} catch (err) {
    console.log(err.code);    // 'INSUFFICIENT_FUNDS'
    console.log(err.message); // 'Insufficient funds'
}
```

### 6. Service Discovery

Discover active nodes of a given type:

```javascript
const calculators = await node.discover('calculator-service');
console.log(calculators);
// [{ nodeId: '...', nodeType: 'calculator-service', timestamp: 1717654800000 }]
```

### 7. Streaming RPC

For responses that arrive in multiple chunks (large result sets, progress updates, log tails), the handler streams via `ctx.stream()` and the caller consumes an async iterable.

**Service (streaming handler):**
```javascript
await node.sub('export.users', async (query, ctx) => {
    const stream = ctx.stream();
    for await (const batch of db.scanUsers(query)) {
        stream.send(batch);   // send each chunk as it's ready
    }
    stream.end();             // terminate the stream
});
```

**Client:**
```javascript
for await (const batch of node.requestStream('export.users', { since: '2024-01-01' })) {
    handle(batch);            // chunks arrive in order
}
// Errors thrown by the handler propagate out of the loop with their .code.
```

---

## Extending with Services

Cortexium is designed to be extensible through services. These are modules that can be loaded into a node.

### Available Services

- [**Scheduler**](./docs/scheduler.md): Schedule tasks to be executed at a later time.

```javascript
const Cortexium = require('cortexium');
const { Scheduler } = Cortexium.services;

const node = new Cortexium({
    prefix: 'my-app',
    url: 'nats://127.0.0.1:4222',
    type: 'scheduler-service',
    services: [Scheduler],
});

await node.ready();
```

---

## Performance & Diagnostics

### Running the Performance Test

```bash
npm run test
```

The test simulates 100 concurrent RPC requests and reports throughput and latency.

### Diagnosing Event Loop Issues

```bash
npm run diagnose
```

Monitors event loop lag while the performance test runs. Healthy systems stay under 20ms.

---

## API Reference

### `new Cortexium(options)`

Creates a new Cortexium node.

- `options` `<Object>`
    - `url` `<string>` **Required.** NATS server URL (e.g., `'nats://127.0.0.1:4222'`).
    - `prefix` `<string>` **Required.** Namespace for all topics (e.g., `'my-app'`).
    - `type` `<string>` **Required.** The type of this service (e.g., `'order-service'`).
    - `services` `<Array>` **Optional.** Array of service classes to load.

### `await node.ready()`

Returns a `Promise` that resolves when the node is connected to NATS and ready.

### `node.use(pattern, middleware)`

Register middleware for topics matching `pattern`.

- `pattern` `<string|RegExp>` Topic pattern. Supports NATS-style wildcards: `payments.*`, `payments.>`.
- `middleware` `<Function>` `async (ctx, next) => void`. Call `await next()` to proceed.

### `await node.sub(topic, handler, options)`

Subscribe to a topic for **RPC** (load-balanced).

- `topic` `<string>` Topic name.
- `handler` `<Function>` `async (payload, ctx) => result`. Return value is sent back as reply.
- `options.queue` `<string>` Override the queue group (default: `nodeType`).

### `await node.subscribe(topic, handler)`

Subscribe to a topic for **broadcast events**.

- `topic` `<string>` Topic name.
- `handler` `<Function>` `async (payload, ctx) => void`.

### `await node.publish(topic, payload)`

Publish a **broadcast event** (fire-and-forget). All subscribers receive it.

### `node.emit(topic, payload, [callback])`

Emit a message.

- **Without callback**: Fire-and-forget RPC (load-balanced, no reply expected).
- **With callback**: RPC with callback reply `(err, result, duration)`.
- **Timeout**: 5 seconds (use `node.request()` for configurable timeout).

### `await node.request(topic, payload, options)`

Promise-based RPC.

- `options.timeout` `<number>` Timeout in ms (default: `5000`).
- Returns: `Promise<result>`
- Throws: `Error` with `.code` property on failure.

### `node.requestStream(topic, payload, options)`

Streaming RPC. Returns an async iterable that yields each chunk the handler emits via `ctx.stream()`, in order, until the stream ends. Additive to `request()` — use it when a handler produces multiple chunks. A handler that returns a single value also works (yields one item).

- `options.idleTimeout` `<number>` Max ms to wait between chunks (default: `30000`).
- Returns: `AsyncIterable<chunk>`
- Throws: `Error` with `.code` — `'NO_RESPONDERS'`, `'TIMEOUT'`, or the handler's error code (mid-stream errors propagate).

```javascript
for await (const chunk of node.requestStream('export.users', { since })) {
    process(chunk);
}
```

> Streaming uses core NATS (at-most-once delivery, same guarantee as `request()`), with no flow control — a very slow consumer buffers chunks in memory. For guaranteed/back-pressured delivery, JetStream is the upgrade path.

### `await node.discover(nodeType)`

Discover active nodes.

- Returns: `Promise<Array<{ nodeId, nodeType, timestamp }>>`

### `await node.shutdown()`

Gracefully disconnect from NATS and shut down services.

---

## Architecture

Cortexium is built on **NATS**, a lightweight, high-performance messaging system.

### Why NATS?

| Feature | Old (Redis Streams) | New (NATS) |
|---------|---------------------|------------|
| Request/Reply | Hand-rolled | Native |
| Load Balancing | Consumer groups (clunky) | Queue groups |
| Broadcast | Not supported | Native Pub/Sub |
| Wildcards | Not supported | `payments.*`, `payments.>` |
| Throughput | ~1-2k ops/sec | ~18k+ ops/sec |
| Latency | ~5-20ms | ~1-2ms |
| Retries/DLQ | Build from scratch | JetStream native |
| Service Discovery | Build from scratch | Built-in |

### Message Flow

**RPC:**
1. Client calls `node.request('add', [1, 2])`
2. NATS sends request to `my-app.add` subject
3. NATS queue group delivers to **one** `calculator-service` worker
4. Handler returns `3`
5. Transport auto-replies via NATS `msg.respond()`
6. Client Promise resolves with `3`

**Events:**
1. Client calls `node.publish('user:login', data)`
2. NATS publishes to `my-app.user:login`
3. **All** subscribers receive the message

### Message Envelope

All messages use a structured envelope:

```javascript
{
    id: "uuid",
    topic: "payments.process",
    type: "rpc",           // "rpc" | "event" | "stream"
    correlationId: "uuid",
    payload: {},
    headers: {},
    timestamp: 1717654800000,
    attempt: 1,
    maxAttempts: 3,
    error: null            // Set on error replies
}
```

---

## Debugging

Cortexium uses the `debug` library for detailed, namespaced logging.

```bash
# See all logs
DEBUG=cortexium:* node your-service.js

# See only specific namespaces
DEBUG=cortexium:emit,cortexium:reply node your-service.js

# Available namespaces:
# cortexium:core      - Node lifecycle
# cortexium:emit      - Outgoing messages
# cortexium:sub       - Incoming messages
# cortexium:reply     - Replies
# cortexium:transport - NATS transport details
# cortexium:discovery - Service discovery
```

---

## License

MIT
