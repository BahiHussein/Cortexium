# Cortexium

**A high-performance, reliable Node.js library for inter-service communication using Redis Streams.**

Cortexium provides a simple and powerful API for building complex, distributed systems and microservices. It allows different services (called "nodes") to perform remote procedure calls (RPC) on each other in a fast, scalable, and fault-tolerant way.

- **High Throughput:** Built on Redis Streams and a non-blocking architecture to handle thousands of concurrent operations per second.
- **Load Balancing:** Automatically distributes work across multiple instances of the same service type.
- **Reliable Request/Reply:** Guarantees that replies to specific requests are delivered to the correct originating node, even in a scaled-out environment.
- **Easy to Use:** A minimal API (`emit` and `sub`) makes it simple to send messages and subscribe to topics.
- **Debuggable:** Includes detailed logging with the `debug` package to provide deep insight into the message flow.
- **Extensible with Services:** Easily load and initialize service modules, such as a task scheduler, within a Cortexium node.

## High-Level Architecture

At its core, Cortexium uses Redis as a central message bus. Your services (nodes) connect to Cortexium and communicate with each other by publishing messages to topics (Redis Streams) and subscribing to topics to process them. This decouples your services, allowing them to be developed, deployed, and scaled independently.

## Getting Started

### 1. Installation

`npm install cortexium`

### 2. Running the RPC Example

The best way to understand Cortexium is to run the included example. It demonstrates a common microservice pattern: an `api-gateway` service making a request to a `calculator-service`.

### Step 1: Start the Calculator Service

In your terminal, navigate to your project folder and run:

`node examples/calculator-service.js`

You will see the output: `[Calculator] is online and ready.` This service is now listening for tasks on the `add` topic.

### Step 2: Start the API Gateway

In a **second terminal**, run the API gateway:

`node examples/api-gateway.js`

After a moment, you will see the gateway send its request and receive the correct result from the calculator service: `[API Gateway] SUCCESS! Result for 15 + 27 is 42`.

## Usage Patterns

Cortexium supports several common communication patterns for microservices.

### 1. One-to-Group (Load-Balanced RPC)

This is the most common and powerful pattern. A service sends a request to a general topic, and **any one** of the available worker services can pick it up and process it. This is ideal for scaling tasks horizontally.

The included example demonstrates this perfectly. The `api-gateway` emits a task to the `add` topic, and the `calculator-service` listens as part of the `'calculator-service'` group.

**To see load balancing in action:**

1. Start a second instance of the calculator service in a new terminal: `node examples/calculator-service.js`.
2. Modify `api-gateway.js` to send multiple requests in a loop.
3. You will see the "Received add request for..." logs appearing in **both** calculator terminals as Redis distributes the work between them.

**Code Example (`api-gateway.js`):**

`// A service emits to a general topic, like 'add'
apiNode.emit('add', [15, 27], (err, result) => {
    // ... handle reply
});`

**Code Example (`calculator-service.js`):**

`// Any node of type 'calculator-service' can handle the request.
calculatorNode.sub('add', (numbers) => {
    return numbers[0] + numbers[1];
});`

### 2. One-to-One (Direct Messaging)

Sometimes, you need to send a message to a *specific* instance of a service. For example, you might want to send a command to a specific logger or session manager. You can achieve this by having a node subscribe to a topic that includes its own unique `nodeId`.

**Example: A service that can receive direct commands.**

`// in a hypothetical monitoring-service.js
const Cortexium = require('./index');

async function startMonitor() {
    const monitorNode = new Cortexium({
        prefix: 'my-app',
        url: 'redis://127.0.0.1:6379',
        type: 'monitor-service',
    });

    await monitorNode.ready();
    console.log(`[Monitor] Node ${monitorNode.nodeId} is online.`);

    // This node subscribes to a topic that is unique to it.
    const directTopic = `commands:${monitorNode.nodeId}`;
    monitorNode.sub(directTopic, (command) => {
        console.log(`[Monitor] Received direct command:`, command);
        if (command.action === 'report_status') {
            return { status: 'OK', memoryUsage: process.memoryUsage() };
        }
    });
}
startMonitor();`

## Extending with Services

Cortexium is designed to be extensible through services. These are modules that can be loaded into a Cortexium node to provide specific functionality.

### Available Services

- [**Scheduler](https://www.google.com/search?q=./services/scheduler/README.md):** A service that allows you to schedule tasks to be executed at a later time.

## Performance & Diagnostics

The library includes scripts to help you test performance and diagnose potential issues. Make sure to define the `test` and `diagnose` scripts in your `package.json`:

`"scripts": {
  "test": "DEBUG=cortexium:* node performance-test.js",
  "diagnose": "node diagnostics.js"
}`

### Running the Performance Test

The `performance-test.js` script simulates a high-throughput scenario where 100 requests are sent concurrently. This is the best way to measure the system's performance under a realistic production load.

To run the test:

`npm run test`

The output will show you two key metrics:

- **Throughput:** The total number of operations (request/reply cycles) the system can handle per second. This is the most important measure of the system's capacity.
- **Average Latency:** The average time for a single request to complete its round trip. In a high-traffic test, this number will include the time a message spent waiting in the queue, which is normal.

### Diagnosing Event Loop Issues

The Node.js event loop is single-threaded. If a piece of code blocks this thread, it can prevent your entire application from processing new requests. The `diagnostics.js` script runs the performance test while monitoring the health of the event loop.

To run the diagnostics:

`npm run diagnose`

This will print an "Event Loop Lag" measurement every second. A healthy, non-blocking application will have a lag of less than 20ms. If you see this number spike, it's a sign that something is blocking the event loop.

## API Reference

### `new Cortexium(options)`

Creates a new Cortexium node.

- `options` `<Object>`
    - `prefix` `<string>` **Required.** A namespace for all Redis keys (e.g., `'my-app'`).
    - `url` `<string>` **Required.** The connection URL for your Redis server (e.g., `'redis://127.0.0.1:6379'`).
    - `type` `<string>` **Required.** The type of this service (e.g., `'order-service'`).
    - `services` `<Array>` **Optional.** An array of service classes to load into the node.

### `cortexNode.ready()`

Returns a `Promise` that resolves when the node has successfully connected to Redis and is ready to send and receive messages. **You should always `await` this method after creating a new node.**

### `cortexNode.sub(topic, handler)`

Subscribes to a topic and provides a handler function to process messages.

- `topic` `<string>` The name of the topic to listen to.
- `handler` `<Function>` An `async` function that will be executed for each message.
    - It receives the `payload` of the message as its first argument.
    - The value returned by the handler will be automatically sent back as the reply if the sender is waiting for one.

### `cortexNode.emit(topic, payload, [callback])`

Publishes a message to a topic.

- `topic` `<string>` The name of the topic to publish to.
- `payload` `<Object>` The data to send. This should be a JSON-serializable object.
- `callback` `<Function>` **Optional.** If provided, Cortexium will wait for a reply.
    - The callback receives `(err, result)`. `result` is the value returned by the subscriber's handler function.
    - If no reply is received within 5 seconds, the callback will be called with a timeout error.
    - If you omit the callback, the message is "fire-and-forget."

### `cortexNode.shutdown()`

Returns a `Promise` that resolves after gracefully disconnecting from Redis and shutting down any loaded services.

## Architecture Deep Dive

Cortexium's reliability, especially in a scaled-out environment with multiple instances of the same service, comes from its dual-manager architecture. Each Cortexium node opens two underlying consumer managers to handle different types of traffic.

1. **Topic Manager:** This manager handles all general `sub` and `emit` calls. It joins a Redis Consumer Group named after the node's `type` (e.g., `my-app:order-service`). This is the **shared, public channel**. If you run 5 instances of `order-service`, they all share the work for incoming topics, providing natural load balancing.
2. **Reply Manager:** This manager handles the **private reply channel**. It creates a consumer group with a unique name for each node instance (e.g., `my-app:group_for_...<random_node_id>`). When a node emits a message with a callback, it tells the recipient to send the reply to its unique reply topic. This ensures the reply comes back to the correct originating instance, avoiding crosstalk and guaranteeing reliable request/reply.

## Debugging

Cortexium uses the `debug` library for detailed, namespaced logging. To see the logs, set the `DEBUG` environment variable when running your application.

- **See all Cortexium logs:**
    
    `DEBUG=cortexium:* node your-service.js`
    
- **See only emit and reply logs:**
    
    `DEBUG=cortexium:emit,cortexium:reply node your-service.js`
    

## License

MIT