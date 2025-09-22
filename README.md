# Cortexium

**A high-performance, reliable Node.js library for inter-service communication using Redis Streams.**

Cortexium provides a simple and powerful API for building complex, distributed systems and microservices. It allows different services (called "nodes") to perform remote procedure calls (RPC) on each other in a fast, scalable, and fault-tolerant way.

- **High Throughput:** Built on Redis Streams and a non-blocking architecture to handle thousands of concurrent operations per second.
- **Load Balancing:** Automatically distributes work across multiple instances of the same service type.
- **Reliable Request/Reply:** Guarantees that replies to specific requests are delivered to the correct originating node, even in a scaled-out environment.
- **Easy to Use:** A minimal API (`emit` and `sub`) makes it simple to send messages and subscribe to topics.
- **Debuggable:** Includes detailed logging with the `debug` package to provide deep insight into the message flow.
- **Extensible with Services:** Easily load and initialize service modules, such as a task scheduler, within a Cortexium node.

# How Cortexium Works: A Simple Guide

At its core, Cortexium is a communication system for your microservices. It lets your different services (which we call **nodes**) talk to each other reliably and efficiently, using Redis as a central message hub.

Think of your entire application as a large, busy restaurant kitchen.

- **Services (Nodes)** are like specialized chefs (e.g., the Grill Chef, the Sauce Chef).
- **Redis** is the Head Chef who manages the order tickets.
- **Messages** are the order tickets themselves.

---

## The Core Components

### 1. Nodes

A **node** is simply one of your services. When you start a service with Cortexium, you give it a `type`, like `'order-service'` or `'payment-service'`. This is like giving a chef a title. You can have multiple chefs with the same title (e.g., three Grill Chefs) to handle more work.

### 2. Topics

A **topic** is like a category of work. When one service needs another to do something, it sends a message to a specific topic. For example, the `order-service` might send a message to the `payment:process` topic.

### 3. Two Types of Messages

Cortexium sends two kinds of messages, just like a kitchen has two types of orders:

- **Fire-and-Forget (`emit` without a callback):** This is like shouting an announcement in the kitchen, such as "8 salmon just arrived!" You don't expect a direct reply. You just send the information and move on. In Cortexium, you do this by calling `emit` without a callback function.
- **Request/Reply (`emit` with a callback):** This is the most common type of message. It's like placing a specific order, such as "Grill one salmon for Table 5." You need a specific dish to come back to you. In Cortexium, you do this by providing a callback function to `emit`.

---

## The Communication Flow: A Tale of Two Messages

Let's follow the journey of a message to see how Cortexium works its magic.

### Scenario 1: Fire-and-Forget

An `analytics-service` wants to log that a user has just signed in. It doesn't need a response.

1. The `analytics-service` calls `emit('user:login', { userId: 123 });`.
2. Cortexium packages the user data into a message and publishes it to the `user:login` topic on Redis Streams.
3. A `logging-service`, which is subscribed to the `user:login` topic, receives the message and records it.
4. That's it. The flow ends here.

### Scenario 2: The Request/Reply Lifecycle (with `correlationId`)

This is where the real power of Cortexium shines. An `api-gateway` needs to process a payment and must get a confirmation back.

**Part 1: The Request**

1. **Making the Request:** The `api-gateway` calls `emit` with a callback:JavaScript
    
    `apiNode.emit('payment:process', { amount: 50 }, (err, result) => {
      // ... handle the reply later
    });`
    
2. **Creating the "Ticket":** Because a callback is present, Cortexium knows a reply is expected. It immediately does two crucial things:
    - It generates a unique **`correlationId`** (like `V1StGXR8_Z5jdHi6B-myT`). This is the unique ticket number for this specific request.
    - It notes down the **`replyTo`** address, which is the private, unique channel for this specific `api-gateway` node.
3. **Storing the Callback:** The `api-gateway` stores the callback function in a `Map`, using the `correlationId` as the key. It's now waiting for a reply with that specific ticket number.
4. **Sending the Message:** The message is sent to the `payment:process` topic. It contains the payment data, the `correlationId`, and the `replyTo` address.

**Part 2: The Reply**

1. **Receiving the Work:** On the other side, one of the available `payment-service` nodes receives the message. Because multiple `payment-service` nodes might be running, Redis ensures only **one** of them gets this specific task (this is called load balancing).
2. **Processing:** The `payment-service` executes its handler function, processes the payment, and `returns` a result, like `{ success: true, transactionId: 'xyz' }`.
3. **Packaging the Reply:** Cortexium on the `payment-service` node sees the returned value. It also sees the `correlationId` and `replyTo` address from the original message. It packages the result into a new reply message, making sure to include the original `correlationId`.
4. **Sending it Back:** The reply is sent directly to the private `replyTo` channel of the `api-gateway` that made the request.
5. **Matching the Ticket:** The `api-gateway`'s `replyManager` receives the reply. It looks at the `correlationId` (`V1StGXR8_Z5jdHi6B-myT`) and finds the matching callback function it stored in its `replyHandlers` map earlier.
6. **Completing the Cycle:** The callback is executed with the result, and the `api-gateway` can now continue its work. The `correlationId` is removed from the map to clean up.

This entire process happens asynchronously, allowing the `api-gateway` to handle hundreds of other requests while waiting for the payment to be processed. The `correlationId` ensures that no matter how many requests and replies are flying around, each response is delivered to its correct origin.

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

- [**Scheduler](./docs/scheduler.md):** A service that allows you to schedule tasks to be executed at a later time.

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