// cortexium/index.js - NATS-based implementation
const { nanoid } = require('nanoid/non-secure');
const { performance } = require('perf_hooks');

const NatsTransport = require('./lib/transport');
const Message = require('./lib/message');
const MiddlewarePipeline = require('./lib/middleware');
const ServiceDiscovery = require('./lib/discovery');

const debug = {
    core: require('debug')('cortexium:core'),
    emit: require('debug')('cortexium:emit'),
    sub: require('debug')('cortexium:sub'),
    reply: require('debug')('cortexium:reply'),
};

/**
 * Cortexium - A high-performance topic-based pub/sub + callback RPC system.
 * Built on NATS for native request/reply, queue groups, and wildcards.
 */
class Cortexium {
    constructor(options = {}) {
        const { prefix, url, type, services = [] } = options;

        if (!type) throw new Error('Node type must be defined');
        if (!url) throw new Error('NATS URL must be defined');

        this.options = { prefix, url, type };
        this.nodeType = type;
        this.nodeId = nanoid();
        this.isReady = false;
        this.isShutdown = false;

        this.transport = new NatsTransport({
            url,
            prefix,
            nodeId: this.nodeId,
            nodeType: type,
        });

        this.middleware = new MiddlewarePipeline();
        this.discovery = new ServiceDiscovery(this.transport, this.nodeId, type, prefix);
        this.loadedServices = [];

        debug.core(`Initializing node ${this.nodeId} of type "${type}"`);

        // Load and initialize services
        for (const ServiceClass of services) {
            const serviceInstance = new ServiceClass({ cortexNode: this });
            this.loadedServices.push(serviceInstance);
            debug.core(`Loaded service: ${ServiceClass.name}`);
        }
    }

    async ready() {
        if (this.isReady) return;

        await this.transport.connect();
        await this.discovery.start();

        // Mark ready before starting services: a service's start() may call
        // node.sub()/subscribe(), which require the node to already be ready.
        this.isReady = true;

        // Start all loaded services
        for (const service of this.loadedServices) {
            if (typeof service.start === 'function') {
                await service.start();
            }
        }

        debug.core(`Node ${this.nodeId} is ready.`);
    }

    /**
     * Register middleware for topic pattern matching.
     * @param {string|RegExp|Function} pattern - Topic pattern or middleware function
     * @param {Function} fn - async (ctx, next) => void
     */
    use(pattern, fn) {
        this.middleware.use(pattern, fn);
    }

    /**
     * Subscribe to a topic for RPC (load-balanced across queue group).
     * Only ONE node in the queue group will receive each message.
     *
     * @param {string} topic - Topic name
     * @param {Function} handler - async (payload, ctx) => result
     * @param {Object} options
     * @param {string} options.queue - Override queue group (default: nodeType)
     */
    async sub(topic, handler, options = {}) {
        if (!this.isReady) throw new Error('Node not ready. Call await node.ready() first.');

        const wrappedHandler = async (message, ctx) => {
            const startTime = performance.now();
            debug.sub(`[RPC] Received on "${topic}" from ${message.sourceNode} (correlationId: ${message.correlationId})`);

            try {
                const result = await this.middleware.execute(topic, message, ctx, async (msg, innerCtx) => {
                    const result = await handler(msg.payload, innerCtx);
                    // Return value is handled by transport auto-reply
                    return result;
                });

                const duration = (performance.now() - startTime).toFixed(2);
                debug.sub(`[RPC] Handled "${topic}" in ${duration}ms`);
                return result;
            } catch (err) {
                debug.sub(`[RPC] Handler error on "${topic}":`, err.message);
                throw err; // Transport will convert to error reply
            }
        };

        await this.transport.subscribe(topic, wrappedHandler, {
            queue: options.queue || this.nodeType,
            broadcast: false,
        });

        debug.core(`Subscribed to RPC topic "${topic}" (queue: ${options.queue || this.nodeType})`);
    }

    /**
     * Subscribe to a topic for broadcast events.
     * ALL active subscribers will receive each message.
     *
     * @param {string} topic - Topic name
     * @param {Function} handler - async (payload, ctx) => void
     */
    async subscribe(topic, handler) {
        if (!this.isReady) throw new Error('Node not ready. Call await node.ready() first.');

        const wrappedHandler = async (message, ctx) => {
            debug.sub(`[Event] Received on "${topic}" from ${message.sourceNode}`);

            try {
                await this.middleware.execute(topic, message, ctx, async (msg, innerCtx) => {
                    await handler(msg.payload, innerCtx);
                });
            } catch (err) {
                debug.sub(`[Event] Handler error on "${topic}":`, err.message);
                // Events don't have replies, just log
            }
        };

        await this.transport.subscribe(topic, wrappedHandler, { broadcast: true });
        debug.core(`Subscribed to broadcast topic "${topic}"`);
    }

    /**
     * Unsubscribe from a topic.
     */
    async unsub(topic, options = {}) {
        await this.transport.unsubscribe(topic, options);
        debug.core(`Unsubscribed from "${topic}"`);
    }

    /**
     * Publish a broadcast event (fire-and-forget).
     * All subscribers receive this message.
     *
     * @param {string} topic - Topic name
     * @param {*} payload - Message payload
     */
    async publish(topic, payload) {
        if (!this.isReady) throw new Error('Node not ready. Call await node.ready() first.');

        const message = new Message({
            topic,
            type: 'event',
            payload,
            sourceNode: this.nodeId,
        });

        debug.emit(`Publishing event to "${topic}"`);
        await this.transport.publish(topic, message);
    }

    /**
     * Emit a message.
     * - Without callback: fire-and-forget RPC (load-balanced, no reply expected)
     * - With callback: RPC with callback reply
     *
     * @param {string} topic - Topic name
     * @param {*} payload - Message payload
     * @param {Function} [callback] - Optional callback (err, result, duration)
     */
    async emit(topic, payload, callback) {
        if (!this.isReady) throw new Error('Node not ready. Call await node.ready() first.');

        const isRPC = typeof callback === 'function';

        const message = new Message({
            topic,
            type: isRPC ? 'rpc' : 'event',
            payload,
            sourceNode: this.nodeId,
            replyTo: isRPC ? `reply.${this.nodeId}` : undefined,
        });

        if (isRPC) {
            debug.emit(`Emitting RPC to "${topic}" (correlationId: ${message.correlationId})`);
            const startTime = performance.now();

            try {
                const reply = await this.transport.request(topic, message, { timeout: 5000 });
                const duration = performance.now() - startTime;

                if (reply.error) {
                    const err = new Error(reply.error.message);
                    err.code = reply.error.code;
                    err.details = reply.error.details;
                    debug.emit(`RPC error for ${message.correlationId}: ${err.message}`);
                    callback(err, null, duration.toFixed(2));
                } else {
                    debug.emit(`RPC reply for ${message.correlationId} received (${duration.toFixed(2)}ms)`);
                    callback(null, reply.payload, duration.toFixed(2));
                }
            } catch (err) {
                const duration = performance.now() - startTime;
                debug.emit(`RPC failed for ${message.correlationId}: ${err.message}`);
                callback(err, null, duration.toFixed(2));
            }
        } else {
            debug.emit(`Emitting fire-and-forget to "${topic}"`);
            await this.transport.publish(topic, message);
        }
    }

    /**
     * Promise-based RPC request.
     * Modern alternative to emit() with callback.
     *
     * @param {string} topic - Topic name
     * @param {*} payload - Message payload
     * @param {Object} options
     * @param {number} options.timeout - Timeout in ms (default: 5000)
     * @returns {Promise<*>} - Reply payload
     */
    async request(topic, payload, options = {}) {
        if (!this.isReady) throw new Error('Node not ready. Call await node.ready() first.');

        const message = new Message({
            topic,
            type: 'rpc',
            payload,
            sourceNode: this.nodeId,
        });

        debug.emit(`Request to "${topic}" (correlationId: ${message.correlationId})`);

        const reply = await this.transport.request(topic, message, {
            timeout: options.timeout || 5000,
        });

        if (reply.error) {
            const err = new Error(reply.error.message);
            err.code = reply.error.code;
            err.details = reply.error.details;
            throw err;
        }

        return reply.payload;
    }

    /**
     * Discover active nodes of a given type.
     * @param {string} nodeType - Type of service to discover
     * @returns {Promise<Array>} - List of active node info objects
     */
    async discover(nodeType) {
        return this.discovery.discover(nodeType);
    }

    async shutdown() {
        if (this.isShutdown) return;
        this.isShutdown = true;

        // Shutdown services
        await Promise.all(this.loadedServices.map(async (service) => {
            if (typeof service.shutdown === 'function') {
                await service.shutdown();
            }
        }));

        await this.discovery.shutdown();
        await this.transport.disconnect();

        debug.core(`Node ${this.nodeId} shut down.`);
    }
}

// --- Services ---

// Scheduler service is loaded from the existing path but will be updated
const SchedulerModule = require('./services/scheduler/scheduler');

Cortexium.services = {
    Scheduler: SchedulerModule,
};

module.exports = Cortexium;
