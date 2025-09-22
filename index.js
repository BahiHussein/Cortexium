// cortexium/index.js
const RedisManager = require('./redis-manager');
const { nanoid } = require('nanoid/non-secure');
const { performance } = require('perf_hooks');
const SchedulerModule = require('./services/scheduler/scheduler');

const debug = {
    core: require('debug')('cortexium:core'),
    emit: require('debug')('cortexium:emit'),
    sub: require('debug')('cortexium:sub'),
    reply: require('debug')('cortexium:reply'),
};

class Cortexium {
    constructor({ prefix, url, type, services = [] }) {
        if (!type) throw new Error('Node type must be defined');

        this.options = { prefix, url, type };
        this.nodeType = type;
        this.nodeId = nanoid();
        this.replyHandlers = new Map();
        this.loadedServices = [];
        debug.core(`Initializing node ${this.nodeId} of type "${type}"`);

        this.topicManager = new RedisManager({ prefix, url });
        this.replyManager = new RedisManager({ prefix, url });

        const replyTopic = `reply_to_${this.nodeId}`;
        const replyGroup = `group_for_${this.nodeId}`;

        this.replyManager.registerHandler(replyTopic, replyGroup, async (data) => {
            const handler = this.replyHandlers.get(data.correlationId);
            if (handler) {
                data.timestamps.clientReceivedAt = performance.now();
                debug.reply(`Received reply for correlationId: ${data.correlationId}`);
                handler(data.error, data.payload, data.timestamps);
                this.replyHandlers.delete(data.correlationId);
            } else {
                debug.reply(`Received unexpected reply for correlationId: ${data.correlationId}`);
            }
        });

        this.replyManager.startConsuming(replyGroup).catch(err => {
            debug.core(`[ERROR] Private reply consumer for ${this.nodeId} failed:`, err);
        });

        this.topicManager.startConsuming(this.nodeType).catch(err => {
            debug.core(`[ERROR] Shared topic consumer for ${this.nodeType} failed:`, err);
        });

        // Load and initialize services
        for (const ServiceClass of services) {
            const serviceInstance = new ServiceClass({ cortexNode: this });
            this.loadedServices.push(serviceInstance);
            debug.core(`Loaded service: ${ServiceClass.name}`);
        }
    }

    async ready() {
        await Promise.all([
            this.topicManager.ready,
            this.replyManager.ready,
        ]);
        debug.core(`Node ${this.nodeId} is ready.`);
        // Start all loaded services
        for (const service of this.loadedServices) {
            if (typeof service.start === 'function') {
                service.start();
            }
        }
    }

    sub(topic, handler) {
        this.topicManager.registerHandler(topic, this.nodeType, async (data) => {
            data.timestamps.serverReceivedAt = performance.now();
            debug.sub(`Received message on topic "${topic}" from node ${data.sourceNode}`);

            const result = await handler(data.payload, data);

            if (data.replyTo) {
                data.timestamps.serverRepliedAt = performance.now();
                debug.sub(`Sending reply for correlationId ${data.correlationId} to topic ${data.replyTo}`);
                this.topicManager.publish(data.replyTo, {
                    correlationId: data.correlationId,
                    payload: result,
                    timestamps: data.timestamps,
                });
            }
        });
    }

    emit(topic, payload, callback) {
        const message = {
            payload,
            sourceNode: this.nodeId,
            timestamps: { clientSentAt: performance.now() },
        };

        if (callback) {
            message.correlationId = nanoid();
            message.replyTo = `reply_to_${this.nodeId}`;

            debug.emit(`Emitting message to topic "${topic}" with reply expected (correlationId: ${message.correlationId})`);

            this.replyHandlers.set(message.correlationId, (err, result, timestamps) => {
                const totalDuration = (timestamps.clientReceivedAt - timestamps.clientSentAt).toFixed(2);
                debug.emit(`Reply for correlationId ${message.correlationId} received. Total round-trip: ${totalDuration}ms`);

                if (err) return callback(new Error(err), null, totalDuration);
                callback(null, result, totalDuration, timestamps);
            });

            setTimeout(() => {
                if (this.replyHandlers.has(message.correlationId)) {
                    debug.emit(`[ERROR] Request timed out for correlationId ${message.correlationId}`);
                    this.replyHandlers.delete(message.correlationId);
                    callback(new Error('Request timed out'));
                }
            }, 5000);
        } else {
            debug.emit(`Emitting fire-and-forget message to topic "${topic}"`);
        }

        this.topicManager.publish(topic, message);
    }

    async shutdown() {
        await Promise.all(this.loadedServices.map(service => {
            if (typeof service.shutdown === 'function') {
                return service.shutdown();
            }
        }));
        await Promise.all([
            this.topicManager.shutdown(),
            this.replyManager.shutdown(),
        ]);
    }
}

Cortexium.services = {
    Scheduler: SchedulerModule,
};

module.exports = Cortexium;