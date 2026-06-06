const { connect, JSONCodec, StringCodec, createInbox, headers: createHeaders } = require('nats');
const Message = require('./message');

const debug = {
    transport: require('debug')('cortexium:transport'),
};

const JSON_CODEC = JSONCodec();

/**
 * NATS Transport Layer for Cortexium.
 * Wraps Core NATS (pub/sub, request/reply, queue groups) and
 * optionally JetStream for persistent messaging.
 */
class NatsTransport {
    constructor(options = {}) {
        this.url = options.url;
        this.prefix = options.prefix || 'cortexium';
        this.nodeId = options.nodeId;
        this.nodeType = options.nodeType;
        this.connection = null;
        this.jetstream = null;
        this.subscriptions = new Map(); // subject -> subscription
        this.isShutdown = false;
    }

    /**
     * Establish connection to NATS server.
     */
    async connect() {
        if (this.connection) return;

        debug.transport(`Connecting to NATS at ${this.url}...`);

        this.connection = await connect({
            servers: this.url,
            name: `${this.nodeType}-${this.nodeId}`,
            reconnectTimeWait: 2000,
            maxReconnectAttempts: -1,
        });

        debug.transport(`Connected to NATS. Server: ${this.connection.getServer()}`);

        // Setup JetStream context (optional, for persistent messaging)
        try {
            this.jetstream = this.connection.jetstream();
            debug.transport('JetStream context initialized');
        } catch (err) {
            debug.transport('JetStream not available:', err.message);
        }

        // Handle disconnections
        (async () => {
            for await (const status of this.connection.status()) {
                if (status.type === 'disconnect') {
                    debug.transport('Disconnected from NATS');
                } else if (status.type === 'reconnect') {
                    debug.transport('Reconnected to NATS');
                }
            }
        })().catch(() => {});
    }

    /**
     * Gracefully close the connection.
     */
    async disconnect() {
        this.isShutdown = true;

        // Unsubscribe all
        for (const [subject, sub] of this.subscriptions) {
            debug.transport(`Unsubscribing from ${subject}`);
            sub.unsubscribe();
        }
        this.subscriptions.clear();

        if (this.connection) {
            await this.connection.drain();
            await this.connection.close();
            debug.transport('NATS connection closed');
            this.connection = null;
        }
    }

    /**
     * Prefix a subject with the application namespace.
     */
    _subject(topic) {
        return `${this.prefix}.${topic}`;
    }

    /**
     * Encode a Message to NATS payload + headers.
     */
    _encode(msg) {
        const data = JSON_CODEC.encode(msg.toJSON());
        const hdrs = createHeaders();
        hdrs.set('Cortexium-Version', '2.0');
        hdrs.set('Cortexium-Node', this.nodeId);
        hdrs.set('Cortexium-Type', this.nodeType);
        return { data, headers: hdrs };
    }

    /**
     * Decode NATS payload + headers to a Message.
     */
    _decode(rawData, msg) {
        try {
            const data = JSON_CODEC.decode(rawData);
            return Message.fromJSON(data);
        } catch (err) {
            // Fallback: wrap raw data as payload
            return new Message({
                topic: msg.subject.replace(`${this.prefix}.`, ''),
                payload: rawData,
                sourceNode: msg.headers?.get('Cortexium-Node') || 'unknown',
            });
        }
    }

    /**
     * Publish a broadcast message (all subscribers receive it).
     */
    async publish(topic, message) {
        if (!this.connection) throw new Error('Transport not connected');
        const subject = this._subject(topic);
        const { data, headers } = this._encode(message);
        this.connection.publish(subject, data, { headers });
        debug.transport(`Published to ${subject} (broadcast)`);
    }

    /**
     * Subscribe to a topic.
     * @param {string} topic - Subject to subscribe to
     * @param {Function} handler - async (Message, Context) => any
     * @param {Object} options
     * @param {string} options.queue - Queue group for load balancing (RPC mode)
     * @param {boolean} options.broadcast - If true, no queue group (all nodes receive)
     */
    async subscribe(topic, handler, options = {}) {
        if (!this.connection) throw new Error('Transport not connected');

        const subject = this._subject(topic);
        const queue = options.broadcast ? undefined : (options.queue || this.nodeType);
        const subKey = `${subject}:${queue || 'broadcast'}`;

        if (this.subscriptions.has(subKey)) {
            throw new Error(`Already subscribed to ${topic} with queue=${queue || 'broadcast'}`);
        }

        debug.transport(`Subscribing to ${subject} (queue=${queue || 'broadcast'})`);

        const subscription = this.connection.subscribe(subject, {
            queue,
            callback: (err, msg) => {
                if (err) {
                    debug.transport(`Subscription error on ${subject}:`, err);
                    return;
                }
                this._handleMessage(msg, handler, options);
            },
        });

        this.subscriptions.set(subKey, subscription);
        return subscription;
    }

    /**
     * Unsubscribe from a topic.
     */
    async unsubscribe(topic, options = {}) {
        const subject = this._subject(topic);
        const queue = options.broadcast ? undefined : (options.queue || this.nodeType);
        const subKey = `${subject}:${queue || 'broadcast'}`;

        const sub = this.subscriptions.get(subKey);
        if (sub) {
            sub.unsubscribe();
            this.subscriptions.delete(subKey);
            debug.transport(`Unsubscribed from ${subject}`);
        }
    }

    /**
     * Internal message handler wrapper.
     */
    async _handleMessage(msg, handler, options) {
        const message = this._decode(msg.data, msg);
        const isRPC = !!msg.reply;

        const ctx = new MessageContext(this, msg, message);

        try {
            const result = await handler(message, ctx);

            // If it's an RPC and handler returned something, auto-reply
            if (isRPC && result !== undefined && !ctx._replied && !ctx._streaming) {
                const replyMsg = Message.reply(message, result);
                const { data } = this._encode(replyMsg);
                msg.respond(data);
                debug.transport(`Auto-replied to ${message.correlationId}`);
            }

            // If streaming and not explicitly ended, send end marker
            if (ctx._streaming && !ctx._ended) {
                const endMsg = Message.reply(message, null, { streamEnd: true });
                const { data } = this._encode(endMsg);
                msg.respond(data);
                debug.transport(`Stream ended for ${message.correlationId}`);
            }
        } catch (err) {
            debug.transport(`Handler error on ${message.topic}:`, err.message);

            if (isRPC && !ctx._replied) {
                const errorMsg = Message.errorReply(
                    message,
                    err.code || 'HANDLER_ERROR',
                    err.message,
                    { stack: err.stack }
                );
                const { data } = this._encode(errorMsg);
                msg.respond(data);
            }
        }
    }

    /**
     * Send an RPC request and await a reply.
     * @param {string} topic - Target topic
     * @param {Message} message - Request message
     * @param {Object} options
     * @param {number} options.timeout - Timeout in ms (default 5000)
     * @returns {Promise<Message>} - Reply message
     */
    async request(topic, message, options = {}) {
        if (!this.connection) throw new Error('Transport not connected');

        const subject = this._subject(topic);
        const timeout = options.timeout || 5000;
        const { data, headers } = this._encode(message);

        debug.transport(`Request to ${subject} (timeout=${timeout}ms)`);

        try {
            const response = await this.connection.request(subject, data, {
                headers,
                timeout,
            });

            const replyMsg = this._decode(response.data, response);
            debug.transport(`Reply received for ${message.correlationId}`);
            return replyMsg;
        } catch (err) {
            if (err.code === 'TIMEOUT') {
                const timeoutErr = new Error(`Request timed out after ${timeout}ms`);
                timeoutErr.code = 'TIMEOUT';
                throw timeoutErr;
            }
            // NATS fast-fails with 503 when no node is subscribed to the subject.
            // Surface a clear, stable error code rather than the raw '503'.
            if (err.code === '503' || err.code === 'NO_RESPONDERS') {
                const nrErr = new Error(`No responders available for "${topic}"`);
                nrErr.code = 'NO_RESPONDERS';
                throw nrErr;
            }
            throw err;
        }
    }

    /**
     * Publish via JetStream for persistence (optional).
     */
    async jetstreamPublish(topic, message) {
        if (!this.jetstream) throw new Error('JetStream not available');
        const subject = this._subject(topic);
        const { data, headers } = this._encode(message);
        await this.jetstream.publish(subject, data, { headers });
        debug.transport(`JetStream published to ${subject}`);
    }
}

/**
 * Context object passed to message handlers.
 */
class MessageContext {
    constructor(transport, rawMsg, message) {
        this.transport = transport;
        this.rawMsg = rawMsg;
        this.message = message;
        this._replied = false;
        this._streaming = false;
        this._ended = false;
    }

    /**
     * Manually send a reply (useful for async workflows).
     */
    reply(payload) {
        if (!this.rawMsg.reply) throw new Error('Cannot reply: not an RPC message');
        if (this._replied) throw new Error('Already replied');

        const replyMsg = Message.reply(this.message, payload);
        const { data } = this.transport._encode(replyMsg);
        this.rawMsg.respond(data);
        this._replied = true;
    }

    /**
     * Start a streaming response.
     */
    stream() {
        if (!this.rawMsg.reply) throw new Error('Cannot stream: not an RPC message');
        this._streaming = true;
        this._ended = false;
        return {
            send: (chunk) => this._sendChunk(chunk),
            end: () => this._endStream(),
        };
    }

    _sendChunk(chunk) {
        if (this._ended) throw new Error('Stream already ended');
        const replyMsg = Message.reply(this.message, chunk, { streamChunk: true });
        const { data } = this.transport._encode(replyMsg);
        this.rawMsg.respond(data);
    }

    _endStream() {
        if (this._ended) return;
        const endMsg = Message.reply(this.message, null, { streamEnd: true });
        const { data } = this.transport._encode(endMsg);
        this.rawMsg.respond(data);
        this._ended = true;
    }

    /**
     * Publish a message to another topic.
     */
    async publish(topic, payload) {
        const msg = new Message({
            topic,
            payload,
            sourceNode: this.transport.nodeId,
        });
        await this.transport.publish(topic, msg);
    }
}

module.exports = NatsTransport;
