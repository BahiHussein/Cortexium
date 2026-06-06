const { nanoid } = require('nanoid/non-secure');

/**
 * Structured message envelope for Cortexium.
 * Provides a consistent format for all messages regardless of transport.
 */
class Message {
    constructor(options = {}) {
        this.id = options.id || nanoid();
        this.topic = options.topic;
        this.type = options.type || 'event'; // 'event' | 'rpc' | 'stream'
        this.payload = options.payload;
        this.headers = options.headers || {};
        this.timestamp = options.timestamp || Date.now();
        this.sourceNode = options.sourceNode;
        this.correlationId = options.correlationId || nanoid();
        this.replyTo = options.replyTo;
        this.attempt = options.attempt || 1;
        this.maxAttempts = options.maxAttempts || 3;
        this.ttl = options.ttl || 30000;
        this.error = options.error || null;
        this.streamChunk = options.streamChunk || false;
        this.streamEnd = options.streamEnd || false;
    }

    toJSON() {
        return {
            id: this.id,
            topic: this.topic,
            type: this.type,
            payload: this.payload,
            headers: this.headers,
            timestamp: this.timestamp,
            sourceNode: this.sourceNode,
            correlationId: this.correlationId,
            replyTo: this.replyTo,
            attempt: this.attempt,
            maxAttempts: this.maxAttempts,
            ttl: this.ttl,
            error: this.error,
            streamChunk: this.streamChunk,
            streamEnd: this.streamEnd,
        };
    }

    static fromJSON(data) {
        return new Message(data);
    }

    /**
     * Check if the message has exceeded its TTL.
     */
    isExpired() {
        return Date.now() - this.timestamp > this.ttl;
    }

    /**
     * Create an error reply message.
     */
    static errorReply(original, errorCode, errorMessage, details = {}) {
        return new Message({
            id: nanoid(),
            topic: original.replyTo,
            type: 'rpc',
            correlationId: original.correlationId,
            sourceNode: original.sourceNode,
            replyTo: undefined,
            error: {
                code: errorCode,
                message: errorMessage,
                details,
            },
            payload: null,
            timestamp: Date.now(),
        });
    }

    /**
     * Create a success reply message.
     */
    static reply(original, payload, opts = {}) {
        return new Message({
            id: nanoid(),
            topic: original.replyTo,
            type: original.type,
            correlationId: original.correlationId,
            sourceNode: original.sourceNode,
            replyTo: undefined,
            payload,
            timestamp: Date.now(),
            streamChunk: opts.streamChunk || false,
            streamEnd: opts.streamEnd || false,
        });
    }
}

module.exports = Message;
