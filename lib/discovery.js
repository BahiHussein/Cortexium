const { nanoid } = require('nanoid/non-secure');

const debug = require('debug')('cortexium:discovery');

const HEARTBEAT_INTERVAL = 10000; // 10 seconds
const HEARTBEAT_TTL = 30000;      // 30 seconds

/**
 * Service Discovery using NATS KV store.
 * Falls back to in-memory if KV is not available.
 */
class ServiceDiscovery {
    constructor(transport, nodeId, nodeType, prefix = 'cortexium') {
        this.transport = transport;
        this.nodeId = nodeId;
        this.nodeType = nodeType;
        this.prefix = prefix;
        this.kv = null;
        this.heartbeatTimer = null;
        this.isShutdown = false;
    }

    async start() {
        const js = this.transport.jetstream || this.transport.connection?.jetstream?.();
        if (!js) {
            debug('JetStream not available, service discovery disabled');
            return;
        }

        const bucketName = `${this.prefix}_discovery`;

        // Opening the bucket can race when many nodes start at once: the node
        // that wins creates the stream, the others get a transient error. Retry
        // so the losers bind to the now-existing bucket instead of silently
        // disabling discovery for themselves.
        for (let attempt = 0; attempt < 5 && !this.kv && !this.isShutdown; attempt++) {
            try {
                this.kv = await js.views.kv(bucketName, { history: 1, ttl: HEARTBEAT_TTL });
            } catch (err) {
                debug(`discovery KV open attempt ${attempt + 1} failed: ${err.message}`);
                await new Promise((r) => setTimeout(r, 50 * (attempt + 1)));
            }
        }

        if (!this.kv) {
            debug('Service discovery disabled (could not open KV bucket)');
            return;
        }

        // Await the first heartbeat so the node is discoverable as soon as
        // ready() resolves, then keep it alive on an interval.
        await this._heartbeat();
        this.heartbeatTimer = setInterval(() => this._heartbeat(), HEARTBEAT_INTERVAL);

        debug(`Service discovery started (bucket: ${bucketName})`);
    }

    async _heartbeat() {
        if (this.isShutdown || !this.kv) return;
        try {
            const info = JSON.stringify({
                nodeId: this.nodeId,
                nodeType: this.nodeType,
                timestamp: Date.now(),
            });
            await this.kv.put(this.nodeId, info);
        } catch (err) {
            debug('Heartbeat failed:', err.message);
        }
    }

    async discover(nodeType) {
        if (!this.kv) {
            debug('Service discovery not available');
            return [];
        }

        // Drain the keys() iterator fully BEFORE issuing any get(). keys() is
        // backed by an ordered consumer; calling get() (another JetStream op)
        // mid-iteration disrupts it and truncates the key list.
        const keyList = [];
        for await (const key of await this.kv.keys()) {
            keyList.push(key);
        }

        const nodes = [];
        for (const key of keyList) {
            try {
                const entry = await this.kv.get(key);
                if (!entry) continue;
                const info = JSONCodec().decode(entry.value);
                if (!nodeType || info.nodeType === nodeType) {
                    nodes.push(info);
                }
            } catch (err) {
                // Entry expired or invalid
            }
        }

        return nodes;
    }

    async shutdown() {
        this.isShutdown = true;
        if (this.heartbeatTimer) {
            clearInterval(this.heartbeatTimer);
            this.heartbeatTimer = null;
        }
        if (this.kv) {
            try {
                await this.kv.delete(this.nodeId);
            } catch (err) {
                debug('Failed to unregister:', err.message);
            }
        }
        debug('Service discovery stopped');
    }
}

// Helper to avoid circular dependency issues
function JSONCodec() {
    return {
        encode: (d) => Buffer.from(JSON.stringify(d)),
        decode: (d) => JSON.parse(Buffer.from(d).toString()),
    };
}

module.exports = ServiceDiscovery;
