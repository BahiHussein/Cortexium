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
        try {
            const js = this.transport.jetstream || this.transport.connection?.jetstream?.();
            if (!js) {
                debug('JetStream not available, service discovery disabled');
                return;
            }

            const bucketName = `${this.prefix}_discovery`;
            this.kv = await js.views.kv(bucketName, {
                history: 1,
                ttl: HEARTBEAT_TTL,
            });

            // Start heartbeat
            this._heartbeat();
            this.heartbeatTimer = setInterval(() => this._heartbeat(), HEARTBEAT_INTERVAL);

            debug(`Service discovery started (bucket: ${bucketName})`);
        } catch (err) {
            debug('Service discovery init failed:', err.message);
        }
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

        const nodes = [];
        const keys = await this.kv.keys();
        for await (const key of keys) {
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
