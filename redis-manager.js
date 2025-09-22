const Redis = require('ioredis');
const { nanoid } = require('nanoid');
const debug = require('debug')('cortexium:manager');

class RedisManager {
    constructor({ prefix, url }) {
        if (!prefix || !url) {
            throw new Error('Prefix and URL are required');
        }
        this.prefix = prefix;
        this.redis = new Redis(url);
        this.consumerId = nanoid();
        this.isShutdown = false;
        this.handlers = new Map();

        this.ready = new Promise((resolve) => {
            this.redis.on('ready', () => {
                debug(`Redis client for consumer ${this.consumerId} is ready.`);
                resolve();
            });
        });
    }

    _prefixKey(key) {
        return `${this.prefix}:${key}`;
    }

    async _createConsumerGroup(streamKey, groupName) {
        try {
            await this.redis.xgroup('CREATE', streamKey, groupName, '0', 'MKSTREAM');
        } catch (error) {
            if (!error.message.includes('BUSYGROUP')) {
                debug(`Error creating consumer group for ${streamKey}:`, error);
            }
        }
    }

    async registerHandler(key, group, handler) {
        await this.ready;
        const streamKey = this._prefixKey(key);
        const groupName = `${this.prefix}:${group}`;
        this.handlers.set(streamKey, handler);
        await this._createConsumerGroup(streamKey, groupName);
        debug(`Handler registered for stream "${streamKey}" in group "${groupName}"`);
    }

    async startConsuming(group) {
        await this.ready;
        const groupName = `${this.prefix}:${group}`;
        debug(`Consumer ${this.consumerId} starting to listen on group "${groupName}"...`);

        const consume = async () => {
            if (this.isShutdown) return;

            try {
                const streamKeys = Array.from(this.handlers.keys());
                if (streamKeys.length === 0) {
                    await new Promise(resolve => setTimeout(resolve, 1000));
                } else {
                    const response = await this.redis.xreadgroup(
                        'GROUP', groupName, this.consumerId,
                        'COUNT', 10,
                        'BLOCK', 2000,
                        'STREAMS', ...streamKeys,
                        ...streamKeys.map(() => '>')
                    );

                    if (response) {
                        for (const [streamKey, messages] of response) {
                            const handler = this.handlers.get(streamKey);
                            for (const [messageId, messageData] of messages) {
                                const data = JSON.parse(messageData[1]);
                                handler(data)
                                    .then(() => this.redis.xack(streamKey, groupName, messageId))
                                    .catch(err => debug(`[ERROR] Handler/XACK failed for msg ${messageId}:`, err));
                            }
                        }
                    }
                }
            } catch (err) {
                debug('[ERROR] Redis consumer error:', err);
                await new Promise(resolve => setTimeout(resolve, 5000));
            } finally {
                if (!this.isShutdown) {
                    setTimeout(consume, 0);
                }
            }
        };

        consume();
    }
    
    async publish(key, data) {
        await this.ready;
        const streamKey = this._prefixKey(key);
        await this.redis.xadd(streamKey, '*', 'data', JSON.stringify(data));
    }

    async shutdown() {
        this.isShutdown = true;
        await this.redis.quit();
    }
}

module.exports = RedisManager;
