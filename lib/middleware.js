/**
 * Express-style middleware pipeline for Cortexium.
 * Allows cross-cutting concerns like auth, logging, metrics, tracing.
 */
class MiddlewarePipeline {
    constructor() {
        this.middlewares = [];
    }

    /**
     * Register a middleware function.
     * @param {string|RegExp|Function} pattern - Topic pattern to match, or middleware function
     * @param {Function} fn - async (ctx, next) => void
     */
    use(pattern, fn) {
        if (typeof pattern === 'function') {
            fn = pattern;
            pattern = '*';
        }
        this.middlewares.push({ pattern, fn });
    }

    /**
     * Check if a middleware matches a topic.
     */
    _matches(pattern, topic) {
        if (pattern === '*') return true;
        if (typeof pattern === 'string') {
            // Support wildcards: payments.* matches payments.process
            const regex = new RegExp('^' + pattern.replace(/\./g, '\\.').replace(/\*/g, '[^.]+').replace(/>/g, '.+') + '$');
            return regex.test(topic);
        }
        if (pattern instanceof RegExp) {
            return pattern.test(topic);
        }
        return false;
    }

    /**
     * Execute middleware chain for a given message.
     * @param {string} topic - The topic being processed
     * @param {Message} message - The message object
     * @param {MessageContext} ctx - The message context
     * @param {Function} finalHandler - The actual subscription handler
     */
    async execute(topic, message, ctx, finalHandler) {
        const matching = this.middlewares.filter(m => this._matches(m.pattern, topic));
        let index = 0;
        let result;

        const next = async () => {
            if (index < matching.length) {
                const middleware = matching[index++];
                await middleware.fn(ctx, next);
            } else {
                result = await finalHandler(message, ctx);
            }
        };

        await next();
        return result;
    }
}

module.exports = MiddlewarePipeline;
