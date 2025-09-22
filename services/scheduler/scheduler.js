// cortexium/services/scheduler.js
const Redis = require('ioredis');
const debug = require('debug')('cortexium:service:scheduler');

const SCHEDULER_CONFIG = {
    checkInterval: 1000, // Check for due tasks every 1 second
    sortedSetKey: 'cortexium:scheduler:tasks',
    scheduleTopic: 'scheduler:add', // Topic to listen for new tasks
};

class SchedulerModule {
    /**
     * @param {object} options
     * @param {import('../../index')} options.cortexNode The Cortexium node instance that is loading this service.
     */
    constructor({ cortexNode }) {
        if (!cortexNode) {
            throw new Error('SchedulerModule requires a cortexNode instance.');
        }
        this.cortexNode = cortexNode;
        this.redis = new Redis(cortexNode.options.url);
        this.isShutdown = false;
    }

    async start() {
        debug('Scheduler service module started.');
        this.cortexNode.sub(SCHEDULER_CONFIG.scheduleTopic, this.handleNewTask.bind(this));
        this.poll();
    }

    async handleNewTask(task) {
        const { topic, payload, delay } = task;
        if (!topic || !payload || typeof delay !== 'number') {
            debug('Invalid task received:', task);
            return;
        }

        const scheduledTime = Date.now() + delay;
        const taskJson = JSON.stringify({ topic, payload });
        await this.redis.zadd(SCHEDULER_CONFIG.sortedSetKey, scheduledTime, taskJson);
        debug(`Scheduled task to topic "${topic}" in ${delay}ms`);
    }

    async poll() {
        if (this.isShutdown) return;

        try {
            const now = Date.now();
            
            const dueTasks = await this.redis.zrangebyscore(
                SCHEDULER_CONFIG.sortedSetKey, 0, now
            );

            if (dueTasks.length > 0) {
                debug(`Found ${dueTasks.length} due tasks to process.`);

                await this.redis.zremrangebyscore(
                    SCHEDULER_CONFIG.sortedSetKey, 0, now
                );
                
                for (const taskJson of dueTasks) {
                    try {
                        const task = JSON.parse(taskJson);
                        debug(`Firing task: emit to topic "${task.topic}"`);
                        this.cortexNode.emit(task.topic, task.payload);
                    } catch (parseError) {
                        console.error('Error parsing task from Redis:', parseError);
                    }
                }
            }
        } catch (err) {
            console.error('Scheduler poll failed:', err);
        } finally {
            setTimeout(() => this.poll(), SCHEDULER_CONFIG.checkInterval);
        }
    }

    async shutdown() {
        this.isShutdown = true;
        await this.redis.quit();
    }
}

module.exports = SchedulerModule;