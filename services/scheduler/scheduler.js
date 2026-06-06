// cortexium/services/scheduler/scheduler.js
// NATS-native, multi-node-safe scheduler.
//
// Design: leader election via a JetStream KV lease.
//   - Any scheduler node can RECEIVE schedule requests (queue group) and persist
//     them to a shared KV bucket. No task is tied to the node that received it.
//   - Exactly ONE node holds a lease at a time (the leader). Only the leader arms
//     in-memory timers and fires tasks, so each task fires once across the cluster.
//   - The leader watches the KV bucket, so new/removed tasks are reflected live.
//   - If the leader dies, its lease expires (TTL) and a standby takes over within
//     seconds, replaying pending tasks from KV. This gives HA without double-firing.
//
// If JetStream is unavailable, the node runs standalone (always leader, in-memory
// only) and tasks do not survive a restart.
const debug = require('debug')('cortexium:service:scheduler');
const { nanoid } = require('nanoid/non-secure');

const SCHEDULER_CONFIG = {
    scheduleTopic: 'scheduler:add',
    tasksBucket: 'scheduler_tasks',
    leaderBucket: 'scheduler_leader',
    leaseKey: 'leader',
    leaseTtl: 6000,       // a lease is considered dead after this with no renewal
    renewInterval: 2000,  // how often the leader renews / standbys probe
};

class SchedulerModule {
    /**
     * @param {object} options
     * @param {import('../../index')} options.cortexNode The Cortexium node instance.
     */
    constructor({ cortexNode }) {
        if (!cortexNode) {
            throw new Error('SchedulerModule requires a cortexNode instance.');
        }
        this.cortexNode = cortexNode;
        this.nodeId = cortexNode.nodeId;
        this.isShutdown = false;

        this.tasks = new Map();   // id -> { topic, payload, scheduledTime, timeoutId }  (leader only)
        this.kv = null;           // tasks bucket
        this.leaseKv = null;      // leader-lease bucket
        this.isLeader = false;
        this.electionTimer = null;
        this.watchIter = null;
    }

    async start() {
        debug(`Scheduler module starting (node ${this.nodeId}).`);

        // Any node may receive schedule requests; load-balanced via queue group.
        await this.cortexNode.sub(SCHEDULER_CONFIG.scheduleTopic, this.handleNewTask.bind(this));

        await this._initKv();

        if (this.kv && this.leaseKv) {
            // Multi-node mode: contend for leadership, react on the result.
            await this._electionTick();
            this.electionTimer = setInterval(() => {
                this._electionTick().catch((e) => debug('election tick error:', e.message));
            }, SCHEDULER_CONFIG.renewInterval);
        } else {
            // Standalone mode: this node is the sole, permanent leader (in-memory only).
            this.isLeader = true;
            debug('Running standalone (no JetStream); tasks will not survive a restart.');
        }
    }

    async _initKv() {
        try {
            const js = this.cortexNode.transport.jetstream;
            if (!js) {
                debug('JetStream not available; scheduler runs standalone in-memory.');
                return;
            }
            const prefix = this.cortexNode.options?.prefix || 'cortexium';
            this.kv = await js.views.kv(`${prefix}_${SCHEDULER_CONFIG.tasksBucket}`, { history: 1 });
            // Lease bucket auto-expires entries, so a dead leader's lease frees itself.
            this.leaseKv = await js.views.kv(`${prefix}_${SCHEDULER_CONFIG.leaderBucket}`, {
                history: 1,
                ttl: SCHEDULER_CONFIG.leaseTtl,
            });
            debug('JetStream KV ready (tasks + leader lease).');
        } catch (err) {
            debug('KV init failed; scheduler runs standalone in-memory:', err.message);
            this.kv = null;
            this.leaseKv = null;
        }
    }

    // ---- Scheduling (runs on whichever node receives the request) ----------

    async handleNewTask(task) {
        const { topic, payload, delay, id } = task;
        if (!topic || !payload || typeof delay !== 'number') {
            debug('Invalid task received:', task);
            return;
        }

        const taskId = id || nanoid();
        const scheduledTime = Date.now() + delay;
        debug(`Persisting task "${taskId}" -> "${topic}" in ${delay}ms`);

        if (this.kv) {
            // Persist only. The leader's watch will arm the timer (even on this node
            // if it happens to be the leader), keeping a single source of truth.
            try {
                await this.kv.put(taskId, JSON.stringify({ id: taskId, topic, payload, scheduledTime }));
            } catch (err) {
                debug(`Failed to persist task "${taskId}":`, err.message);
            }
        } else {
            // Standalone: arm directly, no watch exists.
            this._scheduleInMemory(taskId, topic, payload, scheduledTime);
        }
    }

    // ---- Leader election ----------------------------------------------------

    async _electionTick() {
        if (this.isShutdown || !this.leaseKv) return;

        const now = Date.now();
        const mine = JSON.stringify({ nodeId: this.nodeId, ts: now });

        let entry;
        try {
            entry = await this.leaseKv.get(SCHEDULER_CONFIG.leaseKey);
        } catch (err) {
            // Can't read the lease (e.g. partition) — safest is to assume we lost it.
            debug('lease read failed, stepping down:', err.message);
            await this._stepDown();
            return;
        }

        try {
            if (!entry || entry.operation === 'DEL' || entry.operation === 'PURGE') {
                // No leader — try to claim atomically (only one create wins).
                await this.leaseKv.create(SCHEDULER_CONFIG.leaseKey, mine);
                await this._becomeLeader();
                return;
            }

            const data = JSON.parse(Buffer.from(entry.value).toString());
            const fresh = now - data.ts < SCHEDULER_CONFIG.leaseTtl;

            if (data.nodeId === this.nodeId) {
                // We hold it — renew via CAS. Losing the CAS means we were superseded.
                await this.leaseKv.update(SCHEDULER_CONFIG.leaseKey, mine, entry.revision);
                if (!this.isLeader) await this._becomeLeader();
            } else if (!fresh) {
                // Held by a node that stopped renewing — take over via CAS.
                await this.leaseKv.update(SCHEDULER_CONFIG.leaseKey, mine, entry.revision);
                await this._becomeLeader();
            } else {
                // A healthy leader exists elsewhere.
                if (this.isLeader) await this._stepDown();
            }
        } catch (err) {
            // Lost a create/update race against another node — remain/return follower.
            debug('lease contention, staying follower:', err.message);
            if (this.isLeader) await this._stepDown();
        }
    }

    async _becomeLeader() {
        if (this.isLeader) return;
        this.isLeader = true;
        debug(`Node ${this.nodeId} became LEADER.`);
        await this._startWatch(); // replays current KV state -> arms pending timers
    }

    async _stepDown() {
        if (!this.isLeader) return;
        this.isLeader = false;
        debug(`Node ${this.nodeId} stepped down (now follower).`);
        await this._stopWatch();
        this._clearAllTimers(); // tasks stay in KV; the new leader will arm them
    }

    // ---- KV watch (leader only): KV is the source of truth for timers -------

    async _startWatch() {
        if (this.watchIter) return;
        try {
            this.watchIter = await this.kv.watch();
        } catch (err) {
            debug('failed to start KV watch:', err.message);
            return;
        }
        (async () => {
            try {
                for await (const e of this.watchIter) {
                    if (!this.isLeader || this.isShutdown) break;
                    if (e.operation === 'DEL' || e.operation === 'PURGE') {
                        this._clearTimer(e.key);
                    } else {
                        try {
                            const task = JSON.parse(Buffer.from(e.value).toString());
                            this._scheduleInMemory(task.id, task.topic, task.payload, task.scheduledTime);
                        } catch (parseErr) {
                            debug('watch: bad task entry, skipping:', parseErr.message);
                        }
                    }
                }
            } catch (err) {
                debug('watch loop ended:', err.message);
            }
        })();
    }

    async _stopWatch() {
        if (this.watchIter) {
            try { this.watchIter.stop(); } catch { /* already stopped */ }
            this.watchIter = null;
        }
    }

    // ---- Timer management (leader only) ------------------------------------

    _scheduleInMemory(taskId, topic, payload, scheduledTime) {
        // Replace any existing timer for this id so watch replays can't double-arm.
        const existing = this.tasks.get(taskId);
        if (existing) clearTimeout(existing.timeoutId);

        const delay = Math.max(0, scheduledTime - Date.now());
        const timeoutId = setTimeout(() => this._fireTask(taskId, topic, payload), delay);
        this.tasks.set(taskId, { topic, payload, scheduledTime, timeoutId });
    }

    async _fireTask(taskId, topic, payload) {
        this.tasks.delete(taskId);

        // Fence: only fire while we still believe we're the leader. A node that lost
        // leadership clears its timers on step-down, bounding any duplicate window to
        // one renew interval.
        if (!this.isLeader || this.isShutdown) return;

        try {
            debug(`Firing task "${taskId}" -> "${topic}"`);
            await this.cortexNode.emit(topic, payload);
        } catch (err) {
            debug(`Failed to fire task "${taskId}":`, err.message);
        } finally {
            if (this.kv) {
                try { await this.kv.delete(taskId); } catch (e) { debug(`KV delete "${taskId}" failed:`, e.message); }
            }
        }
    }

    _clearTimer(taskId) {
        const t = this.tasks.get(taskId);
        if (t) {
            clearTimeout(t.timeoutId);
            this.tasks.delete(taskId);
        }
    }

    _clearAllTimers() {
        for (const t of this.tasks.values()) clearTimeout(t.timeoutId);
        this.tasks.clear();
    }

    async shutdown() {
        this.isShutdown = true;
        if (this.electionTimer) {
            clearInterval(this.electionTimer);
            this.electionTimer = null;
        }
        await this._stopWatch();
        this._clearAllTimers();

        // Release the lease promptly so a standby can take over without waiting for TTL.
        if (this.isLeader && this.leaseKv) {
            try { await this.leaseKv.delete(SCHEDULER_CONFIG.leaseKey); } catch { /* best effort */ }
        }
        this.isLeader = false;
        debug('Scheduler stopped');
    }
}

module.exports = SchedulerModule;
