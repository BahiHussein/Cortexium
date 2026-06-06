# Cortexium Services

This directory contains services that can be loaded into a Cortexium node to provide additional functionality.

## Scheduler Service

The `scheduler.js` service allows you to schedule tasks to be executed at a later time. It is **multi-node safe by design**: you can run any number of scheduler nodes for high availability, and each task fires exactly once across the whole cluster. State lives in JetStream KV — no Redis or other external store is required.

### How it Works

1. **Scheduling a Task:** To schedule a task, you `emit` a message to the `scheduler:add` topic. The payload should be an object with:
    - `topic`: The topic the task is emitted to when it's due.
    - `payload`: The payload of the task.
    - `delay`: The delay in milliseconds before execution.
    - `id` (optional): A stable task id. If omitted, one is generated.

    Any scheduler node may receive the request (load-balanced via queue group) and persists it to the shared KV bucket `<prefix>_scheduler_tasks`. The task is **not** tied to the node that received it.

2. **Leader election:** Scheduler nodes contend for a lease in the KV bucket `<prefix>_scheduler_leader` (TTL ~6s, renewed every ~2s). Exactly one node holds the lease at a time — the **leader**. Other nodes are hot standbys.

3. **Execution:** Only the leader arms in-memory timers (watching the KV bucket as the source of truth) and `emit`s tasks when due. Because a single node fires, each task fires **exactly once**. A task is removed from KV once it fires.

4. **High availability / failover:** If the leader dies, its lease expires and a standby takes over within a few seconds, replays the pending tasks from KV, and resumes. Tasks that came due during the gap fire immediately on takeover. The fire path is fenced by leadership, so a node that has lost the lease stops firing — bounding any duplicate window to roughly one renewal interval. (For strict once-only semantics under pathological network partitions, make your consumers idempotent — this is inherent to any distributed scheduler.)

5. **Standalone fallback:** If JetStream is unavailable, a single node runs as the permanent leader, in-memory only, and tasks are lost on restart.

### Usage

First, you need to have a Cortexium node running with the `SchedulerModule` loaded.

**`scheduler-service.js`**

JavaScript

`const Cortexium = require('cortexium');
const { Scheduler } = Cortexium.services;

async function startSchedulerService() {
    const schedulerNode = new Cortexium({
        prefix: 'my-app',
        url: 'nats://127.0.0.1:4222',
        type: 'scheduler-service',
        services: [Scheduler]
    });

    await schedulerNode.ready();
    console.log('[Scheduler Service] is online and ready.');
}

startSchedulerService();`

Then, from any other Cortexium node, you can schedule a task by emitting to the `scheduler:add` topic.

**`scheduler-client.js`**

JavaScript

`const Cortexium = require('cortexium');

async function main() {
    const schedulerClientNode = new Cortexium({
        prefix: 'my-app',
        url: 'nats://127.0.0.1:4222',
        type: 'scheduler-client-service',
    });

    const receiverNode = new Cortexium({
        prefix: 'my-app',
        url: 'nats://127.0.0.1:4222',
        type: 'receiver-service',
    });

    await Promise.all([schedulerClientNode.ready(), receiverNode.ready()]);
    console.log('[Scheduler Client and Receiver] are online and ready.');

    receiverNode.sub('scheduled:task', (payload) => {
        console.log(`[Receiver] Received scheduled task with payload:`, payload);
    });

    function scheduleTask(topic, payload, delay) {
        console.log(`[Scheduler Client] Scheduling task to topic "${topic}" in ${delay / 1000} seconds.`);
        schedulerClientNode.emit('scheduler:add', {
            topic,
            payload,
            delay,
        });
    }

    scheduleTask('scheduled:task', { message: 'This is a scheduled message.' }, 5000); // 5 seconds
}

main();`

To run this example:

1. Start the scheduler service: `node scheduler-service.js`
2. Start the receiver and client: `node scheduler-client.js`
3. After 5 seconds, you will see the receiver log the message.