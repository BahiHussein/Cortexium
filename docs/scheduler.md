# Cortexium Services

This directory contains services that can be loaded into a Cortexium node to provide additional functionality.

## Scheduler Service

The `scheduler.js` service allows you to schedule tasks to be executed at a later time. It works by listening for new tasks on a specific topic, adding them to a Redis sorted set, and then polling that set for tasks that are due.

### How it Works

1. **Scheduling a Task:** To schedule a task, you `emit` a message to the `scheduler:add` topic. The payload of this message should be an object with the following properties:
    - `topic`: The topic to which the task should be emitted when it's due.
    - `payload`: The payload of the task.
    - `delay`: The delay in milliseconds before the task should be executed.
2. **Execution:** The scheduler service periodically checks for due tasks and, when a task is due, it `emit`s the task's payload to the specified topic.

### Usage

First, you need to have a Cortexium node running with the `SchedulerModule` loaded.

**`scheduler-service.js`**

JavaScript

`const Cortexium = require('cortexium');
const { Scheduler } = Cortexium.services;

async function startSchedulerService() {
    const schedulerNode = new Cortexium({
        prefix: 'my-app',
        url: 'redis://127.0.0.1:6379',
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
        url: 'redis://127.0.0.1:6379',
        type: 'scheduler-client-service',
    });

    const receiverNode = new Cortexium({
        prefix: 'my-app',
        url: 'redis://127.0.0.1:6379',
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