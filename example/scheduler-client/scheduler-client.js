const Cortexium = require('../../index');
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

async function startReceiver() {
    const receiverNode = new Cortexium({
        prefix: 'my-app',
        url: 'nats://127.0.0.1:4222',
        type: 'receiver-service',
    });

    await receiverNode.ready();
    console.log('[Receiver] is online and ready.');

    await receiverNode.sub('scheduled:task', (payload) => {
        console.log(`[Receiver] Received scheduled task with payload:`, payload);
    });
}

async function startClient() {
    const clientNode = new Cortexium({
        prefix: 'my-app',
        url: 'nats://127.0.0.1:4222',
        type: 'scheduler-client',
    });

    await clientNode.ready();
    console.log('[Scheduler Client] is online and ready.');

    // Schedule a task to be emitted in 5 seconds
    setTimeout(() => {
        console.log('[Scheduler Client] Scheduling task for 5 seconds from now...');
        clientNode.emit('scheduler:add', {
            topic: 'scheduled:task',
            payload: { message: 'Hello from scheduled task!' },
            delay: 5000,
        });
    }, 1000);
}

// Run all three in one script for demo
startSchedulerService().catch(console.error);
startReceiver().catch(console.error);
startClient().catch(console.error);
