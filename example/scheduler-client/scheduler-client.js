// cortexium/example/scheduler-client.js
const Cortexium = require('../../index');

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
        console.log(`[Receiver] Task was scheduled at ${new Date(payload.scheduledAt).toLocaleTimeString()} and received at ${new Date().toLocaleTimeString()}`);
    });

    function scheduleTask(topic, payload, delay) {
        console.log(`[Scheduler Client] Scheduling task to topic "${topic}" in ${delay / 1000} seconds.`);
        schedulerClientNode.emit('scheduler:add', {
            topic,
            payload: { ...payload, scheduledAt: new Date(Date.now() + delay).toISOString() },
            delay,
        });
    }

    scheduleTask('scheduled:task', { message: 'This is a scheduled message.' }, 5000); // 5 seconds
}

main();