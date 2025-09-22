// cortexium/example/load-services.js
const Cortexium = require('../../index');

// Destructure the Scheduler service from the Cortexium object
const { Scheduler } = Cortexium.services;

async function startSchedulerService() {
    const schedulerNode = new Cortexium({
        prefix: 'my-app',
        url: 'redis://127.0.0.1:6379',
        type: 'scheduler-service',
        services: [Scheduler] // Pass the service class to the constructor
    });

    await schedulerNode.ready();
    console.log('[Scheduler Service] is online and ready.');
}

startSchedulerService();