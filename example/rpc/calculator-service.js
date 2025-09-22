const Cortexium = require('../../index');

async function startCalculator() {
    const calculatorNode = new Cortexium({
        prefix: 'my-app',
        url: 'redis://127.0.0.1:6379',
        type: 'calculator-service',
    });

    await calculatorNode.ready();
    console.log('[Calculator] is online and ready.');

    // Subscribe to the 'add' topic.
    // The handler function will be executed when a message arrives.
    calculatorNode.sub('add', (numbers) => {
        console.log(`[Calculator] Received add request for:`, numbers);
        const result = numbers[0] + numbers[1];
        return result; // The return value is sent back as the reply.
    });
}

startCalculator();
