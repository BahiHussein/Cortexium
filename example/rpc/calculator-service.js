const Cortexium = require('../../index');

async function startCalculator() {
    const calculatorNode = new Cortexium({
        prefix: 'my-app',
        url: 'nats://127.0.0.1:4222',
        type: 'calculator-service',
    });

    await calculatorNode.ready();
    console.log(`[Calculator] Node ${calculatorNode.nodeId} is online and ready.`);

    // Subscribe to the 'add' topic for RPC.
    // Only ONE calculator-service instance will receive each request (load balanced).
    await calculatorNode.sub('add', (numbers, ctx) => {
        console.log(`[Calculator] Received add request for:`, numbers);
        const result = numbers[0] + numbers[1];
        return result; // The return value is sent back as the reply.
    });

    // Subscribe to 'multiply' topic
    await calculatorNode.sub('multiply', (numbers, ctx) => {
        console.log(`[Calculator] Received multiply request for:`, numbers);
        return numbers[0] * numbers[1];
    });
}

startCalculator().catch(console.error);
