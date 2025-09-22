// example.js
const Cortexium = require('./index');

async function main() {
    const calculatorNode = new Cortexium({
        prefix: 'my-app',
        url: 'redis://127.0.0.1:6379',
        type: 'calculator-service',
    });

    const apiNode = new Cortexium({
        prefix: 'my-app',
        url: 'redis://127.0.0.1:6379',
        type: 'api-gateway',
    });

    // The calculator service listens for "add" requests.
    calculatorNode.sub('add', (numbers) => {
        console.log('[Calculator] Received add request for:', numbers);
        if (!Array.isArray(numbers) || numbers.length !== 2) {
            throw new Error('Payload must be an array of two numbers.');
        }
        const result = numbers[0] + numbers[1];
        console.log(`[Calculator] Result is ${result}`);
        return result;
    });

    console.log('[API] Calculator service is setting up...');
    // Give a moment for the subscription to be established in Redis.
    await new Promise(resolve => setTimeout(resolve, 500));

    // Now, the API gateway can safely send its request.

    console.log('[API] Sending request to add [5, 10]');
    apiNode.emit('add', [5, 10], (err, result) => {
        if (err) {
            console.error('[API] Received error:', err.message);
            return;
        }
        console.log(`[API] SUCCESS! Received result from calculator: ${result}`);
    });


}

main();