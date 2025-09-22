// diagnostics.js
const { spawn } = require('child_process');
// *** THE FIX: Pass the desired interval (in ms) to the event-loop-lag function ***
const lag = require('event-loop-lag')(1000);

console.log('--- Starting Diagnostics ---');
console.log('This will monitor the event loop lag while the performance test runs.');
console.log('A high number (e.g., > 100ms) indicates a blocked event loop.\n');

// Start the performance test as a child process
const testProcess = spawn('node', ['performance-test.js'], { stdio: 'inherit' });

// Start monitoring the event loop lag of this diagnostics process
const interval = setInterval(() => {
    // The lag() function now returns the measured delay
    console.log(`Event Loop Lag: ${lag().toFixed(2)}ms`);
}, 1000);

testProcess.on('close', () => {
    console.log('\n--- Diagnostics Complete ---');
    clearInterval(interval);
});