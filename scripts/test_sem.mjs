import { AsyncSemaphore } from '../build/core/concurrency.js';
const sem = new AsyncSemaphore(3, 'test');
console.log('Capacity:', sem.capacity);
console.log('Max:', sem.max);
console.log('Available:', sem.available);
