import { getConfig } from './build/config.js';
const config = getConfig();
console.log('TESTED_CONCURRENCY_CEILING:', config.TESTED_CONCURRENCY_CEILING);
