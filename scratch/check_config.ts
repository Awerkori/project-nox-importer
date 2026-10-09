import { SchedulerStateStore } from '../build/core/scheduler/state-store.js';
import * as dotenv from 'dotenv';
dotenv.config();
async function run() {
  const store = new SchedulerStateStore();
  await store.initialize();
  console.log(store.getConfig());
  process.exit(0);
}
run();
