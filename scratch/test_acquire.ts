import { acquireJobsDirect } from '../src/db/yugabyte-direct';
import * as dotenv from 'dotenv';
dotenv.config();

async function run() {
  console.log('Testing acquireJobsDirect...');
  const t0 = Date.now();
  const jobs = await acquireJobsDirect({
    workerId: 'test-worker',
    taskType: 'IMPORT_CHAPTER',
    batchSize: 1
  });
  console.log(`Acquired ${jobs.length} jobs in ${Date.now() - t0}ms`);
  if (jobs.length > 0) {
    console.log(jobs[0]);
  }
}
run().then(() => process.exit(0)).catch(err => { console.error(err); process.exit(1); });
