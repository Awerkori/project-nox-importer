import pg from 'pg';
import fs from 'fs';
import dotenv from 'dotenv';
import { WorkAffinityScheduler } from '../build/core/scheduler/work-affinity-scheduler.js';
import { SchedulerStateStore } from '../build/core/scheduler/state-store.js';
import { ProtectiveSentinel } from '../build/core/protective-sentinel.js';
import { AdmissionController } from '../build/core/scheduler/admission-controller.js';

const env = dotenv.parse(fs.readFileSync('/home/awerkori/.Projects/project-nox-importer/.env', 'utf8'));

const pool = new pg.Pool({
  host: env.YUGABYTE_HOST, port: Number(env.YUGABYTE_PORT || 5433), user: env.YUGABYTE_USER,
  password: env.YUGABYTE_PASSWORD, database: env.YUGABYTE_DATABASE,
  ssl: {rejectUnauthorized:true,ca:fs.readFileSync(env.YUGABYTE_SSL_CERT)}, max: 2
});

const mockLogger = { debug: console.log, info: console.log, warn: console.log, error: console.error };

async function main() {
  const stateStore = new SchedulerStateStore(pool, mockLogger);
  await stateStore.initialize();
  const sentinel = new ProtectiveSentinel(pool, mockLogger);
  const admissionController = new AdmissionController(stateStore, sentinel, pool, mockLogger);
  const scheduler = new WorkAffinityScheduler(stateStore, admissionController, sentinel, pool, mockLogger);

  console.log('Testing acquireNextChapterJob...');
  const t0 = performance.now();
  const job = await scheduler.acquireNextChapterJob({
    workerId: 'test-trace-worker',
    leaseDurationMinutes: 5
  });
  console.log('Claim took:', (performance.now() - t0).toFixed(1), 'ms');
  console.log('Claimed job:', job ? job.id : null);
  if (job) {
    await pool.query(`UPDATE importer_queue SET status = 'QUEUED', locked_by = NULL, locked_at = NULL WHERE id = $1`, [job.id]);
  }
  await pool.end();
}
main().catch(console.error);
