import fs from 'node:fs';
import dotenv from 'dotenv';
const envFile = '/home/awerkori/.Projects/project-nox-importer/.env';
// Fix password parsing by using real dotenv
const env = dotenv.parse(fs.readFileSync(envFile));
process.env = { ...process.env, ...env };

import { WorkAffinityScheduler } from '../build/core/scheduler/work-affinity-scheduler.js';
import { SchedulerStateStore } from '../build/core/scheduler/state-store.js';
import { ProtectiveSentinel } from '../build/core/protective-sentinel.js';
import { AdmissionController } from '../build/core/scheduler/admission-controller.js';
import pg from 'pg';

const pool = new pg.Pool({
  host: env.YUGABYTE_HOST, port: Number(env.YUGABYTE_PORT || 5433), user: env.YUGABYTE_USER,
  password: env.YUGABYTE_PASSWORD, database: env.YUGABYTE_DATABASE,
  ssl: {rejectUnauthorized:true,ca:fs.readFileSync(env.YUGABYTE_SSL_CERT)}, max: 2
});

const mockLogger = { debug: console.log, info: console.log, warn: console.log, error: console.error };
const stateStore = new SchedulerStateStore(pool, mockLogger);
const protectiveSentinel = new ProtectiveSentinel(pool, mockLogger);
const admissionController = new AdmissionController(pool, stateStore, protectiveSentinel, mockLogger);
const scheduler = new WorkAffinityScheduler(pool, stateStore, protectiveSentinel, admissionController, mockLogger);

async function run() {
  await stateStore.refreshActiveWorks();
  console.log("Active works:", stateStore.getActiveWorks().length);
  const t0 = performance.now();
  const job = await scheduler.acquireNextChapterJob();
  const t1 = performance.now();
  console.log("Job:", job?.id);
  console.log("Total acquireNextChapterJob MS:", t1 - t0);
  pool.end();
}
run().catch(console.error);
