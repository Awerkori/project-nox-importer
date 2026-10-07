import pg from 'pg';
import fs from 'fs';
import dotenv from 'dotenv';
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
  
  console.log('Testing specific admission...');
  const res = await admissionController.executeOnDemandAdmission('P1', ['manhastro'], 5, new Map(), () => 1);
  console.log('Result:', res);
  await pool.end();
}
main().catch(console.error);
