import pg from 'pg';
import dotenv from 'dotenv';
dotenv.config({ path: '/home/awerkori/.Projects/project-nox-importer/.env' });

const { Client } = pg;
const client = new Client({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false }
});

function percentile(arr, p) {
  if (arr.length === 0) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const idx = (sorted.length - 1) * p;
  const lower = Math.floor(idx);
  const upper = Math.ceil(idx);
  const weight = idx - lower;
  return Math.round((sorted[lower] * (1 - weight) + sorted[upper] * weight) * 10) / 10;
}

async function run() {
  await client.connect();

  console.log('Sampling worker occupancy for 30s (1 sample/sec)...');
  const samples = [];
  const TOTAL_WORKERS = 18;

  for (let i = 0; i < 30; i++) {
    const res = await client.query("SELECT count(*)::int as busy FROM importer_queue WHERE status = 'IMPORTING'");
    const busy = res.rows[0].busy;
    const idle = Math.max(0, TOTAL_WORKERS - busy);
    samples.push({ busy, idle });
    await new Promise(r => setTimeout(r, 1000));
  }

  const busyArr = samples.map(s => s.busy);
  const idleArr = samples.map(s => s.idle);

  const busyP50 = percentile(busyArr, 0.50);
  const busyP95 = percentile(busyArr, 0.95);
  const idleP50 = percentile(idleArr, 0.50);
  const idleP95 = percentile(idleArr, 0.95);

  console.log('\n--- WORKER OCCUPANCY RESULTS ---');
  console.log('Busy samples:', busyArr);
  console.log(`WORKERS CONFIGURED: ${TOTAL_WORKERS}`);
  console.log(`WORKERS BUSY P50: ${busyP50}`);
  console.log(`WORKERS BUSY P95: ${busyP95}`);
  console.log(`WORKERS IDLE P50: ${idleP50}`);
  console.log(`WORKERS IDLE P95: ${idleP95}`);

  await client.end();
}

run().catch(console.error);
