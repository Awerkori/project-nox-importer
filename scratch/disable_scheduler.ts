import { Pool } from 'pg';
import * as dotenv from 'dotenv';
dotenv.config();

async function run() {
  const pool = new Pool({
    host: process.env.YUGABYTE_HOST,
    port: parseInt(process.env.YUGABYTE_PORT || '5433'),
    user: process.env.YUGABYTE_USER,
    password: process.env.YUGABYTE_PASSWORD,
    database: process.env.YUGABYTE_DATABASE,
    ssl: { rejectUnauthorized: false }
  });
  try {
    // 1. Update settings
    await pool.query(`
      INSERT INTO settings (key, value, updated_at) 
      VALUES ('work_affinity_scheduler_enabled', 'false', NOW())
      ON CONFLICT (key) DO UPDATE SET value = 'false', updated_at = NOW()
    `);
    
    // 2. Update importer_scheduler_state config
    const res = await pool.query(`SELECT value FROM importer_scheduler_state WHERE key = 'config'`);
    let config = { enabled: false, shadowMode: false, maxActiveWorks: 10, maxInflightPerWork: 2, p0TargetRatio: 0.7, backfillThreshold: 0.3 };
    if (res.rows.length > 0 && typeof res.rows[0].value === 'object') {
      config = { ...res.rows[0].value, enabled: false };
    }
    await pool.query(`
      INSERT INTO importer_scheduler_state (key, value, updated_at)
      VALUES ('config', $1::jsonb, NOW())
      ON CONFLICT (key) DO UPDATE SET value = $1::jsonb, updated_at = NOW()
    `, [JSON.stringify(config)]);
    
    console.log('Disabled WorkAffinityScheduler in DB.');
  } catch (err) {
    console.error(err);
  }
  await pool.end();
}
run();
