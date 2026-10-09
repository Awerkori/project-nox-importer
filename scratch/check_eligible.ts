import { Pool } from 'pg';
import * as dotenv from 'dotenv';
import { isSourceExecutionEligible } from './src/core/source-eligibility.js';

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

  const { rows: srcs } = await pool.query(
    'SELECT id, enabled, chapter_ingestion_enabled, status, cooldown_until, blocked_reason, blocked_details FROM importer_sources WHERE chapter_ingestion_enabled = true'
  );

  const activeIds = srcs
    .filter((s: any) => isSourceExecutionEligible({
      status: s.status,
      enabled: s.enabled,
      chapterIngestionEnabled: s.chapter_ingestion_enabled,
      cooldownUntil: s.cooldown_until ? new Date(s.cooldown_until).getTime() : null,
      blockedReason: s.blocked_reason,
      blockedDetails: s.blocked_details,
    }))
    .map((s: any) => s.id);

  console.log(`Eligible count: ${activeIds.length}`);
  console.log(activeIds);
  await pool.end();
}
run();
