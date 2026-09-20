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

async function main() {
  await client.connect();
  console.log('Connected to YugabyteDB Aeon.');

  // 1. Move all RETRY jobs with attempts >= 7 to FAILED
  const exhaustedRes = await client.query(`
    UPDATE importer_queue
    SET status = 'FAILED',
        last_error = '[RETRY_BUDGET_EXHAUSTED] Max attempts (' || attempts || ') exceeded without resolution: ' || COALESCE(last_error, 'Unknown failure'),
        last_error_at = NOW(),
        retry_reason = 'RETRY_BUDGET_EXHAUSTED',
        locked_by = NULL,
        locked_at = NULL,
        lease_expires_at = NULL,
        next_run_at = NOW(),
        updated_at = NOW()
    WHERE status = 'RETRY' AND attempts >= 7
    RETURNING id, source, attempts, payload->>'chapterNumber' as ch;
  `);
  console.log(`[CLEANUP] Moved ${exhaustedRes.rowCount} jobs with attempts >= 7 to FAILED:`);
  for (const r of exhaustedRes.rows) {
    console.log(`  - [${r.source}] ch:${r.ch} (attempts: ${r.attempts})`);
  }

  // 2. Move permanent 404 unresolved jobs to FAILED
  const perm404Res = await client.query(`
    UPDATE importer_queue
    SET status = 'FAILED',
        last_error = '[IMAGE_404] ' || COALESCE(last_error, 'Permanent 404 unresolved'),
        last_error_at = NOW(),
        retry_reason = 'IMAGE_404',
        locked_by = NULL,
        locked_at = NULL,
        lease_expires_at = NULL,
        next_run_at = NOW(),
        updated_at = NOW()
    WHERE status = 'RETRY' AND last_error ILIKE '%PERMANENT_404_UNRESOLVED%'
    RETURNING id, source, attempts, payload->>'chapterNumber' as ch;
  `);
  console.log(`[CLEANUP] Moved ${perm404Res.rowCount} permanent 404 jobs to FAILED:`);
  for (const r of perm404Res.rows) {
    console.log(`  - [${r.source}] ch:${r.ch}`);
  }

  // 3. Move missing chapter 0 jobs to FAILED (EMPTY_PAGES)
  const ch0Res = await client.query(`
    UPDATE importer_queue
    SET status = 'FAILED',
        last_error = '[EMPTY_PAGES] Chapter 0 does not exist upstream: ' || COALESCE(last_error, '0 pages returned'),
        last_error_at = NOW(),
        retry_reason = 'EMPTY_PAGES',
        locked_by = NULL,
        locked_at = NULL,
        lease_expires_at = NULL,
        next_run_at = NOW(),
        updated_at = NOW()
    WHERE status = 'RETRY' AND payload->>'chapterNumber' = '0' AND (last_error ILIKE '%failed to return pages%' OR last_error ILIKE '%0 valid content pages%')
    RETURNING id, source, attempts, payload->>'chapterNumber' as ch;
  `);
  console.log(`[CLEANUP] Moved ${ch0Res.rowCount} missing chapter 0 jobs to FAILED:`);
  for (const r of ch0Res.rows) {
    console.log(`  - [${r.source}] ch:${r.ch}`);
  }

  // 4. Recover Hanami Heaven stale lease jobs (chapters 54-62) back to QUEUED
  const hhRes = await client.query(`
    UPDATE importer_queue
    SET status = 'QUEUED',
        attempts = 0,
        last_recovered_error = last_error,
        recovered_at = NOW(),
        retry_reason = 'LEASE_EXPIRED',
        last_error = NULL,
        locked_by = NULL,
        locked_at = NULL,
        lease_expires_at = NULL,
        next_run_at = NOW(),
        updated_at = NOW()
    WHERE status = 'RETRY' AND source = 'hanamiheaven' AND attempts < 7 AND last_error ILIKE '%Lease expired%'
    RETURNING id, payload->>'chapterNumber' as ch;
  `);
  console.log(`[CLEANUP] Recovered ${hhRes.rowCount} Hanami Heaven stale lease jobs back to QUEUED:`);
  for (const r of hhRes.rows) {
    console.log(`  - ch:${r.ch}`);
  }

  // 5. Restore expired COOLDOWN sources to ACTIVE
  const cooldownRes = await client.query(`
    UPDATE importer_sources
    SET status = 'ACTIVE',
        cooldown_until = NULL,
        blocked_reason = NULL,
        updated_at = NOW()
    WHERE status = 'COOLDOWN' AND (cooldown_until IS NULL OR cooldown_until <= NOW())
    RETURNING id, name;
  `);
  console.log(`[CLEANUP] Restored ${cooldownRes.rowCount} expired COOLDOWN sources to ACTIVE:`);
  for (const r of cooldownRes.rows) {
    console.log(`  - [${r.id}] ${r.name}`);
  }

  // 6. Summary of current queue status
  const queueSummary = await client.query(`
    SELECT status, count(*) FROM importer_queue GROUP BY status ORDER BY count DESC;
  `);
  console.log('\n=== CURRENT QUEUE STATUS ===');
  console.table(queueSummary.rows);

  const retryRemaining = await client.query(`
    SELECT source, count(*), max(attempts), min(attempts)
    FROM importer_queue
    WHERE status = 'RETRY'
    GROUP BY source
    ORDER BY count DESC;
  `);
  console.log('\n=== REMAINING RETRY JOBS ===');
  console.table(retryRemaining.rows);

  await client.end();
}

main().catch(err => {
  console.error('Error in cleanup script:', err);
  process.exit(1);
});
