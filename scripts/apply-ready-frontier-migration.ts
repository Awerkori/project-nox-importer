/**
 * Applies the ready-work canonical-frontier projection after a quiet-window
 * review. Native DIScloud deploys never execute migrations implicitly.
 *
 * The optional bounded re-projection touches only the currently elevated P0
 * ready-work rows, so an existing future P0 cannot keep an earlier P1
 * prerequisite ahead of unrelated work after the function is updated.
 *
 * Usage:
 *   npx tsx scripts/apply-ready-frontier-migration.ts --apply --confirm-ready-frontier [--reproject-ready-frontiers]
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import dotenv from 'dotenv';
import { getYugabytePool } from '../src/db/yugabyte-direct.js';

dotenv.config({
  path: process.env.NOX_ENV_FILE || '/home/awerkori/.Projects/project-nox-importer/.env',
  override: false,
});

const apply = process.argv.includes('--apply');
const confirmed = process.argv.includes('--confirm-ready-frontier');
const reproject = process.argv.includes('--reproject-ready-frontiers');
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const migrationPath = path.join(root, 'migrations/20261028043000_lite_ready_work_canonical_frontier.sql');

async function main(): Promise<void> {
  if (!apply || !confirmed) {
    throw new Error('Refusing ready-frontier migration without --apply --confirm-ready-frontier');
  }
  const sql = fs.readFileSync(migrationPath, 'utf8');
  const pool = getYugabytePool();
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(`SET LOCAL lock_timeout = '5s'`);
    await client.query(`SET LOCAL statement_timeout = '20s'`);
    await client.query(sql);
    const verify = await client.query<{ definition: string }>(
      `SELECT pg_get_functiondef('public.importer_lite_refresh_ready_work(text)'::regprocedure) AS definition`,
    );
    if (!verify.rows[0]?.definition.includes('ordered_frontier')) {
      throw new Error('Canonical-frontier ready-work function was not visible after update');
    }
    let reprojected = 0;
    if (reproject) {
      const result = await client.query<{ work_key: string }>(`WITH candidates AS (
          SELECT work_key FROM importer_lite_ready_works
          WHERE priority=1
          ORDER BY last_claimed_at NULLS FIRST,next_run_at,work_key
          LIMIT 64 FOR UPDATE SKIP LOCKED
        )
        SELECT work_key,public.importer_lite_refresh_ready_work(work_key) FROM candidates`);
      reprojected = result.rowCount || 0;
    }
    await client.query('COMMIT');
    console.log(JSON.stringify({ applied: true, function: 'importer_lite_refresh_ready_work', reprojected }));
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    throw error;
  } finally {
    client.release();
    await pool.end();
  }
}

main().catch((error) => {
  console.error(`Ready-frontier migration failed: ${error instanceof Error ? error.message : String(error)}`);
  process.exitCode = 1;
});
