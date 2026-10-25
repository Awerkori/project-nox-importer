/**
 * Applies the bounded, fair Lite claim function after a quiet-window review.
 *
 * Native DIScloud deploys deliberately never run migrations implicitly.
 * This update changes no task, work, chapter or metadata row; it only replaces
 * the claim function. The lock/statement budgets make a busy database fail
 * closed so the operator can retry in a clean window.
 *
 * Usage:
 *   npx tsx scripts/apply-bounded-lite-claim-migration.ts --apply --confirm-bounded-lite-claim
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
const confirmed = process.argv.includes('--confirm-bounded-lite-claim');
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const migrationPath = path.join(root, 'migrations/20261025063000_bound_claim_probe_and_metadata_pool.sql');

async function main(): Promise<void> {
  if (!apply || !confirmed) {
    throw new Error('Refusing function update without --apply --confirm-bounded-lite-claim');
  }
  const sql = fs.readFileSync(migrationPath, 'utf8');
  const pool = getYugabytePool();
  const client = await pool.connect();
  try {
    await client.query(`SET lock_timeout = '5s'`);
    await client.query(`SET statement_timeout = '15s'`);
    await client.query(sql);
    const verification = await client.query<{ definition: string }>(
      `SELECT pg_get_functiondef('public.importer_lite_claim_task(text,interval)'::regprocedure) AS definition`,
    );
    const definition = verification.rows[0]?.definition || '';
    if (!definition.includes('LIMIT 4') || !definition.includes('active_work.state = \'RUNNING\'')) {
      throw new Error('Bounded Lite claim function was not visible after update');
    }
    console.log(JSON.stringify({ applied: true, function: 'importer_lite_claim_task', probeLimit: 4 }));
  } finally {
    client.release();
    await pool.end();
  }
}

main().catch((error) => {
  console.error(`Bounded Lite claim migration failed: ${error instanceof Error ? error.message : String(error)}`);
  process.exitCode = 1;
});
