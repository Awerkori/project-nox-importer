import process from 'node:process';
import { PGlite } from '@electric-sql/pglite';
import { withTimeout } from './monitor-utils.mjs';
import { hasDbConfig, runInvalidSqlConnectionCloseTest } from './postgres-probe.mjs';

async function runLocal() {
  const db = new PGlite();
  let queryError;
  let closed = false;
  try {
    await withTimeout(db.waitReady, 2000, 'PGlite startup');
    try {
      await withTimeout(db.query('SELECT coluna_inexistente;'), 500, 'invalid SQL query');
    } catch (error) {
      queryError = error;
    }
  } finally {
    await withTimeout(db.close(), 1000, 'PGlite close').then(() => { closed = true; }).catch(() => {});
  }
  return { queryError: queryError ? String(queryError.message ?? queryError) : null, closed };
}

const result = process.argv.includes('--live') && hasDbConfig()
  ? await runInvalidSqlConnectionCloseTest()
  : await runLocal();

console.log(JSON.stringify({ ...result, intentionalSqlError: Boolean(result.queryError) }, null, 2));
// This command is a deterministic negative SQL test. A non-zero exit code is
// required when the database correctly rejects the invalid statement.
process.exitCode = result.queryError && result.closed ? 1 : 2;

