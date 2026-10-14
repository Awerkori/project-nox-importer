/**
 * Importer Lite staging/disposable migration verifier.
 *
 * This script deliberately has no fallback to the normal YSQL environment.
 * It refuses a production-looking database name and only applies the additive
 * Lite migration after two explicit environment confirmations.  Its default
 * mode is a read-only preflight.
 */
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import pg from 'pg';

const required = [
  'LITE_VALIDATION_TARGET', 'LITE_VALIDATION_YSQL_HOST', 'LITE_VALIDATION_YSQL_USER',
  'LITE_VALIDATION_YSQL_PASSWORD', 'LITE_VALIDATION_DATABASE',
];
const missing = required.filter((name) => !process.env[name]);
if (missing.length) throw new Error(`Missing isolated-validation environment: ${missing.join(', ')}`);

const target = String(process.env.LITE_VALIDATION_TARGET).toLowerCase();
const database = String(process.env.LITE_VALIDATION_DATABASE);
if (!['staging', 'disposable'].includes(target)) {
  throw new Error('LITE_VALIDATION_TARGET must be exactly staging or disposable');
}
if (/(^|[_-])(prod|production)([_-]|$)/i.test(database)) {
  throw new Error('Refusing a production-looking database name');
}

const apply = process.env.LITE_VALIDATION_APPLY === '1';
if (apply && process.env.LITE_VALIDATION_CONFIRM !== 'APPLY_LITE_TO_NON_PRODUCTION') {
  throw new Error('Applying requires LITE_VALIDATION_CONFIRM=APPLY_LITE_TO_NON_PRODUCTION');
}

const caPath = process.env.LITE_VALIDATION_YSQL_CA || path.resolve('certs/yugabyte-root.crt');
if (!fs.existsSync(caPath)) throw new Error(`YSQL CA certificate not found: ${caPath}`);
const migrationPaths = [
  'migrations/20261010090000_importer_lite_v2.sql',
  'migrations/20261014031500_lite_manifest_gap_repair_index.sql',
  'migrations/20261014090000_lite_claim_manifest_chapters_in_order.sql',
];
const migration = migrationPaths.map((migrationPath) => fs.readFileSync(path.resolve(migrationPath), 'utf8')).join('\n');
const migrationSha256 = crypto.createHash('sha256').update(migration).digest('hex');
const options = apply
  ? '-c statement_timeout=15000 -c idle_in_transaction_session_timeout=15000'
  : '-c default_transaction_read_only=on -c statement_timeout=10000';
const pool = new pg.Pool({
  host: process.env.LITE_VALIDATION_YSQL_HOST,
  port: Number(process.env.LITE_VALIDATION_YSQL_PORT || 5433),
  user: process.env.LITE_VALIDATION_YSQL_USER,
  password: process.env.LITE_VALIDATION_YSQL_PASSWORD,
  database,
  max: 1,
  connectionTimeoutMillis: 10_000,
  query_timeout: 15_000,
  ssl: { rejectUnauthorized: true, ca: fs.readFileSync(caPath, 'utf8') },
  options,
  application_name: 'project-nox-lite-staging-validation',
});

const legacyTables = ['works', 'chapters', 'pages', 'media', 'importer_sources', 'importer_staff_requests', 'importer_chapter_mappings', 'settings'];
const liteTables = ['importer_lite_tasks', 'importer_lite_ready_works', 'importer_lite_chapter_sources', 'importer_lite_source_health', 'importer_lite_chapters', 'importer_lite_uploads'];
const expectedIndexes = [
  'importer_lite_tasks_ready_work_idx', 'importer_lite_tasks_monitor_ready_idx', 'importer_lite_tasks_recovery_idx',
  'importer_lite_tasks_work_chapter_idx', 'importer_lite_tasks_blocked_work_idx', 'importer_lite_tasks_publish_recovery_idx',
  'importer_lite_ready_works_claim_idx', 'importer_lite_chapter_sources_select_idx', 'importer_lite_chapter_sources_order_idx',
  'importer_lite_chapter_sources_manifest_repair_idx',
  'importer_lite_chapters_order_idx', 'importer_lite_uploads_recovery_idx',
];

async function rows(sql, values = []) { return (await pool.query(sql, values)).rows; }

async function snapshot() {
  const counts = {};
  for (const table of legacyTables) {
    counts[table] = Number((await rows(`SELECT count(*)::int AS count FROM public.${table}`))[0].count);
  }
  return counts;
}

function assertSameCounts(before, after) {
  for (const table of legacyTables) {
    if (before[table] !== after[table]) throw new Error(`Additive migration changed legacy row count for ${table}`);
  }
}

try {
  const server = (await rows("SELECT current_database() AS database,current_user AS role,current_setting('transaction_read_only') AS read_only,version() AS version"))[0];
  if (server.database !== database) throw new Error('Connected database does not match LITE_VALIDATION_DATABASE');
  // Direct YSQL is intentionally supported without Supabase compatibility
  // roles.  The migration keeps objects private and grants those roles only
  // when they already exist; record their presence for the audit instead of
  // making a production-compatible target fail validation.
  const roles = await rows("SELECT rolname FROM pg_roles WHERE rolname IN ('anon','authenticated','service_role') ORDER BY rolname");
  const before = await snapshot();

  if (apply) {
    await pool.query(migration);
    assertSameCounts(before, await snapshot());
  }

  const tables = await rows(`SELECT relname FROM pg_class WHERE relnamespace='public'::regnamespace
    AND relname IN (${liteTables.map((_, index) => `$${index + 1}`).join(',')}) ORDER BY relname`, liteTables);
  const indexes = await rows(`SELECT indexname FROM pg_indexes WHERE schemaname='public'
    AND indexname IN (${expectedIndexes.map((_, index) => `$${index + 1}`).join(',')}) ORDER BY indexname`, expectedIndexes);
  const functions = await rows("SELECT proname FROM pg_proc WHERE pronamespace='public'::regnamespace AND proname IN ('importer_lite_refresh_ready_work','importer_lite_staff_priority_sync','importer_lite_claim_task','importer_lite_claim_monitor_task') ORDER BY proname");
  const trigger = await rows("SELECT tgname FROM pg_trigger WHERE tgrelid='public.importer_staff_requests'::regclass AND tgname='importer_lite_staff_priority_sync_trigger' AND NOT tgisinternal");
  if (apply && (tables.length !== liteTables.length || indexes.length !== expectedIndexes.length || functions.length !== 4 || trigger.length !== 1)) {
    throw new Error('Migration completed without every required Lite object');
  }

  console.log(JSON.stringify({
    target, applied: apply, database: server.database, role: server.role,
    readOnlySession: server.read_only, version: String(server.version).split(' ').slice(0, 2).join(' '),
    migrationSha256, optionalRoles: roles.map((row) => row.rolname), legacyCounts: before,
    liteTables: tables.map((row) => row.relname), liteIndexes: indexes.map((row) => row.indexname),
    liteFunctions: functions.map((row) => row.proname), staffTrigger: Boolean(trigger.length),
  }, null, 2));
} finally {
  await pool.end();
}
