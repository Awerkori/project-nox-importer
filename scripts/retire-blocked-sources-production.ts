/**
 * One-time, auditable retirement for the sources that permanently failed the
 * 2026-10-18 production admission gate.
 *
 * Usage:
 *   npx tsx scripts/retire-blocked-sources-production.ts --dry-run
 *   npx tsx scripts/retire-blocked-sources-production.ts --apply --confirm-retire-blocked-sources
 *
 * It never deletes canonical works, chapters, pages, media, aliases, tags or
 * editorial data. Every operational record is archived in the same source
 * transaction before deletion. A source with active work, upload receipts,
 * manifest routing, or non-stored published pages refuses to retire.
 */
import dotenv from 'dotenv';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { getYugabytePool } from '../src/db/yugabyte-direct.js';

dotenv.config({
  path: process.env.NOX_ENV_FILE || '/home/awerkori/.Projects/project-nox-importer/.env',
  override: false,
});

const RETIREMENTS = [
  { source: 'acervohentai', ids: ['acervohentai'], reason: 'AUDIT_2026_10_JS_CHALLENGE' },
  { source: 'amuy', ids: ['amuy'], reason: 'AUDIT_2026_10_JS_CHALLENGE' },
  { source: 'arthurscan', ids: ['arthurscan'], reason: 'AUDIT_2026_10_JS_CHALLENGE' },
  { source: 'geasscomics', ids: ['geasscomics'], reason: 'AUDIT_2026_10_JS_CHALLENGE' },
  { source: 'inkapk', ids: ['inkapk'], reason: 'AUDIT_2026_10_JS_CHALLENGE' },
  { source: 'mangaonline', ids: ['mangaonline'], reason: 'AUDIT_2026_10_JS_CHALLENGE' },
  { source: 'mangastop', ids: ['mangastop'], reason: 'AUDIT_2026_10_JS_CHALLENGE' },
  {
    source: 'pointzerotoons',
    ids: ['pointzerotoons', 'pointzero', 'point_zero_toons', 'kitsuneyako'],
    reason: 'AUDIT_2026_10_JS_CHALLENGE',
  },
  { source: 'vegitoons', ids: ['vegitoons'], reason: 'AUDIT_2026_10_API_BLOCK' },
  { source: 'yaoifanclub', ids: ['yaoifanclub'], reason: 'AUDIT_2026_10_JS_CHALLENGE' },
  { source: 'yuriverso', ids: ['yuriverso'], reason: 'AUDIT_2026_10_JS_CHALLENGE' },
] as const;

const apply = process.argv.includes('--apply');
const confirmed = process.argv.includes('--confirm-retire-blocked-sources');

type Queryable = { query: (sql: string, params?: unknown[]) => Promise<any> };
type PublishedTotals = { chapters: number; pages: number; storedPages: number; unavailablePages: number };
type PublicationSnapshot = { totals: PublishedTotals; chapterIds: string[] };
type Retirement = (typeof RETIREMENTS)[number];
type Counts = Record<string, number>;

async function count(client: Queryable, sql: string, params: unknown[] = []): Promise<number> {
  const result = await client.query(sql, params);
  return Number(result.rows[0]?.count || 0);
}

async function archiveAndDelete(
  client: Queryable,
  source: string,
  runId: string,
  recordType: string,
  table: string,
  where: string,
  params: unknown[],
  originalId: string,
): Promise<number> {
  const archiveWhere = where.replace(/\$(\d+)/g, (_match, index) => `$${Number(index) + 3}`);
  const archived = await client.query(
    `INSERT INTO public.importer_retired_source_records
       (source, record_type, original_id, retirement_run_id, record)
     SELECT $1::text, $2::text, ${originalId}, $3::uuid, to_jsonb(row)
       FROM public.${table} AS row
      WHERE ${archiveWhere}
     ON CONFLICT (source, record_type, original_id) DO UPDATE
       SET retirement_run_id = EXCLUDED.retirement_run_id,
           retired_at = now(),
           record = EXCLUDED.record`,
    [source, recordType, runId, ...params],
  );
  const deleted = await client.query(`DELETE FROM public.${table} AS row WHERE ${where}`, params);
  if ((archived.rowCount ?? 0) !== (deleted.rowCount ?? 0)) {
    throw new Error(`Archive/delete mismatch for ${source}/${recordType}`);
  }
  return deleted.rowCount ?? 0;
}

async function publishedSnapshot(
  client: Queryable,
  ids: readonly string[],
  knownChapterIds?: string[],
): Promise<PublicationSnapshot> {
  const candidateIds = knownChapterIds ?? [...new Set((await client.query(
    `SELECT chapter_id
       FROM public.importer_chapter_mappings
      WHERE source = ANY($1::text[])
        AND chapter_id IS NOT NULL`,
    [ids],
  )).rows.map((row: { chapter_id: string }) => row.chapter_id))];

  if (candidateIds.length === 0) {
    return { totals: { chapters: 0, pages: 0, storedPages: 0, unavailablePages: 0 }, chapterIds: [] };
  }

  const chapters = await client.query(
    `SELECT id FROM public.chapters WHERE id = ANY($1::uuid[]) AND published_at IS NOT NULL`,
    [candidateIds],
  );
  const chapterIds = chapters.rows.map((row: { id: string }) => row.id);
  const totals: PublishedTotals = { chapters: chapterIds.length, pages: 0, storedPages: 0, unavailablePages: 0 };

  for (let offset = 0; offset < chapterIds.length; offset += 50) {
    const pages = await client.query(
      `SELECT count(page.media_id)::int AS pages,
              count(*) FILTER (WHERE media.storage_ready IS TRUE)::int AS stored_pages,
              count(*) FILTER (WHERE media.storage_ready IS DISTINCT FROM TRUE)::int AS unavailable_pages
         FROM public.pages AS page
         LEFT JOIN public.media AS media ON media.id = page.media_id
        WHERE page.chapter_id = ANY($1::uuid[])`,
      [chapterIds.slice(offset, offset + 50)],
    );
    totals.pages += Number(pages.rows[0]?.pages || 0);
    totals.storedPages += Number(pages.rows[0]?.stored_pages || 0);
    totals.unavailablePages += Number(pages.rows[0]?.unavailable_pages || 0);
  }

  return { totals, chapterIds };
}

async function inventory(client: Queryable, retirement: Retirement) {
  const ids = [...retirement.ids];
  const workPatterns = ids.map((source) => `source:${source}:%`);
  const params = [ids];
  const published = await publishedSnapshot(client, ids);

  return {
    workMappings: await count(client, `SELECT count(*)::int AS count FROM public.importer_work_mappings WHERE source = ANY($1::text[])`, params),
    chapterMappings: await count(client, `SELECT count(*)::int AS count FROM public.importer_chapter_mappings WHERE source = ANY($1::text[])`, params),
    legacyTasks: await count(client, `SELECT count(*)::int AS count FROM public.importer_queue WHERE source = ANY($1::text[])`, params),
    liteTasks: await count(client, `SELECT count(*)::int AS count FROM public.importer_lite_tasks WHERE COALESCE(payload->>'source', '') = ANY($1::text[]) OR work_key LIKE ANY($2::text[])`, [ids, workPatterns]),
    metadata: await count(client, `SELECT count(*)::int AS count FROM public.importer_lite_metadata_enrichment WHERE source = ANY($1::text[])`, params),
    liteRoutes: await count(client, `SELECT count(*)::int AS count FROM public.importer_lite_chapter_sources WHERE source = ANY($1::text[])`, params),
    sourceHealth: await count(client, `SELECT count(*)::int AS count FROM public.importer_lite_source_health WHERE source = ANY($1::text[])`, params),
    checkpoints: await count(client, `SELECT count(*)::int AS count FROM public.importer_checkpoints WHERE source = ANY($1::text[])`, params),
    readyWorks: await count(client, `SELECT count(*)::int AS count FROM public.importer_lite_ready_works WHERE work_key LIKE ANY($1::text[])`, [workPatterns]),
    manifests: await count(client, `SELECT count(*)::int AS count FROM public.importer_chapter_manifest WHERE selected_source = ANY($1::text[]) OR available_sources ?| $1::text[]`, params),
    runningLite: await count(client, `SELECT count(*)::int AS count FROM public.importer_lite_tasks WHERE state = 'RUNNING' AND (COALESCE(payload->>'source', '') = ANY($1::text[]) OR work_key LIKE ANY($2::text[]))`, [ids, workPatterns]),
    importingLegacy: await count(client, `SELECT count(*)::int AS count FROM public.importer_queue WHERE status = 'IMPORTING' AND source = ANY($1::text[])`, params),
    dependentUploads: await count(client, `SELECT count(*)::int AS count FROM public.importer_lite_uploads AS upload JOIN public.importer_lite_tasks AS task ON task.id = upload.task_id WHERE COALESCE(task.payload->>'source', '') = ANY($1::text[]) OR task.work_key LIKE ANY($2::text[])`, [ids, workPatterns]),
    published,
  };
}

async function resetMetadataCursor(client: Queryable, source: string, runId: string, ids: readonly string[]): Promise<number> {
  const cursor = await client.query(
    `UPDATE public.importer_lite_metadata_source_cursor AS row
        SET last_source = NULL, updated_at = now()
      WHERE row.last_source = ANY($1::text[])
    RETURNING to_jsonb(row) AS record, row.cursor_name`,
    [ids],
  );
  for (const row of cursor.rows) {
    await client.query(
      `INSERT INTO public.importer_retired_source_records
         (source, record_type, original_id, retirement_run_id, record)
       VALUES ($1, 'METADATA_SOURCE_CURSOR', $2, $3, $4)
       ON CONFLICT (source, record_type, original_id) DO UPDATE
         SET retirement_run_id = EXCLUDED.retirement_run_id,
             retired_at = now(), record = EXCLUDED.record`,
      [source, row.cursor_name, runId, row.record],
    );
  }
  return cursor.rowCount ?? 0;
}

async function retireOne(client: Queryable, retirement: Retirement) {
  await client.query('BEGIN');
  try {
    await client.query('SELECT pg_advisory_xact_lock($1)', [7_301_019]);
    const prior = await client.query(
      `SELECT id, counts
         FROM public.importer_source_retirement_runs
        WHERE source = $1 AND reason = $2 AND completed_at IS NOT NULL
        ORDER BY completed_at DESC LIMIT 1`,
      [retirement.source, retirement.reason],
    );
    const before = await inventory(client, retirement);
    const operationalRows = before.workMappings + before.chapterMappings + before.legacyTasks + before.liteTasks
      + before.metadata + before.liteRoutes + before.sourceHealth + before.checkpoints + before.readyWorks;
    if (prior.rowCount && operationalRows === 0) {
      await client.query('COMMIT');
      return { source: retirement.source, mode: 'already-retired', runId: prior.rows[0].id, counts: prior.rows[0].counts };
    }
    if (before.runningLite || before.importingLegacy) {
      throw new Error(`Refusing ${retirement.source}: active leases (lite=${before.runningLite}, legacy=${before.importingLegacy})`);
    }
    if (before.dependentUploads) throw new Error(`Refusing ${retirement.source}: ${before.dependentUploads} upload receipt(s) depend on source tasks`);
    if (before.manifests) throw new Error(`Refusing ${retirement.source}: ${before.manifests} manifest route(s) still reference the source`);
    if (before.published.totals.unavailablePages) {
      throw new Error(`Refusing ${retirement.source}: ${before.published.totals.unavailablePages} public page(s) are not stored`);
    }

    const run = await client.query(
      `INSERT INTO public.importer_source_retirement_runs (source, reason) VALUES ($1, $2) RETURNING id`,
      [retirement.source, retirement.reason],
    );
    const runId = String(run.rows[0].id);
    const ids = [...retirement.ids];
    const workPatterns = ids.map((source) => `source:${source}:%`);
    const counts: Counts = {};

    counts.liteReadyWorks = await archiveAndDelete(client, retirement.source, runId, 'LITE_READY_WORK', 'importer_lite_ready_works', 'row.work_key LIKE ANY($1::text[])', [workPatterns], 'row.work_key');
    counts.liteTasks = await archiveAndDelete(client, retirement.source, runId, 'LITE_TASK', 'importer_lite_tasks', "COALESCE(row.payload->>'source', '') = ANY($1::text[]) OR row.work_key LIKE ANY($2::text[])", [ids, workPatterns], 'row.id::text');
    counts.liteChapterSources = await archiveAndDelete(client, retirement.source, runId, 'LITE_CHAPTER_SOURCE', 'importer_lite_chapter_sources', 'row.source = ANY($1::text[])', [ids], 'row.id::text');
    counts.metadataEnrichment = await archiveAndDelete(client, retirement.source, runId, 'METADATA_ENRICHMENT', 'importer_lite_metadata_enrichment', 'row.source = ANY($1::text[])', [ids], "row.source || ':' || row.source_work_id");
    counts.sourceHealth = await archiveAndDelete(client, retirement.source, runId, 'LITE_SOURCE_HEALTH', 'importer_lite_source_health', 'row.source = ANY($1::text[])', [ids], 'row.source');
    counts.checkpoints = await archiveAndDelete(client, retirement.source, runId, 'CHECKPOINT', 'importer_checkpoints', 'row.source = ANY($1::text[])', [ids], 'row.id::text');
    counts.legacyTasks = await archiveAndDelete(client, retirement.source, runId, 'LEGACY_QUEUE_TASK', 'importer_queue', 'row.source = ANY($1::text[])', [ids], 'row.id::text');
    counts.chapterMappings = await archiveAndDelete(client, retirement.source, runId, 'CHAPTER_MAPPING', 'importer_chapter_mappings', 'row.source = ANY($1::text[])', [ids], 'row.id::text');
    counts.workMappings = await archiveAndDelete(client, retirement.source, runId, 'WORK_MAPPING', 'importer_work_mappings', 'row.source = ANY($1::text[])', [ids], 'row.id::text');
    counts.metadataCursorsReset = await resetMetadataCursor(client, retirement.source, runId, ids);

    await client.query(
      `UPDATE public.importer_sources
          SET enabled = false, status = 'DISABLED',
              chapter_ingestion_enabled = false, catalog_discovery_enabled = false,
              cooldown_until = NULL,
              blocked_reason = 'Retired after production source audit: ' || $2::text,
              blocked_details = jsonb_build_object('sourceRetirement', '2026-10-18', 'reason', $2::text, 'audit', 'production-admission-gate'),
              config = COALESCE(config, '{}'::jsonb) || jsonb_build_object('sourceRetirement', '2026-10-18', 'sourceRetirementReason', $2::text),
              updated_at = now()
        WHERE id = ANY($1::text[])`,
      [ids, retirement.reason],
    );

    const after = await inventory(client, retirement);
    const remaining = after.workMappings + after.chapterMappings + after.legacyTasks + after.liteTasks
      + after.metadata + after.liteRoutes + after.sourceHealth + after.checkpoints + after.readyWorks;
    if (remaining !== 0) throw new Error(`${retirement.source} retirement verification failed; ${remaining} operational row(s) remain`);
    const afterPublished = (await publishedSnapshot(client, ids, before.published.chapterIds)).totals;
    if (JSON.stringify(before.published.totals) !== JSON.stringify(afterPublished)) {
      throw new Error(`Published content preservation verification failed for ${retirement.source}`);
    }
    await client.query(
      `UPDATE public.importer_source_retirement_runs
          SET completed_at = now(), counts = $2::jsonb, updated_at = now()
        WHERE id = $1`,
      [runId, JSON.stringify({ ...counts, publishedBefore: before.published.totals, publishedAfter: afterPublished })],
    );
    await client.query('COMMIT');
    return { source: retirement.source, mode: 'retired', runId, counts, published: afterPublished };
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  }
}

async function main(): Promise<void> {
  if (apply && !confirmed) throw new Error('Refusing destructive operation without --confirm-retire-blocked-sources');
  console.log(JSON.stringify({ mode: apply ? 'apply-started' : 'dry-run-started', sources: RETIREMENTS.map(({ source }) => source) }));
  const pool = getYugabytePool();
  const client = await pool.connect();
  try {
    if (!apply) {
      const sources = [];
      for (const retirement of RETIREMENTS) {
        const result = await inventory(client, retirement);
        const { chapterIds: _chapterIds, ...published } = result.published;
        sources.push({ source: retirement.source, reason: retirement.reason, ...result, published });
      }
      console.log(JSON.stringify({ mode: 'dry-run', sources }));
      return;
    }

    // The first migration supplies the generic archive tables. Deploys do not
    // execute migrations, so apply only these idempotent safety markers before
    // the audited transaction begins.
    await client.query(readFileSync(resolve('migrations/20261018010000_retire_kuro_operational_state.sql'), 'utf8'));
    await client.query(readFileSync(resolve('migrations/20261018090000_retire_persistently_blocked_sources.sql'), 'utf8'));
    const results = [];
    for (const retirement of RETIREMENTS) results.push(await retireOne(client, retirement));
    console.log(JSON.stringify({ mode: 'applied', results }));
  } finally {
    client.release();
    await pool.end();
  }
}

try {
  await main();
} catch (error) {
  console.error(`Blocked-source retirement failed: ${error instanceof Error ? error.message : String(error)}`);
  process.exitCode = 1;
}
