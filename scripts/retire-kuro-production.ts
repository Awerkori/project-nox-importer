/**
 * One-time, auditable retirement of Kuro operational data.
 *
 * Usage:
 *   npx tsx scripts/retire-kuro-production.ts --dry-run
 *   npx tsx scripts/retire-kuro-production.ts --apply --confirm-retire-kuro
 *
 * The script never deletes canonical works, chapters, pages, media, aliases
 * or tags.  Every removed operational row is first snapshotted into
 * importer_retired_source_records inside the same transaction.
 */
import dotenv from 'dotenv';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { getYugabytePool } from '../src/db/yugabyte-direct.js';

dotenv.config({
  path: process.env.NOX_ENV_FILE || '/home/awerkori/.Projects/project-nox-importer/.env',
  override: false,
});

const SOURCES = ['kuro', 'kuromangas'];
const SOURCE = 'kuro';
const RETIREMENT_REASON = 'KURO_EXTERNAL_CLOUDFLARE_LOGIN_CHALLENGE';
const apply = process.argv.includes('--apply');
const confirmed = process.argv.includes('--confirm-retire-kuro');

console.log(JSON.stringify({ mode: apply ? 'apply-started' : 'dry-run-started', source: SOURCE }));

type Count = Record<string, number>;

async function count(client: { query: Function }, sql: string, params: unknown[] = []): Promise<number> {
  const result = await client.query(sql, params);
  return Number(result.rows[0]?.count || 0);
}

async function archiveAndDelete(
  client: { query: Function },
  runId: string,
  recordType: string,
  table: string,
  where: string,
  params: unknown[],
  originalId: string,
): Promise<number> {
  const archived = await client.query(
    `INSERT INTO public.importer_retired_source_records
       (source, record_type, original_id, retirement_run_id, record)
     SELECT $1, $2, ${originalId}, $3, to_jsonb(row)
       FROM public.${table} AS row
      WHERE ${where}
     ON CONFLICT (source, record_type, original_id) DO UPDATE
       SET retirement_run_id = EXCLUDED.retirement_run_id,
           retired_at = now(),
           record = EXCLUDED.record`,
    [SOURCE, recordType, runId, ...params],
  );
  const deleted = await client.query(`DELETE FROM public.${table} AS row WHERE ${where}`, params);
  if ((archived.rowCount ?? 0) !== (deleted.rowCount ?? 0)) {
    throw new Error(`Archive/delete mismatch for ${recordType}`);
  }
  return deleted.rowCount ?? 0;
}

async function inventory(client: { query: Function }) {
  const sourceParam = [SOURCES];
  const workPatterns = SOURCES.map((source) => `source:${source}:%`);
  // Keep source predicates indexed and execute probes sequentially: this is
  // deliberately light on YSQL and avoids parallel waiters during retirement.
  const workMappings = await count(client, `SELECT count(*)::int AS count FROM public.importer_work_mappings WHERE source = ANY($1::text[])`, sourceParam);
  const chapterMappings = await count(client, `SELECT count(*)::int AS count FROM public.importer_chapter_mappings WHERE source = ANY($1::text[])`, sourceParam);
  const legacyTasks = await count(client, `SELECT count(*)::int AS count FROM public.importer_queue WHERE source = ANY($1::text[])`, sourceParam);
  const liteTasks = await count(client, `SELECT count(*)::int AS count FROM public.importer_lite_tasks WHERE COALESCE(payload->>'source', '') = ANY($1::text[]) OR work_key LIKE ANY($2::text[])`, [SOURCES, workPatterns]);
  const metadata = await count(client, `SELECT count(*)::int AS count FROM public.importer_lite_metadata_enrichment WHERE source = ANY($1::text[])`, sourceParam);
  const liteRoutes = await count(client, `SELECT count(*)::int AS count FROM public.importer_lite_chapter_sources WHERE source = ANY($1::text[])`, sourceParam);
  const sourceHealth = await count(client, `SELECT count(*)::int AS count FROM public.importer_lite_source_health WHERE source = ANY($1::text[])`, sourceParam);
  const checkpoints = await count(client, `SELECT count(*)::int AS count FROM public.importer_checkpoints WHERE source = ANY($1::text[])`, sourceParam);
  const readyWorks = await count(client, `SELECT count(*)::int AS count FROM public.importer_lite_ready_works WHERE work_key LIKE ANY($1::text[])`, [workPatterns]);
  const manifests = await count(client, `SELECT count(*)::int AS count FROM public.importer_chapter_manifest WHERE selected_source = ANY($1::text[]) OR available_sources::text ~ '"(?:kuro|kuromangas)"'`, sourceParam);
  const runningLite = await count(client, `SELECT count(*)::int AS count FROM public.importer_lite_tasks WHERE state = 'RUNNING' AND (COALESCE(payload->>'source', '') = ANY($1::text[]) OR work_key LIKE ANY($2::text[]))`, [SOURCES, workPatterns]);
  const importingLegacy = await count(client, `SELECT count(*)::int AS count FROM public.importer_queue WHERE status = 'IMPORTING' AND source = ANY($1::text[])`, sourceParam);
  const dependentUploads = await count(client, `SELECT count(*)::int AS count FROM public.importer_lite_uploads AS upload JOIN public.importer_lite_tasks AS task ON task.id = upload.task_id WHERE COALESCE(task.payload->>'source', '') = ANY($1::text[]) OR task.work_key LIKE ANY($2::text[])`, [SOURCES, workPatterns]);
  const published = await client.query(
    `SELECT count(DISTINCT chapter.id)::int AS chapters,
            count(page.media_id)::int AS pages,
            count(*) FILTER (WHERE media.storage_ready IS TRUE)::int AS stored_pages,
            count(*) FILTER (WHERE media.storage_ready IS DISTINCT FROM TRUE)::int AS unavailable_pages
       FROM public.importer_chapter_mappings AS mapping
       JOIN public.chapters AS chapter ON chapter.id = mapping.chapter_id
       LEFT JOIN public.pages AS page ON page.chapter_id = chapter.id
       LEFT JOIN public.media AS media ON media.id = page.media_id
      WHERE mapping.source = ANY($1::text[])
        AND chapter.published_at IS NOT NULL`,
    sourceParam,
  );

  return {
    workMappings,
    chapterMappings,
    legacyTasks,
    liteTasks,
    metadata,
    liteRoutes,
    sourceHealth,
    checkpoints,
    readyWorks,
    manifests,
    runningLite,
    importingLegacy,
    dependentUploads,
    published: published.rows[0],
  };
}

async function main(): Promise<void> {
  if (apply && !confirmed) {
    throw new Error('Refusing destructive operation without --confirm-retire-kuro');
  }

  const pool = getYugabytePool();
  const client = await pool.connect();
  try {
    if (!apply) {
      console.log(JSON.stringify({ mode: 'dry-run', source: SOURCE, ...(await inventory(client)) }));
      return;
    }

    // Deploys do not execute SQL migrations. Apply the additive archive schema
    // before opening the retirement transaction. The migration itself is
    // idempotent and only disables Kuro; it does not delete operational rows.
    const migration = readFileSync(resolve('migrations/20261018010000_retire_kuro_operational_state.sql'), 'utf8');
    await client.query(migration);

    await client.query('BEGIN');
    try {
      await client.query('SELECT pg_advisory_xact_lock($1)', [7_301_018]);
      const before = await inventory(client);
      if (before.runningLite || before.importingLegacy) {
        throw new Error(`Refusing retirement while Kuro has active leases (lite=${before.runningLite}, legacy=${before.importingLegacy})`);
      }
      if (before.dependentUploads) {
        throw new Error(`Refusing retirement while ${before.dependentUploads} upload receipt(s) still depend on Kuro tasks`);
      }
      if (before.manifests) {
        throw new Error(`Refusing retirement while ${before.manifests} canonical manifest row(s) still select Kuro; resolve those routes explicitly first`);
      }

      const run = await client.query(
        `INSERT INTO public.importer_source_retirement_runs (source, reason)
         VALUES ($1, $2)
         RETURNING id`,
        [SOURCE, RETIREMENT_REASON],
      );
      const runId = String(run.rows[0].id);
      const workPatterns = SOURCES.map((source) => `source:${source}:%`);
      const counts: Count = {};

      counts.liteReadyWorks = await archiveAndDelete(
        client, runId, 'LITE_READY_WORK', 'importer_lite_ready_works',
        'row.work_key LIKE ANY($1::text[])', [workPatterns], 'row.work_key',
      );
      counts.liteTasks = await archiveAndDelete(
        client, runId, 'LITE_TASK', 'importer_lite_tasks',
        "COALESCE(row.payload->>'source', '') = ANY($1::text[]) OR row.work_key LIKE ANY($2::text[])",
        [SOURCES, workPatterns], 'row.id::text',
      );
      counts.liteChapterSources = await archiveAndDelete(
        client, runId, 'LITE_CHAPTER_SOURCE', 'importer_lite_chapter_sources',
        'row.source = ANY($1::text[])', [SOURCES], 'row.id::text',
      );
      counts.metadataEnrichment = await archiveAndDelete(
        client, runId, 'METADATA_ENRICHMENT', 'importer_lite_metadata_enrichment',
        'row.source = ANY($1::text[])', [SOURCES], "row.source || ':' || row.source_work_id",
      );
      counts.sourceHealth = await archiveAndDelete(
        client, runId, 'LITE_SOURCE_HEALTH', 'importer_lite_source_health',
        'row.source = ANY($1::text[])', [SOURCES], 'row.source',
      );
      counts.checkpoints = await archiveAndDelete(
        client, runId, 'CHECKPOINT', 'importer_checkpoints',
        'row.source = ANY($1::text[])', [SOURCES], 'row.id::text',
      );
      counts.legacyTasks = await archiveAndDelete(
        client, runId, 'LEGACY_QUEUE_TASK', 'importer_queue',
        'row.source = ANY($1::text[])', [SOURCES], 'row.id::text',
      );
      counts.chapterMappings = await archiveAndDelete(
        client, runId, 'CHAPTER_MAPPING', 'importer_chapter_mappings',
        'row.source = ANY($1::text[])', [SOURCES], 'row.id::text',
      );
      counts.workMappings = await archiveAndDelete(
        client, runId, 'WORK_MAPPING', 'importer_work_mappings',
        'row.source = ANY($1::text[])', [SOURCES], 'row.id::text',
      );

      const cursor = await client.query(
        `UPDATE public.importer_lite_metadata_source_cursor AS row
            SET last_source = NULL, updated_at = now()
          WHERE row.last_source = ANY($1::text[])
        RETURNING to_jsonb(row) AS record, row.cursor_name`,
        [SOURCES],
      );
      for (const row of cursor.rows) {
        await client.query(
          `INSERT INTO public.importer_retired_source_records
             (source, record_type, original_id, retirement_run_id, record)
           VALUES ($1, 'METADATA_SOURCE_CURSOR', $2, $3, $4)
           ON CONFLICT (source, record_type, original_id) DO UPDATE
             SET retirement_run_id = EXCLUDED.retirement_run_id, retired_at = now(), record = EXCLUDED.record`,
          [SOURCE, row.cursor_name, runId, row.record],
        );
      }
      counts.metadataCursorsReset = cursor.rowCount ?? 0;

      await client.query(
        `UPDATE public.importer_sources
            SET enabled = false,
                status = 'DISABLED',
                chapter_ingestion_enabled = false,
                catalog_discovery_enabled = false,
                cooldown_until = NULL,
                blocked_reason = 'Retired: recurring Cloudflare authentication challenge has no sustainable importer route',
                blocked_details = jsonb_build_object('sourceRetirement', '2026-10-18', 'reason', $1),
                config = COALESCE(config, '{}'::jsonb) || jsonb_build_object('sourceRetirement', '2026-10-18', 'sourceRetirementReason', $1),
                updated_at = now()
          WHERE id = 'kuro'`,
        [RETIREMENT_REASON],
      );

      const after = await inventory(client);
      const remaining = after.workMappings + after.chapterMappings + after.legacyTasks + after.liteTasks
        + after.metadata + after.liteRoutes + after.sourceHealth + after.checkpoints + after.readyWorks;
      if (remaining !== 0) {
        throw new Error(`Kuro retirement verification failed; ${remaining} operational row(s) remain`);
      }

      const publishedAfter = await client.query(
        `SELECT count(DISTINCT chapter.id)::int AS chapters,
                count(page.media_id)::int AS pages,
                count(*) FILTER (WHERE media.storage_ready IS TRUE)::int AS stored_pages,
                count(*) FILTER (WHERE media.storage_ready IS DISTINCT FROM TRUE)::int AS unavailable_pages
           FROM public.importer_retired_source_records AS retired
           JOIN public.chapters AS chapter
             ON chapter.id = NULLIF(retired.record->>'chapter_id', '')::uuid
           LEFT JOIN public.pages AS page ON page.chapter_id = chapter.id
           LEFT JOIN public.media AS media ON media.id = page.media_id
          WHERE retired.retirement_run_id = $1
            AND retired.record_type = 'CHAPTER_MAPPING'
            AND chapter.published_at IS NOT NULL`,
        [runId],
      );
      const beforePublished = before.published;
      const afterPublished = publishedAfter.rows[0];
      if (JSON.stringify(beforePublished) !== JSON.stringify(afterPublished)) {
        throw new Error('Published Kuro chapter/page preservation verification failed');
      }

      await client.query(
        `UPDATE public.importer_source_retirement_runs
            SET completed_at = now(), counts = $2::jsonb, updated_at = now()
          WHERE id = $1`,
        [runId, JSON.stringify({ ...counts, publishedBefore: beforePublished, publishedAfter: afterPublished })],
      );
      await client.query('COMMIT');
      console.log(JSON.stringify({ mode: 'applied', source: SOURCE, runId, counts, published: afterPublished }));
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    }
  } finally {
    client.release();
    await pool.end();
  }
}

try {
  await main();
} catch (error) {
  console.error(`Kuro retirement failed: ${error instanceof Error ? error.message : String(error)}`);
  process.exitCode = 1;
}
