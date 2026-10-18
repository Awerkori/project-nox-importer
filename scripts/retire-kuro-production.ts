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
type PublishedTotals = {
  chapters: number;
  pages: number;
  stored_pages: number;
  unavailable_pages: number;
};

type PublicationSnapshot = {
  totals: PublishedTotals;
  chapterIds: string[];
};

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
  // The archive INSERT reserves $1..$3 for its audit columns.  Shift the
  // caller's predicate placeholders so a source-array predicate cannot be
  // confused with the scalar source stored in the archive record.
  const archiveWhere = where.replace(/\$(\d+)/g, (_match, index) => `$${Number(index) + 3}`);
  const archived = await client.query(
    `INSERT INTO public.importer_retired_source_records
       (source, record_type, original_id, retirement_run_id, record)
     SELECT $1, $2, ${originalId}, $3, to_jsonb(row)
       FROM public.${table} AS row
      WHERE ${archiveWhere}
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

/**
 * Mappings are an operational layer, while published chapters and their
 * media are canonical public data.  Resolve the affected IDs first, then
 * examine pages through their primary-key prefix in small sequential batches.
 * This avoids a catalogue-wide join and keeps the preservation proof bounded.
 */
async function publishedSnapshot(
  client: { query: Function },
  knownChapterIds?: string[],
): Promise<PublicationSnapshot> {
  const candidateIds = knownChapterIds ?? [...new Set((await client.query(
    `SELECT chapter_id
       FROM public.importer_chapter_mappings
      WHERE source = ANY($1::text[])
        AND chapter_id IS NOT NULL`,
    [SOURCES],
  )).rows.map((row: { chapter_id: string }) => row.chapter_id))];

  if (candidateIds.length === 0) {
    return { totals: { chapters: 0, pages: 0, stored_pages: 0, unavailable_pages: 0 }, chapterIds: [] };
  }

  const chapters = await client.query(
    `SELECT id
       FROM public.chapters
      WHERE id = ANY($1::uuid[])
        AND published_at IS NOT NULL`,
    [candidateIds],
  );
  const chapterIds = chapters.rows.map((row: { id: string }) => row.id);
  const totals: PublishedTotals = {
    chapters: chapterIds.length,
    pages: 0,
    stored_pages: 0,
    unavailable_pages: 0,
  };

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
    totals.stored_pages += Number(pages.rows[0]?.stored_pages || 0);
    totals.unavailable_pages += Number(pages.rows[0]?.unavailable_pages || 0);
  }

  return { totals, chapterIds };
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
  // `?|` checks the JSONB array directly.  Casting it to text and applying a
  // regex made this guard scan slowly enough to time out on production,
  // defeating the fail-closed safety check.
  const manifests = await count(client, `SELECT count(*)::int AS count FROM public.importer_chapter_manifest WHERE selected_source = ANY($1::text[]) OR available_sources ?| $1::text[]`, sourceParam);
  const runningLite = await count(client, `SELECT count(*)::int AS count FROM public.importer_lite_tasks WHERE state = 'RUNNING' AND (COALESCE(payload->>'source', '') = ANY($1::text[]) OR work_key LIKE ANY($2::text[]))`, [SOURCES, workPatterns]);
  const importingLegacy = await count(client, `SELECT count(*)::int AS count FROM public.importer_queue WHERE status = 'IMPORTING' AND source = ANY($1::text[])`, sourceParam);
  const dependentUploads = await count(client, `SELECT count(*)::int AS count FROM public.importer_lite_uploads AS upload JOIN public.importer_lite_tasks AS task ON task.id = upload.task_id WHERE COALESCE(task.payload->>'source', '') = ANY($1::text[]) OR task.work_key LIKE ANY($2::text[])`, [SOURCES, workPatterns]);
  const published = await publishedSnapshot(client);

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
    published,
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
      const before = await inventory(client);
      const { chapterIds: _chapterIds, ...published } = before.published;
      console.log(JSON.stringify({ mode: 'dry-run', source: SOURCE, ...before, published }));
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

      const beforePublished = before.published.totals;
      const afterPublished = (await publishedSnapshot(client, before.published.chapterIds)).totals;
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
