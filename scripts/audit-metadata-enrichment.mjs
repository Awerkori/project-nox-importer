// Read-only production audit for the bounded aliases/tags metadata replays.
// Usage: NOX_AUDIT_ENV_FILE=/path/to/.env node scripts/audit-metadata-enrichment.mjs
// It deliberately never contacts upstream sources or mutates task state.
import fs from 'node:fs';
import pg from 'pg';
import dotenv from 'dotenv';

const envFile = process.env.NOX_AUDIT_ENV_FILE;
if (!envFile) throw new Error('NOX_AUDIT_ENV_FILE is required');
const env = dotenv.parse(fs.readFileSync(envFile));
const pool = new pg.Pool({
  host: env.YUGABYTE_HOST,
  port: Number(env.YUGABYTE_PORT || 5433),
  user: env.YUGABYTE_USER,
  password: env.YUGABYTE_PASSWORD,
  database: env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: true, ca: fs.readFileSync(env.YUGABYTE_SSL_CERT) },
  max: 1,
  connectionTimeoutMillis: 5000,
  query_timeout: 22000,
  application_name: 'nox-metadata-enrichment-audit',
  // One bounded, read-only scan of the confirmed manifest may be larger than
  // the cheap coverage/task counters. Keep a single connection and a hard cap.
  options: '-c statement_timeout=20000 -c default_transaction_read_only=on',
});

const metadataEvidence = `
  EXISTS (
    SELECT 1 FROM jsonb_each(m.metadata) entry(key,value)
    WHERE entry.key = ANY(ARRAY[
      'alternativeTitles','alternative_titles','aliases','alternativeNames','alternative_names',
      'originalTitle','original_title','nativeTitle','native_title','englishTitle','english_title',
      'romanizedTitle','romanized_title','genres','tags','categories'
    ]::text[])
    AND CASE jsonb_typeof(entry.value)
      WHEN 'array' THEN jsonb_array_length(entry.value) > 0
      WHEN 'string' THEN COALESCE(NULLIF(lower(btrim(entry.value #>> '{}')),''),'') NOT IN ('[]','{}','null')
      WHEN 'object' THEN entry.value <> '{}'::jsonb
      ELSE false
    END
  )`;

const aliasEvidence = `
  EXISTS (
    SELECT 1 FROM jsonb_each(m.metadata) entry(key,value)
    WHERE entry.key = ANY(ARRAY[
      'alternativeTitles','alternative_titles','aliases','alternativeNames','alternative_names',
      'originalTitle','original_title','nativeTitle','native_title','englishTitle','english_title',
      'romanizedTitle','romanized_title'
    ]::text[])
    AND CASE jsonb_typeof(entry.value)
      WHEN 'array' THEN jsonb_array_length(entry.value) > 0
      WHEN 'string' THEN COALESCE(NULLIF(lower(btrim(entry.value #>> '{}')),''),'') NOT IN ('[]','{}','null')
      ELSE false
    END
  )`;

async function one(sql) {
  return (await pool.query(sql)).rows[0] || {};
}

try {
  const catalog = await one(`
    SELECT count(*)::int AS total_works,
      count(*) FILTER (WHERE COALESCE(array_length(aliases, 1), 0) > 0)::int AS works_with_aliases,
      count(*) FILTER (WHERE COALESCE(array_length(aliases, 1), 0) = 0)::int AS works_without_aliases
    FROM works WHERE published=true`);

  const mappings = await one(`
    WITH evidence AS (
      SELECT m.source,m.source_work_id,m.work_id, ${metadataEvidence} AS metadata_evidence,
        ${aliasEvidence} AS alias_evidence,
        EXISTS (
          SELECT 1 FROM jsonb_each(m.metadata) f(key,value)
          CROSS JOIN LATERAL jsonb_array_elements(
            CASE WHEN jsonb_typeof(f.value)='array' THEN f.value ELSE '[]'::jsonb END
          ) tag(value)
          WHERE f.key=ANY(ARRAY['genres','tags','categories']::text[])
            AND jsonb_typeof(tag.value)='object'
            AND NULLIF(btrim(tag.value->>'nome'),'') IS NOT NULL
        ) AS legacy_pt_tags
      FROM importer_work_mappings m
      JOIN works w ON w.id=m.work_id
      WHERE w.published=true AND m.sync_status IN ('WAITING_ADMISSION','SYNCED','ACTIVE')
    )
    SELECT count(*) FILTER (WHERE metadata_evidence)::int AS metadata_eligible,
      count(*) FILTER (WHERE alias_evidence)::int AS alias_evidence_mappings,
      count(*) FILTER (WHERE alias_evidence AND COALESCE(array_length(w.aliases,1),0)=0)::int AS alias_evidence_missing_on_work,
      count(*) FILTER (WHERE legacy_pt_tags)::int AS legacy_portuguese_tag_mappings,
      count(*) FILTER (WHERE metadata_evidence AND NOT EXISTS (
        SELECT 1 FROM importer_lite_tasks t
        WHERE t.idempotency_key='lite:metadata:v' || CASE WHEN evidence.legacy_pt_tags THEN '3' ELSE '2' END
          || ':' || evidence.source || ':' || evidence.source_work_id
      ))::int AS eligible_not_materialized,
      count(*) FILTER (WHERE metadata_evidence AND EXISTS (
        SELECT 1 FROM importer_lite_tasks t
        WHERE t.idempotency_key='lite:metadata:v' || CASE WHEN evidence.legacy_pt_tags THEN '3' ELSE '2' END
          || ':' || evidence.source || ':' || evidence.source_work_id
          AND t.state IN ('READY','RUNNING','RETRY_WAIT')
      ))::int AS eligible_in_flight
    FROM evidence JOIN works w ON w.id=evidence.work_id`);

  const replay = await pool.query(`
    SELECT state,count(*)::int AS count,min(created_at) AS oldest
    FROM importer_lite_tasks
    WHERE task_type='CATALOG' AND payload->>'metadataEnrichmentVersion' IN ('2','3')
    GROUP BY payload->>'metadataEnrichmentVersion',state
    ORDER BY payload->>'metadataEnrichmentVersion',state`);

  const tags = await one(`
    SELECT count(DISTINCT w.id)::int AS published_works,
      count(DISTINCT wt.work_id)::int AS works_with_tags,
      count(DISTINCT w.id) FILTER (WHERE wt.work_id IS NULL)::int AS works_without_tags,
      count(DISTINCT wt.work_id) FILTER (WHERE t.kind IS NOT NULL)::int AS works_with_classified_tags,
      count(DISTINCT wt.work_id) FILTER (WHERE lower(t.name) IN ('milf','bdsm','ntr','netorare','netorase','netori','vanilla','yuri','yaoi','harem','anal'))::int AS works_with_specific_adult_tags
    FROM works w
    LEFT JOIN work_tags wt ON wt.work_id=w.id
    LEFT JOIN tags t ON t.id=wt.tag_id
    WHERE w.published=true`);

  const gaps = await pool.query(`
    SELECT state,CASE
      WHEN reason LIKE 'UPLOAD_OUTCOME_UNKNOWN%' THEN 'UPLOAD_OUTCOME_UNKNOWN'
      WHEN reason='SOURCE_RATE_LIMITED' THEN 'SOURCE_RATE_LIMITED'
      WHEN reason LIKE 'SOURCE_NOT_READY:%' THEN 'SOURCE_NOT_READY'
      WHEN reason LIKE 'MANIFEST%' OR reason LIKE '%MANIFEST%' THEN 'MANIFEST'
      WHEN reason='WORK_PRESENTATION_INCOMPLETE' THEN 'WORK_PRESENTATION_INCOMPLETE'
      ELSE COALESCE(NULLIF(split_part(reason,':',1),''),'(empty)') END AS reason_class,count(*)::int AS count
    FROM importer_lite_tasks
    WHERE task_type='IMPORT_CHAPTER' AND state IN ('RETRY_WAIT','BLOCKED')
    GROUP BY state,reason_class ORDER BY state,reason_class`);

  const sourceMetadata = await one(`WITH eligible AS (
      SELECT m.source,m.source_work_id,m.work_id,status.aliases_state,status.tags_state
      FROM importer_work_mappings m JOIN works w ON w.id=m.work_id
      LEFT JOIN importer_lite_metadata_enrichment status
        ON status.source=m.source AND status.source_work_id=m.source_work_id
      WHERE w.published=true AND m.sync_status IN ('WAITING_ADMISSION','SYNCED','ACTIVE')
    ) SELECT count(*)::int AS mappings,
      count(DISTINCT work_id)::int AS works,
      count(*) FILTER (WHERE aliases_state='ENRICHED')::int AS aliases_enriched,
      count(*) FILTER (WHERE aliases_state='NO_ALIASES')::int AS no_aliases,
      count(*) FILTER (WHERE aliases_state='EXTERNAL_BLOCKED')::int AS aliases_external_blocked,
      count(*) FILTER (WHERE aliases_state='PARSER_ERROR')::int AS aliases_parser_error,
      count(*) FILTER (WHERE aliases_state IS NULL OR aliases_state='PENDING')::int AS aliases_pending,
      count(*) FILTER (WHERE tags_state='ENRICHED')::int AS tags_enriched,
      count(*) FILTER (WHERE tags_state='NO_TAGS')::int AS no_tags,
      count(*) FILTER (WHERE tags_state='EXTERNAL_BLOCKED')::int AS tags_external_blocked,
      count(*) FILTER (WHERE tags_state='PARSER_ERROR')::int AS tags_parser_error,
      count(*) FILTER (WHERE tags_state IS NULL OR tags_state='PENDING')::int AS tags_pending
    FROM eligible`);

  const sourceMetadataTasks = await pool.query(`SELECT state,count(*)::int AS count
    FROM importer_lite_tasks WHERE task_type='CATALOG' AND payload ? 'metadataSourceRefresh'
    GROUP BY state ORDER BY state`);

  const manifestGaps = await one(`
    WITH confirmed AS (
      SELECT DISTINCT work_id,canonical_chapter_key
      FROM importer_lite_chapter_sources
      WHERE confirmed=true AND has_chapter=true
    ), missing AS (
      SELECT confirmed.*
      FROM confirmed
      WHERE NOT EXISTS (
        SELECT 1 FROM importer_lite_chapters chapter
        WHERE chapter.work_id=confirmed.work_id
          AND chapter.canonical_chapter_key=confirmed.canonical_chapter_key
      )
    )
    SELECT count(*)::int AS confirmed_unpublished,
      count(*) FILTER (WHERE NOT EXISTS (
        SELECT 1 FROM importer_lite_tasks task
        WHERE task.idempotency_key='lite:chapter:' || missing.work_id::text || ':' || missing.canonical_chapter_key
      ))::int AS not_materialized,
      count(*) FILTER (WHERE EXISTS (
        SELECT 1 FROM importer_lite_tasks task
        WHERE task.idempotency_key='lite:chapter:' || missing.work_id::text || ':' || missing.canonical_chapter_key
          AND task.state='READY'
      ))::int AS ready,
      count(*) FILTER (WHERE EXISTS (
        SELECT 1 FROM importer_lite_tasks task
        WHERE task.idempotency_key='lite:chapter:' || missing.work_id::text || ':' || missing.canonical_chapter_key
          AND task.state IN ('RETRY_WAIT','BLOCKED')
      ))::int AS delayed_or_blocked
    FROM missing`);

  console.log(JSON.stringify({
    at: new Date().toISOString(),
    catalog,
    mappings,
    replay: replay.rows,
    tags,
    sourceMetadata,
    sourceMetadataTasks: sourceMetadataTasks.rows,
    manifestGaps,
    gapRecovery: gaps.rows,
  }, null, 2));
} finally {
  await pool.end();
}
