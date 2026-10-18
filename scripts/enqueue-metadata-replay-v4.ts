import { getYugabytePool } from '../src/db/yugabyte-direct.js';
import { LiteRepository } from '../src/lite/repository.js';
import { STORED_MAPPING_ALIAS_FIELDS, STORED_MAPPING_TAG_FIELDS } from '../src/lite/mapping-metadata.js';

type ReplayCandidate = {
  source: string;
  source_work_id: string;
  work_id: string;
  canonical_title: string;
  source_title: string;
  has_explicit_metadata: boolean;
  has_title_variant: boolean;
};

type Arguments = { apply: boolean; enableAuto: boolean; limit: number; source?: string; sourceWorkId?: string };

function readArguments(values: string[]): Arguments {
  const valueAfter = (flag: string): string | undefined => {
    const index = values.indexOf(flag);
    return index >= 0 ? values[index + 1] : undefined;
  };
  const source = valueAfter('--source');
  const sourceWorkId = valueAfter('--source-work-id');
  if (Boolean(source) !== Boolean(sourceWorkId)) throw new Error('--source e --source-work-id devem ser usados juntos');
  const enableAuto = values.includes('--enable-auto');
  if (enableAuto && source) throw new Error('--enable-auto não recebe mapping alvo');
  if (!enableAuto && !source) throw new Error('o canário exige --source e --source-work-id; use --enable-auto somente após validá-lo');
  const requestedLimit = Number(valueAfter('--limit') || '1');
  if (!Number.isInteger(requestedLimit) || requestedLimit < 1 || requestedLimit > 50) throw new Error('--limit deve estar entre 1 e 50');
  return { apply: values.includes('--apply'), enableAuto, limit: requestedLimit, source, sourceWorkId };
}

async function main(): Promise<void> {
  const args = readArguments(process.argv.slice(2));
  const pool = getYugabytePool();
  try {
    if (args.enableAuto) {
      console.log(JSON.stringify({ mode: args.apply ? 'APPLY' : 'DRY_RUN', action: 'ENABLE_V4_AUTOMATIC_REPLAY' }));
      if (args.apply) await pool.query(`INSERT INTO settings(key,value,updated_at) VALUES($1,'true',now())
        ON CONFLICT(key) DO UPDATE SET value='true',updated_at=now()`, ['metadata_replay_v4_enabled']);
      return;
    }
    // A v4 task is scheduled only by this operator-controlled campaign. It
    // never revisits retired sources, contacts no provider, and only accepts
    // mappings already linked to a published canonical work. The runtime then
    // performs the normal idempotent P2 merge after native deployment.
    const fields = [...STORED_MAPPING_ALIAS_FIELDS, ...STORED_MAPPING_TAG_FIELDS];
    const candidates = await pool.query<ReplayCandidate>(`
      SELECT m.source,m.source_work_id,m.work_id::text,w.title AS canonical_title,m.source_title,
        (m.metadata ?| $1::text[]) AS has_explicit_metadata,
        lower(regexp_replace(btrim(m.source_title), '[[:space:]]+', ' ', 'g'))
          <> lower(regexp_replace(btrim(w.title), '[[:space:]]+', ' ', 'g')) AS has_title_variant
      FROM importer_work_mappings m
      JOIN works w ON w.id=m.work_id AND w.published=true
      JOIN importer_sources s ON s.id=m.source
      WHERE s.enabled=true
        AND COALESCE(s.status,'ACTIVE') IN ('ACTIVE','DEGRADED','RECOVERING')
        AND m.sync_status IN ('WAITING_ADMISSION','SYNCED','ACTIVE')
        AND m.work_id IS NOT NULL
        AND (m.metadata ?| $1::text[] OR lower(regexp_replace(btrim(m.source_title), '[[:space:]]+', ' ', 'g'))
          <> lower(regexp_replace(btrim(w.title), '[[:space:]]+', ' ', 'g')))
        AND ($2::text IS NULL OR m.source=$2)
        AND ($3::text IS NULL OR m.source_work_id=$3)
        AND NOT EXISTS (
          SELECT 1 FROM importer_lite_tasks existing
          WHERE existing.idempotency_key='lite:metadata:v4:' || m.source || ':' || m.source_work_id
        )
      ORDER BY
        CASE WHEN cardinality(COALESCE(w.aliases, ARRAY[]::text[]))=0 AND (m.metadata ?| $1::text[]) THEN 0
             WHEN cardinality(COALESCE(w.aliases, ARRAY[]::text[]))=0 THEN 1 ELSE 2 END,
        m.updated_at,m.source,m.source_work_id
      LIMIT $4`, [fields, args.source || null, args.sourceWorkId || null, args.limit]);
    const plan = candidates.rows.map((row) => ({
      source: row.source,
      sourceWorkId: row.source_work_id,
      workId: row.work_id,
      canonicalTitle: row.canonical_title,
      sourceTitle: row.source_title,
      evidence: row.has_explicit_metadata ? 'EXPLICIT_METADATA' : 'SECONDARY_SOURCE_TITLE',
    }));
    console.log(JSON.stringify({ mode: args.apply ? 'APPLY' : 'DRY_RUN', planned: plan.length, candidates: plan }, null, 2));
    if (!args.apply || plan.length === 0) return;

    const queue = new LiteRepository(pool);
    const result = await queue.enqueueMany(plan.map((candidate) => ({
      taskType: 'CATALOG' as const,
      workId: candidate.workId,
      workKey: `work:${candidate.workId}`,
      priority: 'P2' as const,
      maxAttempts: 4,
      idempotencyKey: `lite:metadata:v4:${candidate.source}:${candidate.sourceWorkId}`,
      payload: { source: candidate.source, sourceWorkId: candidate.sourceWorkId, metadataEnrichment: true,
        metadataEnrichmentVersion: 4, metadataReplayReason: candidate.evidence },
    })));
    console.log(JSON.stringify({ inserted: result.inserted, skippedExisting: plan.length - result.inserted }));
  } finally {
    await pool.end();
  }
}

void main().catch((error: unknown) => {
  console.error(JSON.stringify({ error: error instanceof Error ? error.message : String(error) }));
  process.exitCode = 1;
});
