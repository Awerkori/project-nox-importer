import type pg from 'pg';
import { computeMergedWorkAliases, isPortugueseAlias } from './identity-verifier.js';
import type {
  AliasSourceId,
  NoxWorkAliasTarget,
  WorkAliasEnrichmentResult,
} from './types.js';

const CJK_REGEX = /[\u3040-\u30ff\u3400-\u9fff\uf900-\ufaff\uac00-\ud7af]/u;

export function classifyWorkPriorityBucket(work: { title: string; aliases: string[] }): number {
  const aliases = Array.isArray(work.aliases) ? work.aliases : [];
  if (aliases.length === 0) return 1; // 1. Obras com zero aliases
  if (aliases.length === 1) return 2; // 2. Obras com apenas um alias

  const primaryIsPt = isPortugueseAlias(work.title);
  const hasIntlOrOriginal = aliases.some((a) => CJK_REGEX.test(a) || !isPortugueseAlias(a));
  if (primaryIsPt && !hasIntlOrOriginal) return 3; // 3. Título principal em PT-BR sem nome internacional/original

  const hasPtAlias = primaryIsPt || aliases.some((a) => isPortugueseAlias(a));
  if (!primaryIsPt && !hasPtAlias) return 4; // 4. Título estrangeiro sem variante brasileira

  return 5; // 5. Demais obras do catálogo
}

function isDeterministicAccessRestriction(errorMsg?: string | null): boolean {
  if (!errorMsg) return false;
  return /TOKEN_RESTRICTED|WAF_CHALLENGE|CLOUDFLARE_CHALLENGE|VRF_TOKEN_INVALID/i.test(errorMsg);
}

export class AliasOnlyRepository {
  private schemaReady = false;

  constructor(private readonly pool: pg.Pool) {}

  async ensureSchema(): Promise<void> {
    if (this.schemaReady) return;

    // Fast path: avoid distributed catalog DDL roundtrips on YugabyteDB when tables already exist
    try {
      const check = await this.pool.query<{ ok: boolean }>(`
        SELECT (
          to_regclass('public.alias_only_work_source_state') IS NOT NULL
          AND to_regclass('public.alias_only_work_checkpoint') IS NOT NULL
          AND to_regclass('public.alias_only_campaign_rollback') IS NOT NULL
        ) AS ok
      `);
      if (check.rows[0]?.ok) {
        this.schemaReady = true;
        return;
      }
    } catch {
      // PGlite or restricted catalog environments fall through to individual CREATE IF NOT EXISTS statements
    }

    const ddlStatements = [
      `CREATE TABLE IF NOT EXISTS public.alias_only_work_source_state (
        work_id uuid NOT NULL,
        source text NOT NULL,
        status text NOT NULL,
        responded boolean NOT NULL DEFAULT false,
        source_work_id text,
        source_url text,
        source_primary_title text,
        discovered_aliases text[] NOT NULL DEFAULT '{}'::text[],
        added_aliases text[] NOT NULL DEFAULT '{}'::text[],
        pt_br_aliases_added text[] NOT NULL DEFAULT '{}'::text[],
        confidence numeric(5,4) NOT NULL DEFAULT 0,
        match_method text,
        match_reason text,
        evidence jsonb NOT NULL DEFAULT '{}'::jsonb,
        attempts integer NOT NULL DEFAULT 1,
        last_error text,
        checked_at timestamptz NOT NULL DEFAULT now(),
        next_retry_at timestamptz,
        PRIMARY KEY (work_id, source)
      )`,
      `CREATE INDEX IF NOT EXISTS idx_alias_only_source_retry
        ON public.alias_only_work_source_state (status, next_retry_at)
        WHERE next_retry_at IS NOT NULL`,
      `CREATE TABLE IF NOT EXISTS public.alias_only_work_checkpoint (
        work_id uuid PRIMARY KEY,
        campaign_id text NOT NULL,
        priority_bucket smallint NOT NULL DEFAULT 5,
        responded_sources text[] NOT NULL DEFAULT '{}'::text[],
        confirmed_sources text[] NOT NULL DEFAULT '{}'::text[],
        ambiguous_sources text[] NOT NULL DEFAULT '{}'::text[],
        error_sources text[] NOT NULL DEFAULT '{}'::text[],
        aliases_before_count integer NOT NULL DEFAULT 0,
        aliases_after_count integer NOT NULL DEFAULT 0,
        aliases_added text[] NOT NULL DEFAULT '{}'::text[],
        pt_br_added text[] NOT NULL DEFAULT '{}'::text[],
        completed_at timestamptz NOT NULL DEFAULT now(),
        next_recheck_at timestamptz
      )`,
      `CREATE INDEX IF NOT EXISTS idx_alias_only_checkpoint_campaign
        ON public.alias_only_work_checkpoint (campaign_id)`,
      `CREATE INDEX IF NOT EXISTS idx_alias_only_checkpoint_recheck
        ON public.alias_only_work_checkpoint (next_recheck_at)
        WHERE next_recheck_at IS NOT NULL`,
      `CREATE TABLE IF NOT EXISTS public.alias_only_campaign_rollback (
        campaign_id text NOT NULL,
        work_id uuid NOT NULL,
        aliases_before text[] NOT NULL,
        aliases_after text[] NOT NULL,
        added_aliases text[] NOT NULL,
        updated_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (campaign_id, work_id)
      )`,
    ];

    for (const sql of ddlStatements) {
      await this.pool.query(sql);
    }
    this.schemaReady = true;
  }

  /**
   * Loads all published works pending for a given campaign, sorted strictly by Section 4 priority buckets.
   */
  async loadPendingPublishedWorksForCampaign(campaignId: string): Promise<{
    totalPublished: number;
    alreadyCompleted: number;
    pending: Array<NoxWorkAliasTarget & { priorityBucket: number }>;
  }> {
    await this.ensureSchema();
    const { rows } = await this.pool.query<{
      id: string;
      title: string;
      slug: string;
      aliases: string[] | null;
      author: string | null;
      artist: string | null;
      kind: string | null;
      year: number | null;
      age_rating: number | null;
      content_rating: string | null;
      done_campaign: string | null;
    }>(
      `
      SELECT
        w.id,
        w.title,
        w.slug,
        w.aliases,
        w.author,
        w.artist,
        w.kind,
        w.year,
        w.age_rating,
        w.content_rating,
        c.campaign_id AS done_campaign
      FROM public.works w
      LEFT JOIN public.alias_only_work_checkpoint c
        ON c.work_id = w.id AND c.campaign_id = $1
      WHERE w.published = true
      `,
      [campaignId],
    );

    let alreadyCompleted = 0;
    const pending: Array<NoxWorkAliasTarget & { priorityBucket: number }> = [];

    for (const r of rows) {
      if (r.done_campaign === campaignId) {
        alreadyCompleted++;
        continue;
      }
      const aliases = Array.isArray(r.aliases) ? r.aliases : [];
      const priorityBucket = classifyWorkPriorityBucket({ title: r.title, aliases });
      pending.push({
        id: r.id,
        title: r.title,
        slug: r.slug,
        aliases,
        author: r.author,
        artist: r.artist,
        kind: r.kind,
        year: r.year,
        ageRating: r.age_rating,
        contentRating: r.content_rating,
        priorityBucket,
      });
    }

    pending.sort((a, b) => {
      if (a.priorityBucket !== b.priorityBucket) return a.priorityBucket - b.priorityBucket;
      return a.id < b.id ? -1 : a.id > b.id ? 1 : 0;
    });

    return {
      totalPublished: rows.length,
      alreadyCompleted,
      pending,
    };
  }

  /**
   * Persists a small batch of enrichment results safely:
   * - Re-reads current `works.aliases` for modified works so concurrent chapter onboarding aliases are never lost.
   * - Updates EXCLUSIVELY `works.aliases` (and `updated_at`) when new legitimate aliases exist.
   * - Saves rollback snapshots, per-source provenance, and durable checkpoints.
   */
  async persistBatchResults(
    campaignId: string,
    items: Array<{
      target: NoxWorkAliasTarget;
      priorityBucket: number;
      result: WorkAliasEnrichmentResult;
    }>,
  ): Promise<{ worksUpdated: number; aliasesPersisted: number; ptBrPersisted: number }> {
    if (!items.length) return { worksUpdated: 0, aliasesPersisted: 0, ptBrPersisted: 0 };
    await this.ensureSchema();

    const client = await this.pool.connect();
    let worksUpdated = 0;
    let aliasesPersisted = 0;
    let ptBrPersisted = 0;

    try {
      await client.query('BEGIN');

      // Re-read fresh `title` + `aliases` for works with new aliases so we never clobber concurrent additions
      const candidateUpdateIds = items.filter((i) => i.result.addedAliases.length > 0).map((i) => i.target.id);
      const freshMap = new Map<string, { title: string; aliases: string[] }>();
      if (candidateUpdateIds.length > 0) {
        const freshRes = await client.query<{ id: string; title: string; aliases: string[] | null }>(
          `SELECT id, title, aliases FROM public.works WHERE id = ANY($1::uuid[]) FOR UPDATE`,
          [candidateUpdateIds],
        );
        for (const r of freshRes.rows) {
          freshMap.set(r.id, { title: r.title, aliases: Array.isArray(r.aliases) ? r.aliases : [] });
        }
      }

      const rollbackRows: Array<{
        campaign_id: string;
        work_id: string;
        aliases_before: string[];
        aliases_after: string[];
        added_aliases: string[];
      }> = [];
      const workUpdateRows: Array<{
        work_id: string;
        aliases_after: string[];
      }> = [];
      const sourceStateRows: Array<{
        work_id: string;
        source: string;
        status: string;
        responded: boolean;
        source_work_id: string | null;
        source_url: string | null;
        source_primary_title: string | null;
        discovered_aliases: string[];
        added_aliases: string[];
        pt_br_aliases_added: string[];
        confidence: number;
        match_method: string | null;
        match_reason: string | null;
        evidence: Record<string, unknown>;
        last_error: string | null;
        next_retry_minutes: number | null;
      }> = [];
      const checkpointRows: Array<{
        work_id: string;
        campaign_id: string;
        priority_bucket: number;
        responded_sources: string[];
        confirmed_sources: string[];
        ambiguous_sources: string[];
        error_sources: string[];
        aliases_before_count: number;
        aliases_after_count: number;
        aliases_added: string[];
        pt_br_added: string[];
        recheck_days: number | null;
      }> = [];

      for (const { target, priorityBucket, result } of items) {
        let finalAliasesAfter = result.aliasesBefore;
        let finalAdded = result.addedAliases;
        let finalPtBrAdded = result.ptBrAliasesAdded;

        if (result.addedAliases.length > 0) {
          const fresh = freshMap.get(target.id);
          if (fresh) {
            const incomingWithLang = result.addedAliases.map((title) => ({
              title,
              language: result.ptBrAliasesAdded.includes(title) ? 'pt-br' : undefined,
            }));
            const remerged = computeMergedWorkAliases(
              { ...target, title: fresh.title, aliases: fresh.aliases },
              incomingWithLang,
            );
            finalAliasesAfter = remerged.aliasesAfter;
            finalAdded = remerged.addedAliases;
            finalPtBrAdded = remerged.ptBrAliasesAdded;

            if (remerged.aliasesChanged && finalAdded.length > 0) {
              rollbackRows.push({
                campaign_id: campaignId,
                work_id: target.id,
                aliases_before: fresh.aliases,
                aliases_after: finalAliasesAfter,
                added_aliases: finalAdded,
              });
              workUpdateRows.push({
                work_id: target.id,
                aliases_after: finalAliasesAfter,
              });

              worksUpdated++;
              aliasesPersisted += finalAdded.length;
              ptBrPersisted += finalPtBrAdded.length;
            }
          }
        }

        const sources: AliasSourceId[] = ['mangadex', 'mangafire', 'mangaball', 'comix'];
        for (const src of sources) {
          const rec = result.perSource[src];
          if (!rec || (rec.evidence as any)?.skipped) continue;

          const isTransientError =
            rec.status === 'SOURCE_ERROR' && !isDeterministicAccessRestriction(rec.errorMessage);
          const nextRetryMinutes = isTransientError ? 30 : null;

          sourceStateRows.push({
            work_id: target.id,
            source: src,
            status: rec.status,
            responded: rec.responded,
            source_work_id: rec.sourceWorkId ?? null,
            source_url: rec.sourceUrl ?? null,
            source_primary_title: rec.sourcePrimaryTitle ?? null,
            discovered_aliases: rec.discoveredAliases,
            added_aliases: rec.addedAliases,
            pt_br_aliases_added: rec.ptBrAliasesAdded,
            confidence: rec.confidence,
            match_method: rec.matchMethod ?? null,
            match_reason: rec.matchReason ?? null,
            evidence: (rec.evidence || {}) as Record<string, unknown>,
            last_error: rec.errorMessage ?? null,
            next_retry_minutes: nextRetryMinutes,
          });
        }

        const afterCount = finalAliasesAfter.length;
        const recheckDays = afterCount === 0 ? 14 : null;
        checkpointRows.push({
          work_id: target.id,
          campaign_id: campaignId,
          priority_bucket: priorityBucket,
          responded_sources: result.respondedSources,
          confirmed_sources: result.confirmedSources,
          ambiguous_sources: result.ambiguousSources,
          error_sources: result.errorSources,
          aliases_before_count: result.aliasesBefore.length,
          aliases_after_count: afterCount,
          aliases_added: finalAdded,
          pt_br_added: finalPtBrAdded,
          recheck_days: recheckDays,
        });
      }

      if (rollbackRows.length > 0) {
        await client.query(
          `
          INSERT INTO public.alias_only_campaign_rollback
            (campaign_id, work_id, aliases_before, aliases_after, added_aliases, updated_at)
          SELECT
            x.campaign_id,
            x.work_id::uuid,
            ARRAY(SELECT jsonb_array_elements_text(x.aliases_before)),
            ARRAY(SELECT jsonb_array_elements_text(x.aliases_after)),
            ARRAY(SELECT jsonb_array_elements_text(x.added_aliases)),
            now()
          FROM jsonb_to_recordset($1::jsonb) AS x(
            campaign_id text,
            work_id text,
            aliases_before jsonb,
            aliases_after jsonb,
            added_aliases jsonb
          )
          ON CONFLICT (campaign_id, work_id) DO UPDATE SET
            aliases_after = EXCLUDED.aliases_after,
            added_aliases = EXCLUDED.added_aliases,
            updated_at = now()
          `,
          [JSON.stringify(rollbackRows)],
        );
      }

      if (workUpdateRows.length > 0) {
        // STRICT RULE: ONLY `works.aliases` (and `updated_at`) is ever updated on `public.works`!
        await client.query(
          `
          UPDATE public.works w
          SET aliases = ARRAY(SELECT jsonb_array_elements_text(u.aliases_after)),
              updated_at = now()
          FROM jsonb_to_recordset($1::jsonb) AS u(
            work_id text,
            aliases_after jsonb
          )
          WHERE w.id = u.work_id::uuid
          `,
          [JSON.stringify(workUpdateRows)],
        );
      }

      if (sourceStateRows.length > 0) {
        await client.query(
          `
          INSERT INTO public.alias_only_work_source_state (
            work_id, source, status, responded, source_work_id, source_url,
            source_primary_title, discovered_aliases, added_aliases, pt_br_aliases_added,
            confidence, match_method, match_reason, evidence, attempts, last_error,
            checked_at, next_retry_at
          )
          SELECT
            x.work_id::uuid,
            x.source,
            x.status,
            x.responded,
            x.source_work_id,
            x.source_url,
            x.source_primary_title,
            ARRAY(SELECT jsonb_array_elements_text(x.discovered_aliases)),
            ARRAY(SELECT jsonb_array_elements_text(x.added_aliases)),
            ARRAY(SELECT jsonb_array_elements_text(x.pt_br_aliases_added)),
            x.confidence,
            x.match_method,
            x.match_reason,
            COALESCE(x.evidence, '{}'::jsonb),
            1,
            x.last_error,
            now(),
            CASE WHEN x.next_retry_minutes IS NOT NULL
              THEN now() + (x.next_retry_minutes || ' minutes')::interval
              ELSE NULL
            END
          FROM jsonb_to_recordset($1::jsonb) AS x(
            work_id text,
            source text,
            status text,
            responded boolean,
            source_work_id text,
            source_url text,
            source_primary_title text,
            discovered_aliases jsonb,
            added_aliases jsonb,
            pt_br_aliases_added jsonb,
            confidence numeric,
            match_method text,
            match_reason text,
            evidence jsonb,
            last_error text,
            next_retry_minutes integer
          )
          ON CONFLICT (work_id, source) DO UPDATE SET
            status = EXCLUDED.status,
            responded = EXCLUDED.responded,
            source_work_id = EXCLUDED.source_work_id,
            source_url = EXCLUDED.source_url,
            source_primary_title = EXCLUDED.source_primary_title,
            discovered_aliases = EXCLUDED.discovered_aliases,
            added_aliases = CASE
              WHEN COALESCE(array_length(EXCLUDED.added_aliases, 1), 0) > 0 THEN EXCLUDED.added_aliases
              ELSE public.alias_only_work_source_state.added_aliases
            END,
            pt_br_aliases_added = CASE
              WHEN COALESCE(array_length(EXCLUDED.pt_br_aliases_added, 1), 0) > 0 THEN EXCLUDED.pt_br_aliases_added
              ELSE public.alias_only_work_source_state.pt_br_aliases_added
            END,
            confidence = EXCLUDED.confidence,
            match_method = EXCLUDED.match_method,
            match_reason = EXCLUDED.match_reason,
            evidence = EXCLUDED.evidence,
            attempts = CASE
              WHEN EXCLUDED.status = 'SOURCE_ERROR' THEN public.alias_only_work_source_state.attempts + 1
              ELSE 1
            END,
            last_error = EXCLUDED.last_error,
            checked_at = now(),
            next_retry_at = CASE
              WHEN EXCLUDED.status = 'SOURCE_ERROR'
                AND EXCLUDED.next_retry_at IS NOT NULL
                AND public.alias_only_work_source_state.attempts < 3
              THEN now() + ((30 * (public.alias_only_work_source_state.attempts + 1)) || ' minutes')::interval
              ELSE NULL
            END
          `,
          [JSON.stringify(sourceStateRows)],
        );
      }

      if (checkpointRows.length > 0) {
        await client.query(
          `
          INSERT INTO public.alias_only_work_checkpoint (
            work_id, campaign_id, priority_bucket,
            responded_sources, confirmed_sources, ambiguous_sources, error_sources,
            aliases_before_count, aliases_after_count, aliases_added, pt_br_added,
            completed_at, next_recheck_at
          )
          SELECT
            x.work_id::uuid,
            x.campaign_id,
            x.priority_bucket,
            ARRAY(SELECT jsonb_array_elements_text(x.responded_sources)),
            ARRAY(SELECT jsonb_array_elements_text(x.confirmed_sources)),
            ARRAY(SELECT jsonb_array_elements_text(x.ambiguous_sources)),
            ARRAY(SELECT jsonb_array_elements_text(x.error_sources)),
            x.aliases_before_count,
            x.aliases_after_count,
            ARRAY(SELECT jsonb_array_elements_text(x.aliases_added)),
            ARRAY(SELECT jsonb_array_elements_text(x.pt_br_added)),
            now(),
            CASE WHEN x.recheck_days IS NOT NULL
              THEN now() + (x.recheck_days || ' days')::interval
              ELSE NULL
            END
          FROM jsonb_to_recordset($1::jsonb) AS x(
            work_id text,
            campaign_id text,
            priority_bucket smallint,
            responded_sources jsonb,
            confirmed_sources jsonb,
            ambiguous_sources jsonb,
            error_sources jsonb,
            aliases_before_count integer,
            aliases_after_count integer,
            aliases_added jsonb,
            pt_br_added jsonb,
            recheck_days integer
          )
          ON CONFLICT (work_id) DO UPDATE SET
            campaign_id = EXCLUDED.campaign_id,
            priority_bucket = EXCLUDED.priority_bucket,
            responded_sources = EXCLUDED.responded_sources,
            confirmed_sources = EXCLUDED.confirmed_sources,
            ambiguous_sources = EXCLUDED.ambiguous_sources,
            error_sources = EXCLUDED.error_sources,
            aliases_after_count = EXCLUDED.aliases_after_count,
            aliases_added = CASE
              WHEN COALESCE(array_length(EXCLUDED.aliases_added, 1), 0) > 0 THEN EXCLUDED.aliases_added
              ELSE public.alias_only_work_checkpoint.aliases_added
            END,
            pt_br_added = CASE
              WHEN COALESCE(array_length(EXCLUDED.pt_br_added, 1), 0) > 0 THEN EXCLUDED.pt_br_added
              ELSE public.alias_only_work_checkpoint.pt_br_added
            END,
            completed_at = now(),
            next_recheck_at = EXCLUDED.next_recheck_at
          `,
          [JSON.stringify(checkpointRows)],
        );
      }

      await client.query('COMMIT');
      return { worksUpdated, aliasesPersisted, ptBrPersisted };
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {});
      throw err;
    } finally {
      client.release();
    }
  }

  /**
   * Used by the permanent ImporterLiteRuntime background loop to pick up:
   * 1. Newly published works that have never passed through the 4 ALIAS_ONLY sources,
   * 2. Bounded transient retries (`SOURCE_ERROR` with `attempts <= 3` and `next_retry_at <= now()`),
   * 3. Lightweight 14-day periodic rechecks for zero-alias works (`next_recheck_at <= now()`).
   */
  async findDueWorksForRuntimeEnrichment(limit = 4): Promise<
    Array<{
      target: NoxWorkAliasTarget;
      priorityBucket: number;
      onlySources?: AliasSourceId[];
      reason: 'NEW_WORK' | 'BOUNDED_RETRY' | 'PERIODIC_INCOMPLETE_RECHECK';
    }>
  > {
    await this.ensureSchema();
    const out: Array<{
      target: NoxWorkAliasTarget;
      priorityBucket: number;
      onlySources?: AliasSourceId[];
      reason: 'NEW_WORK' | 'BOUNDED_RETRY' | 'PERIODIC_INCOMPLETE_RECHECK';
    }> = [];
    const seen = new Set<string>();

    // 1. Brand-new published works without checkpoint (highest priority for new onboarding!)
    const newRes = await this.pool.query<{
      id: string;
      title: string;
      slug: string;
      aliases: string[] | null;
      author: string | null;
      artist: string | null;
      kind: string | null;
      year: number | null;
      age_rating: number | null;
      content_rating: string | null;
    }>(
      `
      SELECT w.id, w.title, w.slug, w.aliases, w.author, w.artist, w.kind, w.year, w.age_rating, w.content_rating
      FROM public.works w
      LEFT JOIN public.alias_only_work_checkpoint c ON c.work_id = w.id
      WHERE w.published = true AND c.work_id IS NULL
      ORDER BY w.created_at DESC
      LIMIT $1
      `,
      [limit],
    );

    for (const r of newRes.rows) {
      const aliases = Array.isArray(r.aliases) ? r.aliases : [];
      seen.add(r.id);
      out.push({
        target: {
          id: r.id,
          title: r.title,
          slug: r.slug,
          aliases,
          author: r.author,
          artist: r.artist,
          kind: r.kind,
          year: r.year,
          ageRating: r.age_rating,
          contentRating: r.content_rating,
        },
        priorityBucket: classifyWorkPriorityBucket({ title: r.title, aliases }),
        reason: 'NEW_WORK',
      });
    }

    if (out.length >= limit) return out;

    // 2. Bounded transient retries (max 3 attempts)
    const remaining = limit - out.length;
    const retryRes = await this.pool.query<{
      id: string;
      title: string;
      slug: string;
      aliases: string[] | null;
      author: string | null;
      artist: string | null;
      kind: string | null;
      year: number | null;
      age_rating: number | null;
      content_rating: string | null;
      retry_sources: string[];
    }>(
      `
      SELECT
        w.id, w.title, w.slug, w.aliases, w.author, w.artist, w.kind, w.year, w.age_rating, w.content_rating,
        array_agg(s.source)::text[] AS retry_sources
      FROM public.alias_only_work_source_state s
      JOIN public.works w ON w.id = s.work_id
      WHERE w.published = true
        AND s.status = 'SOURCE_ERROR'
        AND s.source != 'comix'
        AND s.attempts <= 3
        AND s.next_retry_at IS NOT NULL
        AND s.next_retry_at <= now()
      GROUP BY w.id
      LIMIT $1
      `,
      [remaining],
    );

    for (const r of retryRes.rows) {
      if (seen.has(r.id)) continue;
      seen.add(r.id);
      const aliases = Array.isArray(r.aliases) ? r.aliases : [];
      out.push({
        target: {
          id: r.id,
          title: r.title,
          slug: r.slug,
          aliases,
          author: r.author,
          artist: r.artist,
          kind: r.kind,
          year: r.year,
          ageRating: r.age_rating,
          contentRating: r.content_rating,
        },
        priorityBucket: classifyWorkPriorityBucket({ title: r.title, aliases }),
        onlySources: (r.retry_sources || []) as AliasSourceId[],
        reason: 'BOUNDED_RETRY',
      });
    }

    if (out.length >= limit) return out;

    // 3. Lightweight periodic recheck for zero-alias works after 14-day cooldown (max 1 per turn)
    const recheckRes = await this.pool.query<{
      id: string;
      title: string;
      slug: string;
      aliases: string[] | null;
      author: string | null;
      artist: string | null;
      kind: string | null;
      year: number | null;
      age_rating: number | null;
      content_rating: string | null;
    }>(
      `
      SELECT w.id, w.title, w.slug, w.aliases, w.author, w.artist, w.kind, w.year, w.age_rating, w.content_rating
      FROM public.alias_only_work_checkpoint c
      JOIN public.works w ON w.id = c.work_id
      WHERE w.published = true
        AND COALESCE(array_length(w.aliases, 1), 0) = 0
        AND c.next_recheck_at IS NOT NULL
        AND c.next_recheck_at <= now()
      ORDER BY c.next_recheck_at ASC
      LIMIT 1
      `,
    );

    for (const r of recheckRes.rows) {
      if (seen.has(r.id)) continue;
      const aliases = Array.isArray(r.aliases) ? r.aliases : [];
      out.push({
        target: {
          id: r.id,
          title: r.title,
          slug: r.slug,
          aliases,
          author: r.author,
          artist: r.artist,
          kind: r.kind,
          year: r.year,
          ageRating: r.age_rating,
          contentRating: r.content_rating,
        },
        priorityBucket: 1,
        reason: 'PERIODIC_INCOMPLETE_RECHECK',
      });
    }

    return out;
  }

  /**
   * Reverts ONLY the `works.aliases` updates performed by a specific `campaignId`.
   */
  async rollbackCampaign(campaignId: string): Promise<{ restoredWorks: number }> {
    await this.ensureSchema();
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const res = await client.query<{ row_count: number }>(
        `
        WITH restored AS (
          UPDATE public.works w
          SET aliases = r.aliases_before,
              updated_at = now()
          FROM public.alias_only_campaign_rollback r
          WHERE r.campaign_id = $1
            AND w.id = r.work_id
          RETURNING w.id
        )
        SELECT count(*)::int AS row_count FROM restored
        `,
        [campaignId],
      );
      await client.query(`DELETE FROM public.alias_only_work_checkpoint WHERE campaign_id = $1`, [campaignId]);
      await client.query('COMMIT');
      return { restoredWorks: res.rows[0]?.row_count ?? 0 };
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {});
      throw err;
    } finally {
      client.release();
    }
  }
}
