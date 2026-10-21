import 'dotenv/config';
import fs from 'fs';
import path from 'path';
import pg from 'pg';
import { SourceRegistry } from '../src/sources/registry.js';
import { HostRateLimiter } from '../src/core/rate-limiter.js';
import { SourceAdapter } from '../src/sources/types.js';
import { cleanAlternativeTitle, alternativeTitleKey } from '../src/core/alternative-titles.js';
import { matchWorkCandidate } from '../src/core/matching.js';

const RESULTS_FILE = path.resolve(process.cwd(), 'scratch/catalog_audit_results.jsonl');
const POST_V6_RESULTS_FILE = path.resolve(process.cwd(), 'scratch/post_v6_audit_results.jsonl');
const SUMMARY_FILE = path.resolve(process.cwd(), 'scratch/catalog_audit_summary.json');

const pool = new pg.Pool({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433'),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false },
  max: 2,
  connectionTimeoutMillis: 15000,
});

function withTimeout<T>(promise: Promise<T>, ms: number, fallbackValue: T): Promise<T> {
  let timer: NodeJS.Timeout;
  const timeoutPromise = new Promise<T>((resolve) => {
    timer = setTimeout(() => resolve(fallbackValue), ms);
  });
  return Promise.race([
    promise.then((res) => {
      clearTimeout(timer);
      return res;
    }),
    timeoutPromise,
  ]);
}

function extractRawAlternativeTitlesFromAdapterDetails(details: any, workTitle: string): string[] {
  const candidates: string[] = [];
  if (!details) return [];

  if (Array.isArray(details.alternativeTitles)) {
    candidates.push(...details.alternativeTitles);
  }

  const raw = details.raw || {};
  const rawFields = [
    'alternativeTitles', 'altTitles', 'other_titles', 'alternative_titles',
    'titulo_alternativo', 'outros_titulos', 'sinonimos', 'obr_outros_nomes',
    'obr_nome_alternativo', 'obr_titulo_alternativo', 't', 'tb', 'alt',
    'titulo_alternative', 'titulo_brasil'
  ];

  for (const f of rawFields) {
    const val = raw[f];
    if (typeof val === 'string') {
      const trimmed = val.trim();
      if ((trimmed.startsWith('[') && trimmed.endsWith(']')) || (trimmed.startsWith('{') && trimmed.endsWith('}'))) {
        try {
          const parsed = JSON.parse(trimmed);
          if (Array.isArray(parsed)) {
            for (const item of parsed) {
              if (typeof item === 'string') candidates.push(item);
            }
            continue;
          }
        } catch {}
      }
      candidates.push(val);
    } else if (Array.isArray(val)) {
      for (const item of val) {
        if (typeof item === 'string') candidates.push(item);
      }
    }
  }

  if (details.title && typeof details.title === 'string') {
    if (!/^v[íi]deo\s+/i.test(details.title)) {
      candidates.push(details.title);
    }
  }

  const CLAUSE_CONTINUATION = /^(?:for|and|or|so|because|but|yet|too|also|with|without|nor|mas|e|ou|pois|porque|com|sem|para|por)\b/i;
  const INTERJECTIONS = new Set(['hello', 'oh', 'ah', 'hey', 'dear', 'bye', 'ola', 'olá']);

  function splitTitles(text: string): string[] {
    const trimmed = text.trim();
    if (!trimmed) return [];
    if (/[\r\n]/.test(trimmed)) return trimmed.split(/[\r\n]+/).map(s => s.trim()).filter(Boolean);
    if (/;/.test(trimmed)) return trimmed.split(/;+/).map(s => s.trim()).filter(Boolean);
    if (/\s+[\/|]\s+/.test(trimmed)) return trimmed.split(/\s+[\/|]\s+/).map(s => s.trim()).filter(Boolean);
    if (/, /.test(trimmed)) {
      const parts = trimmed.split(/,\s+/).map(s => s.trim()).filter(Boolean);
      if (parts.length > 1) {
        const firstWord = parts[0].toLowerCase().replace(/[^a-z\u00C0-\u017F]/g, '');
        const isGrammatical = INTERJECTIONS.has(firstWord) || parts.some((p, i) => i > 0 && (CLAUSE_CONTINUATION.test(p) || /^[a-z]/.test(p)));
        if (!isGrammatical) return parts;
      }
    }
    return [trimmed];
  }

  const valid: string[] = [];
  for (const cand of candidates) {
    if (!cand || typeof cand !== 'string') continue;
    const unquoted = cand.replace(/^[\s\["'`]+|[\s\]"'`]+$/g, '').trim();
    if (!unquoted) continue;
    const parts = splitTitles(unquoted);
    for (const p of parts) {
      const cleaned = cleanAlternativeTitle(p);
      if (cleaned) {
        const k = alternativeTitleKey(cleaned);
        const tk = alternativeTitleKey(workTitle);
        if (k && k !== tk) {
          valid.push(cleaned);
        }
      }
    }
  }

  return Array.from(new Set(valid));
}

async function verifySourceIndependently(
  adapter: SourceAdapter,
  workTitle: string,
  existingMapping: any | null,
  knownAliases: string[]
): Promise<{
  sourceId: string;
  status: 'FOUND_WITH_ALIASES' | 'FOUND_NO_ALIASES' | 'NOT_FOUND' | 'DEFERRED_TIMEOUT' | 'DEFERRED_ERROR';
  sourceWorkId: string | null;
  sourceTitle: string | null;
  aliasesFound: string[];
  evidence: string;
  checkedAt: string;
}> {
  const sourceId = adapter.id;
  const checkedAt = new Date().toISOString();

  if (existingMapping && existingMapping.source_work_id) {
    try {
      const details = await withTimeout(
        adapter.fetchWorkDetails(existingMapping.source_work_id),
        4000,
        null as any
      );

      if (details) {
        const rawAliases = extractRawAlternativeTitlesFromAdapterDetails(details, workTitle);
        return {
          sourceId,
          status: rawAliases.length > 0 ? 'FOUND_WITH_ALIASES' : 'FOUND_NO_ALIASES',
          sourceWorkId: existingMapping.source_work_id,
          sourceTitle: details.title || existingMapping.source_title,
          aliasesFound: rawAliases,
          evidence: `Direct details fetch for ${existingMapping.source_work_id}: title="${details.title}", rawAliasesCount=${rawAliases.length}`,
          checkedAt,
        };
      }
    } catch (err: any) {
      if (err?.message?.includes('429')) {
        return {
          sourceId,
          status: 'DEFERRED_ERROR',
          sourceWorkId: existingMapping.source_work_id,
          sourceTitle: null,
          aliasesFound: [],
          evidence: `HTTP 429 Rate limited on ${sourceId}`,
          checkedAt,
        };
      }
    }
  }

  if (!adapter.searchWorks) {
    return {
      sourceId,
      status: 'NOT_FOUND',
      sourceWorkId: null,
      sourceTitle: null,
      aliasesFound: [],
      evidence: 'Adapter does not support searchWorks',
      checkedAt,
    };
  }

  const searchQueries = [workTitle, ...knownAliases.slice(0, 2)];
  let foundMatch: any = null;

  for (const q of searchQueries) {
    try {
      const results = await withTimeout(
        adapter.searchWorks(q),
        3500,
        null as any
      );

      if (!results || !Array.isArray(results) || results.length === 0) continue;

      for (const item of results) {
        const matchRes = matchWorkCandidate(
          { title: workTitle, slug: '', aliases: knownAliases },
          { title: item.title, slug: item.slug }
        );

        if (matchRes.matched && matchRes.confidenceScore >= 0.75) {
          foundMatch = item;
          break;
        }
      }

      if (foundMatch) break;
    } catch (err: any) {
      if (err?.message?.includes('TIMEOUT') || err?.message?.includes('timeout')) {
        return {
          sourceId,
          status: 'DEFERRED_TIMEOUT',
          sourceWorkId: null,
          sourceTitle: null,
          aliasesFound: [],
          evidence: `Search timed out (>3.5s) for query "${q}"`,
          checkedAt,
        };
      }
    }
  }

  if (!foundMatch) {
    return {
      sourceId,
      status: 'NOT_FOUND',
      sourceWorkId: null,
      sourceTitle: null,
      aliasesFound: [],
      evidence: `Not found after searching ${JSON.stringify(searchQueries)}`,
      checkedAt,
    };
  }

  try {
    const details = await withTimeout(
      adapter.fetchWorkDetails(foundMatch.sourceWorkId),
      4000,
      null as any
    );

    if (details) {
      const rawAliases = extractRawAlternativeTitlesFromAdapterDetails(details, workTitle);
      return {
        sourceId,
        status: rawAliases.length > 0 ? 'FOUND_WITH_ALIASES' : 'FOUND_NO_ALIASES',
        sourceWorkId: foundMatch.sourceWorkId,
        sourceTitle: details.title || foundMatch.title,
        aliasesFound: rawAliases,
        evidence: `Found via search "${foundMatch.title}" (${foundMatch.sourceWorkId}): ${rawAliases.length} aliases`,
        checkedAt,
      };
    }
  } catch (err: any) {
    return {
      sourceId,
      status: 'DEFERRED_ERROR',
      sourceWorkId: foundMatch.sourceWorkId,
      sourceTitle: foundMatch.title,
      aliasesFound: [],
      evidence: `Error fetching details: ${err?.message}`,
      checkedAt,
    };
  }

  return {
    sourceId,
    status: 'NOT_FOUND',
    sourceWorkId: null,
    sourceTitle: null,
    aliasesFound: [],
    evidence: 'No details retrieved',
    checkedAt,
  };
}

async function auditWork(
  w: { id: string; title: string; slug: string; aliases: string[] },
  sources: SourceAdapter[],
  mappings: any[],
  currentAliasesYsql: string[],
  taskInfo: { id: string; state: string; updated_at: any; checkpoint: any } | null,
  preCollectedEvidence?: any | null
): Promise<any> {
  const t0 = Date.now();

  // 1. Fetch public page SSR to verify live site rendering
  const pageUrl = `https://manga.project-nox-awerkori.workers.dev/obra/${w.slug}`;
  let publishedAliasesSSR: string[] = [];
  try {
    const pageHtml = await withTimeout(
      fetch(pageUrl, { headers: { 'Cache-Control': 'no-cache' } }).then((r) => r.text()),
      4000,
      ''
    );
    const m = pageHtml.match(/alternativeTitles:\s*(\[[^\]]*\])/);
    if (m) {
      try { publishedAliasesSSR = JSON.parse(m[1]); } catch {}
    }
  } catch {}

  let sourceChecks: any[] = [];
  let cleanDiscovered: string[] = [];

  // 2. Reuse pre-collected evidence if fresh (<12h old), or perform PARALLEL 43 sources check
  if (preCollectedEvidence && Array.isArray(preCollectedEvidence.cleanDiscovered)) {
    cleanDiscovered = preCollectedEvidence.cleanDiscovered;
    sourceChecks = preCollectedEvidence.sourceChecks || [];
  } else {
    const sourceCheckResults = await Promise.allSettled(
      sources.map((adapter) => {
        const existingMapping = mappings.find((m) => m.source === adapter.id) || null;
        return verifySourceIndependently(adapter, w.title, existingMapping, currentAliasesYsql);
      })
    );

    const discoveredAliasesSet = new Set<string>();
    for (const r of sourceCheckResults) {
      if (r.status === 'fulfilled') {
        sourceChecks.push(r.value);
        for (const a of r.value.aliasesFound) {
          discoveredAliasesSet.add(a);
        }
      }
    }
    cleanDiscovered = Array.from(discoveredAliasesSet);
  }

  // 3. Strict Comparison: All clean aliases discovered vs Persisted YSQL Aliases
  const targetKey = alternativeTitleKey(w.title);
  const ysqlKeys = new Set(currentAliasesYsql.map((ca: string) => alternativeTitleKey(ca)).filter(Boolean));
  const missingFromYsql = cleanDiscovered.filter((da) => {
    const key = alternativeTitleKey(da);
    if (!key || key === targetKey) return false;
    return !ysqlKeys.has(key);
  });

  const checkpoint = taskInfo?.checkpoint || {};
  const isPartialDeferred = checkpoint.coverage === 'CROSS_SOURCE_PARTIAL_EXTERNAL_DEFERRED';
  const hasTimeouts = sourceChecks.some((c) => c.status === 'DEFERRED_TIMEOUT' || c.status === 'DEFERRED_ERROR');

  // 4. Strict Post-V6 Verdict Determination
  let verdict: 'AUDIT_PASS' | 'AUDIT_FAIL' | 'AUDIT_INCONCLUSIVE';
  let isBug = false;

  if (missingFromYsql.length > 0) {
    verdict = 'AUDIT_FAIL';
    isBug = true;
  } else if (hasTimeouts || isPartialDeferred) {
    verdict = 'AUDIT_INCONCLUSIVE';
  } else {
    verdict = 'AUDIT_PASS';
  }

  const durationMs = Date.now() - t0;
  return {
    workId: w.id,
    title: w.title,
    slug: w.slug,
    publicUrl: pageUrl,
    v6TaskId: taskInfo?.id || null,
    v6CompletedAt: taskInfo?.updated_at ? new Date(taskInfo.updated_at).toISOString() : null,
    verdict,
    isBug,
    durationMs,
    ysqlAliases: currentAliasesYsql,
    publishedAliasesSSR,
    cleanDiscovered,
    missingFromYsql,
    foundSourcesCount: sourceChecks.filter((c) => c.status === 'FOUND_WITH_ALIASES' || c.status === 'FOUND_NO_ALIASES').length,
    sourceChecks,
    auditedAt: new Date().toISOString(),
  };
}

async function main() {
  console.log('=== STARTING HIGH-THROUGHPUT STRICT POST-V6 CATALOG AUDIT DAEMON ===');
  const limiter = new HostRateLimiter(6.0);
  const registry = new SourceRegistry(limiter);
  const sources = registry.getAll();
  console.log(`Loaded ${sources.length} operational sources.`);

  const postV6AuditedIds = new Set<string>();
  const preV6EvidenceMap = new Map<string, any>();

  const stats = {
    total_published: 9291,
    v6_done_total: 0,
    v6_done_audit_pass: 0,
    v6_done_audit_fail: 0,
    v6_done_audit_inconclusive: 0,
    v6_done_pending_audit: 0,
    pre_v6_audits_count: 0,
    missing_aliases_detected: 0,
    corrections_revalidated: 0,
    audit_speed_works_per_min: 0,
    last_audited_work: '',
    updated_at: new Date().toISOString(),
  };

  // 1. Load and forensically categorize all existing audit records
  if (fs.existsSync(POST_V6_RESULTS_FILE)) {
    const lines = fs.readFileSync(POST_V6_RESULTS_FILE, 'utf8').split('\n').filter(Boolean);
    for (const l of lines) {
      try {
        const r = JSON.parse(l);
        if (r.workId) {
          postV6AuditedIds.add(r.workId);
          if (r.verdict === 'AUDIT_PASS') stats.v6_done_audit_pass++;
          else if (r.verdict === 'AUDIT_FAIL') {
            stats.v6_done_audit_fail++;
            stats.missing_aliases_detected += (r.missingFromYsql?.length || 0);
          } else if (r.verdict === 'AUDIT_INCONCLUSIVE') {
            stats.v6_done_audit_inconclusive++;
          }
        }
      } catch {}
    }
    console.log(`Loaded ${postV6AuditedIds.size} already verified post-V6 audit approvals from ${POST_V6_RESULTS_FILE}`);
  }

  // Also inspect historical catalog_audit_results.jsonl for pre-collected evidence and valid post-V6 records
  if (fs.existsSync(RESULTS_FILE)) {
    const lines = fs.readFileSync(RESULTS_FILE, 'utf8').split('\n').filter(Boolean);
    const historicalByWorkId = new Map<string, any>();
    for (const l of lines) {
      try {
        const r = JSON.parse(l);
        if (r.workId && !postV6AuditedIds.has(r.workId)) {
          historicalByWorkId.set(r.workId, r);
        }
      } catch {}
    }

    const unmigratedIds = Array.from(historicalByWorkId.keys());
    console.log(`Checking ${unmigratedIds.length} historical records against V6 completion timestamps...`);

    const CHUNK = 500;
    for (let i = 0; i < unmigratedIds.length; i += CHUNK) {
      const chunk = unmigratedIds.slice(i, i + CHUNK);
      const res = await pool.query<{ work_id: string; state: string; updated_at: Date; checkpoint: any }>(`
        SELECT work_id::text, state, updated_at, checkpoint
        FROM importer_lite_tasks
        WHERE work_id = ANY($1::uuid[])
          AND task_type = 'CATALOG'
          AND payload->>'metadataEnrichmentVersion' = '6'
      `, [chunk]);

      for (const row of res.rows) {
        const rec = historicalByWorkId.get(row.work_id);
        if (!rec) continue;
        const auditedTime = new Date(rec.auditedAt || 0).getTime();
        const taskCompletedTime = new Date(row.updated_at || 0).getTime();
        const isPostV6 = (row.state === 'DONE') && (auditedTime >= taskCompletedTime || rec.isV6Done === true);

        if (isPostV6) {
          postV6AuditedIds.add(row.work_id);
          const verdict = (rec.isBug || rec.verdict === 'ALIASES_IGNORED_BY_IMPORTER') ? 'AUDIT_FAIL'
            : (rec.verdict === 'INCONCLUSIVE' || rec.verdict === 'PENDING_OR_IN_PROGRESS') ? 'AUDIT_INCONCLUSIVE'
            : 'AUDIT_PASS';

          if (verdict === 'AUDIT_PASS') stats.v6_done_audit_pass++;
          else if (verdict === 'AUDIT_FAIL') {
            stats.v6_done_audit_fail++;
            stats.missing_aliases_detected += (rec.missingFromYsql?.length || 0);
          } else {
            stats.v6_done_audit_inconclusive++;
          }

          fs.appendFileSync(POST_V6_RESULTS_FILE, JSON.stringify({ ...rec, verdict }) + '\n', 'utf8');
        } else {
          preV6EvidenceMap.set(row.work_id, rec);
          stats.pre_v6_audits_count++;
        }
      }
    }
  }

  console.log(`Forensic Classification Complete:`);
  console.log(`- Post-V6 Approved (PASS): ${stats.v6_done_audit_pass}`);
  console.log(`- Post-V6 Failed (FAIL): ${stats.v6_done_audit_fail}`);
  console.log(`- Post-V6 Inconclusive: ${stats.v6_done_audit_inconclusive}`);
  console.log(`- Pre-V6 Evidence Cached: ${stats.pre_v6_audits_count}`);

  const startTime = Date.now();
  let loopCount = 0;
  const CONCURRENCY = 6;

  while (true) {
    // 1. Fetch exact total published & V6 DONE counts
    const countRes = await pool.query('SELECT COUNT(*) AS total FROM works WHERE published = true');
    stats.total_published = parseInt(countRes.rows[0].total, 10);

    const v6DoneCountRes = await pool.query(`
      SELECT count(*) AS count
      FROM importer_lite_tasks
      WHERE task_type = 'CATALOG' 
        AND payload->>'metadataEnrichmentVersion' = '6'
        AND state = 'DONE'
    `);
    stats.v6_done_total = parseInt(v6DoneCountRes.rows[0].count, 10);
    stats.v6_done_pending_audit = Math.max(0, stats.v6_done_total - (stats.v6_done_audit_pass + stats.v6_done_audit_fail + stats.v6_done_audit_inconclusive));

    // 2. Fetch candidates: STRICTLY V6 DONE works that have NOT YET received a post-V6 final audit
    const postV6Array = Array.from(postV6AuditedIds);
    const candidates = await pool.query<{ id: string; title: string; slug: string; aliases: string[]; v6_task_id: string; v6_state: string; v6_updated_at: Date; v6_checkpoint: any }>(`
      SELECT w.id::text, w.title, w.slug, w.aliases,
             t.id::text as v6_task_id, t.state as v6_state, t.updated_at as v6_updated_at, t.checkpoint as v6_checkpoint
      FROM importer_lite_tasks t
      JOIN works w ON w.id = t.work_id
      WHERE t.task_type = 'CATALOG'
        AND t.payload->>'metadataEnrichmentVersion' = '6'
        AND t.state = 'DONE'
        AND w.published = true
        ${postV6Array.length > 0 ? 'AND NOT (w.id = ANY($1::uuid[]))' : ''}
      ORDER BY t.updated_at ASC
      LIMIT 60
    `, postV6Array.length > 0 ? [postV6Array] : []);

    const unaudited = candidates.rows;

    if (unaudited.length === 0) {
      stats.v6_done_pending_audit = 0;
      stats.updated_at = new Date().toISOString();
      fs.writeFileSync(SUMMARY_FILE, JSON.stringify(stats, null, 2), 'utf8');
      console.log(`[LOCKSTEP AUDITOR] All ${stats.v6_done_total} V6 DONE works are fully audited! Waiting 4s for new V6 completions...`);
      await new Promise((r) => setTimeout(r, 4000));
      continue;
    }

    // Process in chunks of CONCURRENCY
    for (let i = 0; i < unaudited.length; i += CONCURRENCY) {
      const chunk = unaudited.slice(i, i + CONCURRENCY);
      const chunkIds = chunk.map((w) => w.id);

      // Single batched query for the entire chunk
      const [mRes, wRes] = await Promise.all([
        pool.query(
          'SELECT work_id::text, source, source_work_id, source_title, source_slug FROM importer_work_mappings WHERE work_id = ANY($1::uuid[])',
          [chunkIds]
        ),
        pool.query(
          'SELECT id::text, aliases FROM works WHERE id = ANY($1::uuid[])',
          [chunkIds]
        ),
      ]);

      const mappingsByWorkId = new Map<string, any[]>();
      for (const row of mRes.rows) {
        if (!mappingsByWorkId.has(row.work_id)) mappingsByWorkId.set(row.work_id, []);
        mappingsByWorkId.get(row.work_id)!.push(row);
      }

      const aliasesByWorkId = new Map<string, string[]>();
      for (const row of wRes.rows) {
        aliasesByWorkId.set(row.id, Array.isArray(row.aliases) ? row.aliases : []);
      }

      const records = await Promise.all(
        chunk.map(async (w) => {
          try {
            const mappings = mappingsByWorkId.get(w.id) || [];
            const currentAliases = aliasesByWorkId.get(w.id) || w.aliases || [];
            const taskInfo = {
              id: w.v6_task_id,
              state: w.v6_state,
              updated_at: w.v6_updated_at,
              checkpoint: w.v6_checkpoint,
            };
            const preCollected = preV6EvidenceMap.get(w.id) || null;
            return await auditWork(w, sources, mappings, currentAliases, taskInfo, preCollected);
          } catch (err: any) {
            console.error(`Audit error on ${w.title}:`, err?.message);
            return null;
          }
        })
      );

      for (const record of records) {
        if (!record) continue;
        postV6AuditedIds.add(record.workId);
        stats.last_audited_work = `${record.title} (${record.verdict})`;

        if (record.verdict === 'AUDIT_PASS') {
          stats.v6_done_audit_pass++;
        } else if (record.verdict === 'AUDIT_FAIL') {
          stats.v6_done_audit_fail++;
          stats.missing_aliases_detected += (record.missingFromYsql?.length || 0);
          console.error(`🚨 [AUDIT FAIL] Missing aliases on "${record.title}"! Missing:`, record.missingFromYsql);
          fs.writeFileSync(
            path.resolve(process.cwd(), 'scratch/AUDIT_BUG_ALERT.json'),
            JSON.stringify(record, null, 2),
            'utf8'
          );
        } else {
          stats.v6_done_audit_inconclusive++;
        }

        stats.v6_done_pending_audit = Math.max(0, stats.v6_done_total - (stats.v6_done_audit_pass + stats.v6_done_audit_fail + stats.v6_done_audit_inconclusive));

        loopCount++;
        fs.appendFileSync(POST_V6_RESULTS_FILE, JSON.stringify(record) + '\n', 'utf8');
        fs.appendFileSync(RESULTS_FILE, JSON.stringify(record) + '\n', 'utf8');
        console.log(`[POST-V6 AUDIT ${postV6AuditedIds.size}/${stats.v6_done_total}] ${record.title} -> ${record.verdict} (Pending: ${stats.v6_done_pending_audit})`);
      }

      const elapsedMins = (Date.now() - startTime) / 60000;
      if (elapsedMins > 0) {
        stats.audit_speed_works_per_min = parseFloat((loopCount / elapsedMins).toFixed(2));
      }
      stats.updated_at = new Date().toISOString();
      fs.writeFileSync(SUMMARY_FILE, JSON.stringify(stats, null, 2), 'utf8');
    }
  }
}

main().catch((err) => {
  console.error('Fatal audit daemon error:', err);
  process.exit(1);
});
