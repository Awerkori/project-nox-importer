import 'dotenv/config';
import fs from 'fs';
import path from 'path';
import pg from 'pg';
import { SourceRegistry } from '../src/sources/registry.js';
import { HostRateLimiter } from '../src/core/rate-limiter.js';
import { SourceAdapter } from '../src/sources/types.js';
import { cleanAlternativeTitle } from '../src/core/alternative-titles.js';
import { matchWorkCandidate } from '../src/core/matching.js';

const RESULTS_FILE = path.resolve(process.cwd(), 'scratch/catalog_audit_results.jsonl');
const SUMMARY_FILE = path.resolve(process.cwd(), 'scratch/catalog_audit_summary.json');
const INITIAL_100_FILE = path.resolve(process.cwd(), 'scratch/audit_100_progress.json');

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
      const cleaned = cleanAlternativeTitle(p, workTitle);
      if (cleaned && cleaned.toLowerCase() !== workTitle.toLowerCase()) {
        valid.push(cleaned);
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
  taskInfo: { state: string; checkpoint: any } | null
): Promise<any> {
  const t0 = Date.now();

  // 1. Fetch public page SSR
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

  // 2. PARALLEL Independent 43 Sources Verification
  const sourceCheckResults = await Promise.allSettled(
    sources.map((adapter) => {
      const existingMapping = mappings.find((m) => m.source === adapter.id) || null;
      return verifySourceIndependently(adapter, w.title, existingMapping, currentAliasesYsql);
    })
  );

  const sourceChecks: any[] = [];
  const discoveredAliasesSet = new Set<string>();

  for (const r of sourceCheckResults) {
    if (r.status === 'fulfilled') {
      sourceChecks.push(r.value);
      for (const a of r.value.aliasesFound) {
        discoveredAliasesSet.add(a);
      }
    }
  }

  const cleanDiscovered = Array.from(discoveredAliasesSet);
  const missingFromYsql = cleanDiscovered.filter((da) => {
    return !currentAliasesYsql.some((ca: string) => ca.toLowerCase() === da.toLowerCase());
  });

  // Determine Verdict
  let verdict: string;
  let isBug = false;

  if (cleanDiscovered.length === 0 && currentAliasesYsql.length === 0) {
    verdict = 'GENUINE_NO_ALIAS';
  } else if (missingFromYsql.length === 0) {
    verdict = 'ALL_LEGITIMATE_ALIASES_RECOVERED';
  } else {
    const taskState = taskInfo?.state || 'UNKNOWN';
    const checkpoint = taskInfo?.checkpoint || {};
    const isPartialDeferred = checkpoint.coverage === 'CROSS_SOURCE_PARTIAL_EXTERNAL_DEFERRED';

    if (taskState === 'DONE' && !isPartialDeferred) {
      verdict = 'ALIASES_IGNORED_BY_IMPORTER';
      isBug = true;
    } else {
      verdict = 'PENDING_OR_IN_PROGRESS';
    }
  }

  const durationMs = Date.now() - t0;
  return {
    workId: w.id,
    title: w.title,
    slug: w.slug,
    publicUrl: pageUrl,
    verdict,
    isBug,
    durationMs,
    ysqlAliases: currentAliasesYsql,
    publishedAliasesSSR,
    cleanDiscovered,
    missingFromYsql,
    foundSourcesCount: sourceChecks.filter((c) => c.status === 'FOUND_WITH_ALIASES' || c.status === 'FOUND_NO_ALIASES').length,
    auditedAt: new Date().toISOString(),
  };
}

async function main() {
  console.log('=== STARTING HIGH-THROUGHPUT PARALLEL CATALOG AUDIT DAEMON ===');
  const limiter = new HostRateLimiter(6.0);
  const registry = new SourceRegistry(limiter);
  const sources = registry.getAll();
  console.log(`Loaded ${sources.length} operational sources.`);

  const auditedIds = new Set<string>();
  const stats = {
    total_published: 9291,
    v6_done_total: 0,
    v6_done_audited: 0,
    v6_done_pending_audit: 0,
    audited_count: 0,
    genuine_no_alias: 0,
    all_aliases_recovered: 0,
    pending_or_in_progress: 0,
    aliases_ignored_bug: 0,
    inconclusive: 0,
    speed_works_per_min: 0,
    last_work_audited: '',
    updated_at: new Date().toISOString(),
  };

  // Pre-seed from RESULTS_FILE if present
  if (fs.existsSync(RESULTS_FILE)) {
    const lines = fs.readFileSync(RESULTS_FILE, 'utf8').split('\n').filter(Boolean);
    for (const line of lines) {
      try {
        const record = JSON.parse(line);
        if (record.workId) {
          auditedIds.add(record.workId);
          if (record.verdict === 'GENUINE_NO_ALIAS') stats.genuine_no_alias++;
          else if (record.verdict === 'ALL_LEGITIMATE_ALIASES_RECOVERED') stats.all_aliases_recovered++;
          else if (record.verdict === 'PENDING_OR_IN_PROGRESS') stats.pending_or_in_progress++;
          else if (record.verdict === 'ALIASES_IGNORED_BY_IMPORTER') stats.aliases_ignored_bug++;
          else stats.inconclusive++;
        }
      } catch {}
    }
    stats.audited_count = auditedIds.size;
    console.log(`Resuming from ${stats.audited_count} previously audited works.`);
  }

  const startTime = Date.now();
  let loopCount = 0;
  const CONCURRENCY = 6; // Process 6 works concurrently in parallel

  while (true) {
    // 1. Fetch counts
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

    // 2. Fetch next batch with V6 DONE works PRIORITIZED FIRST
    const candidates = await pool.query<{ id: string; title: string; slug: string; aliases: string[]; is_v6_done: boolean }>(`
      SELECT w.id::text, w.title, w.slug, w.aliases,
             (t.state = 'DONE') AS is_v6_done
      FROM works w
      LEFT JOIN importer_lite_tasks t ON t.work_id = w.id 
        AND t.task_type = 'CATALOG' 
        AND t.payload->>'metadataEnrichmentVersion' = '6'
      WHERE w.published = true
      ORDER BY 
        (t.state = 'DONE') DESC NULLS LAST,
        (COALESCE(cardinality(w.aliases), 0) = 0) DESC,
        w.views_total DESC
      LIMIT 1000
    `);

    const unaudited = candidates.rows.filter((w) => !auditedIds.has(w.id));

    // Calculate how many V6 DONE are audited vs pending
    let v6DoneAuditedCount = 0;
    for (const w of candidates.rows) {
      if (w.is_v6_done && auditedIds.has(w.id)) v6DoneAuditedCount++;
    }
    stats.v6_done_audited = v6DoneAuditedCount;
    stats.v6_done_pending_audit = Math.max(0, stats.v6_done_total - stats.v6_done_audited);

    if (unaudited.length === 0) {
      console.log(`All ${stats.total_published} works have been audited! Daemon sleeping 15s...`);
      stats.updated_at = new Date().toISOString();
      fs.writeFileSync(SUMMARY_FILE, JSON.stringify(stats, null, 2), 'utf8');
      await new Promise((r) => setTimeout(r, 15000));
      continue;
    }

    // Process in chunks of CONCURRENCY
    for (let i = 0; i < unaudited.length; i += CONCURRENCY) {
      const chunk = unaudited.slice(i, i + CONCURRENCY);
      const chunkIds = chunk.map((w) => w.id);

      // Single batched query for the entire chunk
      const [mRes, wRes, tRes] = await Promise.all([
        pool.query(
          'SELECT work_id::text, source, source_work_id, source_title, source_slug FROM importer_work_mappings WHERE work_id = ANY($1::uuid[])',
          [chunkIds]
        ),
        pool.query(
          'SELECT id::text, aliases FROM works WHERE id = ANY($1::uuid[])',
          [chunkIds]
        ),
        pool.query(
          `SELECT work_id::text, state, checkpoint FROM importer_lite_tasks WHERE work_id = ANY($1::uuid[]) AND task_type = 'CATALOG' AND payload->>'metadataEnrichmentVersion' = '6'`,
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

      const taskByWorkId = new Map<string, { state: string; checkpoint: any }>();
      for (const row of tRes.rows) {
        taskByWorkId.set(row.work_id, { state: row.state, checkpoint: row.checkpoint });
      }

      const records = await Promise.all(
        chunk.map(async (w) => {
          try {
            const mappings = mappingsByWorkId.get(w.id) || [];
            const currentAliases = aliasesByWorkId.get(w.id) || w.aliases || [];
            const taskInfo = taskByWorkId.get(w.id) || null;
            return await auditWork(w, sources, mappings, currentAliases, taskInfo);
          } catch (err: any) {
            console.error(`Audit error on ${w.title}:`, err?.message);
            return null;
          }
        })
      );

      for (const record of records) {
        if (!record) continue;
        auditedIds.add(record.workId);
        stats.audited_count = auditedIds.size;
        stats.last_work_audited = `${record.title} (${record.verdict})`;

        if (record.verdict === 'GENUINE_NO_ALIAS') stats.genuine_no_alias++;
        else if (record.verdict === 'ALL_LEGITIMATE_ALIASES_RECOVERED') stats.all_aliases_recovered++;
        else if (record.verdict === 'PENDING_OR_IN_PROGRESS') stats.pending_or_in_progress++;
        else if (record.verdict === 'ALIASES_IGNORED_BY_IMPORTER') {
          stats.aliases_ignored_bug++;
          console.error(`🚨 BUG ALERT on "${record.title}"! Discovered:`, record.missingFromYsql);
          fs.writeFileSync(
            path.resolve(process.cwd(), 'scratch/AUDIT_BUG_ALERT.json'),
            JSON.stringify(record, null, 2),
            'utf8'
          );
        } else {
          stats.inconclusive++;
        }

        loopCount++;
        fs.appendFileSync(RESULTS_FILE, JSON.stringify(record) + '\n', 'utf8');
        console.log(`[AUDIT ${stats.audited_count}/${stats.total_published}] ${record.title} -> ${record.verdict} (${record.durationMs}ms)`);
      }

      const elapsedMins = (Date.now() - startTime) / 60000;
      if (elapsedMins > 0) {
        stats.speed_works_per_min = parseFloat((loopCount / elapsedMins).toFixed(2));
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
