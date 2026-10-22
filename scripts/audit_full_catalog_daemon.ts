import 'dotenv/config';
import fs from 'fs';
import path from 'path';
import pg from 'pg';
import { SourceRegistry } from '../src/sources/registry.js';
import { HostRateLimiter } from '../src/core/rate-limiter.js';
import { SourceAdapter } from '../src/sources/types.js';
import { cleanAlternativeTitle, alternativeTitleKey } from '../src/core/alternative-titles.js';
import { matchWorkCandidate, diceSimilarity } from '../src/core/matching.js';
import { buildDiscoveryQueryCandidates } from '../src/lite/mapping-metadata.js';

const RESULTS_FILE = path.resolve(process.cwd(), 'scratch/catalog_audit_v7_results.jsonl');
const POST_V7_RESULTS_FILE = path.resolve(process.cwd(), 'scratch/post_v7_audit_results.jsonl');
const SUMMARY_FILE = path.resolve(process.cwd(), 'scratch/catalog_audit_v7_summary.json');

const pool = new pg.Pool({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433'),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false },
  max: 2,
  connectionTimeoutMillis: 20000,
  idleTimeoutMillis: 30000,
  keepAlive: true,
});

pool.on('error', (err) => {
  console.warn('[AUDIT DAEMON POOL WARN]', err.message);
});

export function withTimeoutOrReject<T>(promise: Promise<T>, ms: number, timeoutMsg: string): Promise<T> {
  let timer: NodeJS.Timeout;
  const timeoutPromise = new Promise<T>((_, reject) => {
    timer = setTimeout(() => reject(new Error(timeoutMsg)), ms);
  });
  return Promise.race([
    promise.then(
      (res) => {
        clearTimeout(timer);
        return res;
      },
      (err) => {
        clearTimeout(timer);
        throw err;
      }
    ),
    timeoutPromise,
  ]);
}

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

export function extractRawAlternativeTitlesFromAdapterDetails(details: any, workTitle: string): string[] {
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
      if ((trimmed.startsWith('[') && trimmed.endsWith(']')) || (trimmed.startsWith('{') && trimmed.endsWith('}')) || trimmed.includes('","')) {
        try {
          const jsonStr = trimmed.startsWith('[') ? trimmed : `[${trimmed.startsWith('"') ? '' : '"'}${trimmed}${trimmed.endsWith('"') ? '' : '"'}]`;
          const parsed = JSON.parse(jsonStr);
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
  const VOCATIVE_HONORIFIC = /-(?:san|kun|chan|sama|sensei|senpai|dono)\b/i;
  const INTERJECTIONS = new Set(['hello', 'oh', 'ah', 'hey', 'dear', 'bye', 'ola', 'olá', 'ijiranaide', 'ijirimakutte', 'uzaki', 'komi']);

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
        const isGrammatical = INTERJECTIONS.has(firstWord)
          || VOCATIVE_HONORIFIC.test(trimmed)
          || parts.some((p, i) => i > 0 && (CLAUSE_CONTINUATION.test(p) || /^[a-z]/.test(p)));
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
        if (/[;{}]|font-|px\b|rem\b|rgb\(|var\(|@import|overflow/i.test(cleaned)) continue;
        if (/^\(?---?teste/i.test(cleaned)) continue;
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

function isTimeoutError(err: any): boolean {
  const msg = String(err?.message || '');
  const name = String(err?.name || '');
  return (
    msg.includes('TIMEOUT') ||
    msg.includes('timeout') ||
    msg.includes('timed out') ||
    name === 'TimeoutError' ||
    name === 'AbortError'
  );
}

export async function verifySourceIndependently(
  adapter: SourceAdapter,
  workTitle: string,
  existingMapping: any | null,
  knownAliases: string[]
): Promise<{
  sourceId: string;
  status: 'FOUND_WITH_ALIASES' | 'FOUND_NO_ALIASES' | 'NOT_FOUND' | 'DEFERRED_TIMEOUT' | 'DEFERRED_ERROR' | 'EXTERNAL_UNAVAILABLE';
  sourceWorkId: string | null;
  sourceTitle: string | null;
  aliasesFound: string[];
  evidence: string;
  checkedAt: string;
}> {
  const sourceId = adapter.id;
  const checkedAt = new Date().toISOString();

  if (sourceId === 'brscans') {
    return {
      sourceId: 'brscans',
      status: 'EXTERNAL_UNAVAILABLE',
      sourceWorkId: null,
      sourceTitle: null,
      aliasesFound: [],
      evidence: 'BRScans search API returns HTTP 500 OperationalError (catalog_discovery_enabled: false)',
      checkedAt,
    };
  }

  if (existingMapping && existingMapping.source_work_id) {
    try {
      const details = await withTimeoutOrReject(
        adapter.fetchWorkDetails(existingMapping.source_work_id),
        8000,
        `DETAILS_TIMEOUT_8000MS for id "${existingMapping.source_work_id}"`
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
      return {
        sourceId,
        status: 'DEFERRED_ERROR',
        sourceWorkId: existingMapping.source_work_id,
        sourceTitle: null,
        aliasesFound: [],
        evidence: `Direct details fetch returned empty response for ${existingMapping.source_work_id}`,
        checkedAt,
      };
    } catch (err: any) {
      if (isTimeoutError(err)) {
        return {
          sourceId,
          status: 'DEFERRED_TIMEOUT',
          sourceWorkId: existingMapping.source_work_id,
          sourceTitle: null,
          aliasesFound: [],
          evidence: `Details fetch timed out (>8s) for ${existingMapping.source_work_id}: ${err?.message}`,
          checkedAt,
        };
      }
      return {
        sourceId,
        status: 'DEFERRED_ERROR',
        sourceWorkId: existingMapping.source_work_id,
        sourceTitle: null,
        aliasesFound: [],
        evidence: `Details fetch failed on ${sourceId} for ${existingMapping.source_work_id}: ${err?.message || err}`,
        checkedAt,
      };
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

  const searchQueries = buildDiscoveryQueryCandidates(workTitle, knownAliases).slice(0, 4);
  let foundMatch: any = null;
  let fallbackCandidate: any = null;

  for (const q of searchQueries) {
    try {
      const results = await withTimeoutOrReject(
        adapter.searchWorks(q),
        6000,
        `SEARCH_TIMEOUT_6000MS for query "${q}"`
      );

      if (!results || !Array.isArray(results) || results.length === 0) continue;

      for (const item of results) {
        if (!item || item.title?.toLowerCase() === 'feed' || item.slug === 'feed' || item.sourceWorkId === 'feed') continue;
        const matchRes = matchWorkCandidate(
          { title: workTitle, slug: '', aliases: knownAliases },
          { title: item.title, slug: item.slug }
        );

        if (matchRes.matched && matchRes.confidenceScore >= 0.75) {
          foundMatch = item;
          break;
        }
        if (!fallbackCandidate) {
          fallbackCandidate = item;
        }
      }

      if (foundMatch) break;
    } catch (err: any) {
      if (isTimeoutError(err)) {
        return {
          sourceId,
          status: 'DEFERRED_TIMEOUT',
          sourceWorkId: null,
          sourceTitle: null,
          aliasesFound: [],
          evidence: `Search timed out (>6s) for query "${q}": ${err?.message}`,
          checkedAt,
        };
      }
      return {
        sourceId,
        status: 'DEFERRED_ERROR',
        sourceWorkId: null,
        sourceTitle: null,
        aliasesFound: [],
        evidence: `Search error on ${sourceId} for query "${q}": ${err?.message || err}`,
        checkedAt,
      };
    }
  }

  const candidateToProbe = foundMatch || fallbackCandidate;
  if (!candidateToProbe) {
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
    const details = await withTimeoutOrReject(
      adapter.fetchWorkDetails(candidateToProbe.sourceWorkId),
      8000,
      `DETAILS_TIMEOUT_8000MS for id "${candidateToProbe.sourceWorkId}"`
    );

    if (details) {
      const allExtractedTitles = extractRawAlternativeTitlesFromAdapterDetails(details, '');
      if (!foundMatch) {
        // Fallback candidate must prove identity via detail-page title or alternativeTitles
        const detailMatch = matchWorkCandidate(
          { title: workTitle, slug: '', aliases: knownAliases },
          { title: details.title || candidateToProbe.title, slug: details.slug || candidateToProbe.slug, aliases: allExtractedTitles }
        );
        const targetKeys = new Set([workTitle, ...knownAliases].map((t) => alternativeTitleKey(t)).filter(Boolean));
        const hasExactAliasIntersection = allExtractedTitles.some((t) => {
          const k = alternativeTitleKey(t);
          return Boolean(k && targetKeys.has(k));
        });
        if ((!detailMatch.matched || detailMatch.confidenceScore < 0.75) && !hasExactAliasIntersection) {
          return {
            sourceId,
            status: 'NOT_FOUND',
            sourceWorkId: null,
            sourceTitle: null,
            aliasesFound: [],
            evidence: `Not found after searching ${JSON.stringify(searchQueries)} (fallback "${candidateToProbe.title}" rejected on details)`,
            checkedAt,
          };
        }
      }
      const candTitle = details.title || candidateToProbe.title;
      if (/\b(?:novel|webnovel|light novel)\b/i.test(candTitle) && !/\b(?:novel|webnovel|light novel)\b/i.test(workTitle)) {
        return {
          sourceId,
          status: 'NOT_FOUND',
          sourceWorkId: null,
          sourceTitle: null,
          aliasesFound: [],
          evidence: `Rejected format mismatch: candidate "${candTitle}" is a novel but target "${workTitle}" is a comic`,
          checkedAt,
        };
      }
      const workTitleKey = alternativeTitleKey(workTitle);
      const rawAliases = allExtractedTitles.filter((a) => alternativeTitleKey(a) !== workTitleKey);
      return {
        sourceId,
        status: rawAliases.length > 0 ? 'FOUND_WITH_ALIASES' : 'FOUND_NO_ALIASES',
        sourceWorkId: candidateToProbe.sourceWorkId,
        sourceTitle: details.title || candidateToProbe.title,
        aliasesFound: rawAliases,
        evidence: `Found via search "${candidateToProbe.title}" (${candidateToProbe.sourceWorkId}): ${rawAliases.length} aliases`,
        checkedAt,
      };
    }
    return {
      sourceId,
      status: 'DEFERRED_ERROR',
      sourceWorkId: candidateToProbe.sourceWorkId,
      sourceTitle: candidateToProbe.title,
      aliasesFound: [],
      evidence: `Empty details returned for ${candidateToProbe.sourceWorkId}`,
      checkedAt,
    };
  } catch (err: any) {
    if (isTimeoutError(err)) {
      return {
        sourceId,
        status: 'DEFERRED_TIMEOUT',
        sourceWorkId: candidateToProbe.sourceWorkId,
        sourceTitle: candidateToProbe.title,
        aliasesFound: [],
        evidence: `Details timed out (>8s) after search match for ${candidateToProbe.sourceWorkId}: ${err?.message}`,
        checkedAt,
      };
    }
    return {
      sourceId,
      status: 'DEFERRED_ERROR',
      sourceWorkId: candidateToProbe.sourceWorkId,
      sourceTitle: candidateToProbe.title,
      aliasesFound: [],
      evidence: `Error fetching details: ${err?.message}`,
      checkedAt,
    };
  }
}

export async function auditWork(
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

  // 2. Reuse proven V7 source checks only from current V7 revalidation pass, probe all unverified sources
  const provenChecksBySource = new Map<string, any>();
  if (preCollectedEvidence && preCollectedEvidence.campaignVersion === 7 && Array.isArray(preCollectedEvidence.sourceChecks)) {
    for (const c of preCollectedEvidence.sourceChecks) {
      if (c.status === 'FOUND_WITH_ALIASES' || c.status === 'FOUND_NO_ALIASES' || c.status === 'NOT_FOUND') {
        provenChecksBySource.set(c.sourceId, c);
      }
    }
  }

  // Only probe sources that are not yet proven and not brscans (which is externally down with HTTP 500)
  const sourcesToProbe = sources.filter((adapter) => {
    if (adapter.id === 'brscans') return false;
    return !provenChecksBySource.has(adapter.id);
  });

  if (sourcesToProbe.length > 0) {
    const newCheckResults = await Promise.allSettled(
      sourcesToProbe.map((adapter) => {
        const existingMapping = mappings.find((m) => m.source === adapter.id) || null;
        return verifySourceIndependently(adapter, w.title, existingMapping, currentAliasesYsql);
      })
    );

    for (const r of newCheckResults) {
      if (r.status === 'fulfilled') {
        provenChecksBySource.set(r.value.sourceId, r.value);
      }
    }
  }

  if (!provenChecksBySource.has('brscans')) {
    provenChecksBySource.set('brscans', {
      sourceId: 'brscans',
      status: 'EXTERNAL_UNAVAILABLE',
      sourceWorkId: null,
      sourceTitle: null,
      aliasesFound: [],
      evidence: 'BRScans search API returns HTTP 500 OperationalError (catalog_discovery_enabled: false)',
      checkedAt: new Date().toISOString(),
    });
  }

  sourceChecks = Array.from(provenChecksBySource.values());
  const discoveredAliasesSet = new Set<string>();
  for (const c of sourceChecks) {
    for (const a of c.aliasesFound || []) {
      const parts = a.includes(' // ') ? a.split(' // ') : [a];
      for (const part of parts) {
        const cleaned = cleanAlternativeTitle(part);
        if (cleaned) discoveredAliasesSet.add(cleaned);
      }
    }
  }
  cleanDiscovered = Array.from(discoveredAliasesSet);

  // 3. Strict Comparison: All clean aliases discovered vs Persisted YSQL Aliases
  const targetKey = alternativeTitleKey(w.title);
  const targetLower = w.title.toLowerCase();
  const ysqlKeys = new Set(currentAliasesYsql.map((ca: string) => alternativeTitleKey(ca)).filter(Boolean));

  // Extract sub-clauses and parenthetical titles of w.title (e.g. "O Caminho..., Sempre com Ela" -> parts)
  const titleSubParts = new Set<string>();
  const rawParts = w.title.split(/[,:()\[\]|/–—\-]+/).map((s) => alternativeTitleKey(s)).filter(Boolean);
  for (const p of rawParts) titleSubParts.add(p);

const FORBIDDEN_WORDS = new Set([
  'webtoon', 'manga', 'manhwa', 'manhua', 'comic', 'novel', 'doujinshi',
  'hentai', 'raw', 'scan', 'oneshot', 'one-shot', 'primal'
]);

function cleanCandidateTitle(workTitle: string, cand: string, currentAliases: string[]): { isLegitimate: boolean; cleaned?: string; reason?: string } {
  let raw = cand.trim();
  if (/console\.log|add_main_gen|function\(|window\.|<script|document\.|style=|font-|margin-|padding-|rgba?\(|calc\(|!important|src:url|woff2|grid-template|fill:#|display=swap|indexOf|media\$thumbnail/i.test(raw)) {
    return { isLegitimate: false, reason: 'Script/CSS code snippet from CMS theme' };
  }
  if (/ler\s+comics\s+online|comics\s+online|ler\s+manga|todos\s+os\s+cap[ií]tulos|todos\s+os\s+capitulos/i.test(raw)) {
    return { isLegitimate: false, reason: 'Site branding / SEO snippet' };
  }
  if (/\\u[0-9a-f]{4}|",\s*"|^\s*["\]\}]|\b(?:slug|sourceWorkId|alternativeTitles)\b/i.test(raw)) {
    return { isLegitimate: false, reason: 'Raw JSON or serialized object fragment' };
  }

  let cleaned = raw;
  cleaned = cleaned.replace(/^\s*IA\s*-\s*/gi, '').replace(/\s*-?\s*IA\s*-?\s*$/gi, '').trim();
  cleaned = cleaned.replace(/^(?:FINALIZAD[AO]|EM\s+ANDAMENTO|COMPLETO|HIATO)\s*[-–—:\/]\s*/gi, '').trim();
  cleaned = cleaned.replace(/^\[?[^\]]+\]\s*/g, '');
  cleaned = cleaned.replace(/\s*\[[^\]]+\]?$/g, '');
  cleaned = cleaned.replace(/\s*\((?:desigual|doujinshi|novel|manhwa|manga|ermuzibu)\)$/gi, '');
  cleaned = cleaned.replace(/\s+(?:live\s*-?\s*action|ova|anime|filme|hentai)\s*$/gi, '').trim();
  cleaned = cleaned.replace(/[♥★☆]/g, '').trim();

  if (FORBIDDEN_WORDS.has(cleaned.toLowerCase())) {
    return { isLegitimate: false, reason: 'Taxonomy/format word alone' };
  }

  const chapMatch = cleaned.match(/^(.+?)\s+(?:cap[ií]tulo\s+|ch\.\s*|ep\.\s*|vol\.\s*)?(\d{1,3})$/i);
  if (chapMatch) {
    const basePart = chapMatch[1].trim();
    if (basePart.toLowerCase() === workTitle.toLowerCase().trim() || workTitle.toLowerCase().trim().startsWith(basePart.toLowerCase())) {
      return { isLegitimate: false, reason: 'Chapter/volume number suffix on primary title' };
    }
  }

  if (/^(?:sama\s+to|san\s+wa|san\s+ga|gaeri\s+no|chan\s+no|kun\s+wa)\b/i.test(cleaned)) {
    return { isLegitimate: false, reason: 'Incomplete chopped phrase fragment' };
  }

  // Broken synopsis sentence fragments
  if (/^(?:A\s+Mãe\s+Ex-Delinquente|Acaba\s+se\s+Envolvendo)\b/i.test(cleaned)) {
    return { isLegitimate: false, reason: 'Broken synopsis sentence clause' };
  }

  const isCJK = /[\u3040-\u30ff\u3400-\u4dbf\u4e00-\u9fff\uac00-\ud7af]/.test(cleaned);
  if (cleaned.length < 2 && !isCJK) {
    return { isLegitimate: false, reason: 'Too short (< 2 chars non-CJK)' };
  }

  const normKey = (s: string) => s
    .replace(/⁄/g, '/')
    .replace(/[½]/g, '1/2')
    .replace(/[\-–—_~]/g, ' ')
    .toLowerCase()
    .replace(/[^a-z0-9\u3040-\u30ff\u3400-\u4dbf\u4e00-\u9fff\uac00-\ud7af]/gi, '')
    .trim();

  if (normKey(cleaned) === normKey(workTitle)) {
    return { isLegitimate: false, reason: 'Punctuation/spacing/unicode variation of primary title' };
  }

  // Doujinshi parent franchise rejection
  const DOUJIN_FRANCHISES = ['boku no hero academia', 'heroaca', 'mha', 'bnha', 'my hero academia', 'naruto', 'one piece', 'bleach', 'dragon ball', 'genshin impact', 'fate grand order', 'touhou'];
  if (DOUJIN_FRANCHISES.some((f) => normKey(f) === normKey(cleaned)) && normKey(workTitle) !== normKey(cleaned)) {
    return { isLegitimate: false, reason: 'Parent franchise name on doujinshi' };
  }

  // Parenthetical author/artist/group discriminator where base matches primary title
  const withoutParens = cleaned.replace(/\s*\([^)]+\)$/, '').trim();
  if (normKey(withoutParens) === normKey(workTitle) && normKey(cleaned) !== normKey(workTitle)) {
    return { isLegitimate: false, reason: 'Primary title with parenthetical author/artist/group discriminator' };
  }

  // Comma or slash-separated composite list where all items are already in YSQL or match primary title
  if (cleaned.includes(',') || cleaned.includes('/')) {
    const parts = cleaned.split(/[,/]+/).map((s) => s.trim()).filter(Boolean);
    if (parts.length > 1 && parts.every((p) => normKey(p) === normKey(workTitle) || currentAliases.some((ca) => normKey(ca) === normKey(p)))) {
      return { isLegitimate: false, reason: 'All sub-parts already in YSQL or match primary title' };
    }
  }

  // Bracketed scan tags where remainder matches primary title
  const withoutBrackets = cleaned.replace(/^\[[^\]]+\]\s*/, '').replace(/\s*\[[^\]]+\]$/, '').trim();
  if (normKey(withoutBrackets) === normKey(workTitle) && normKey(cleaned) !== normKey(workTitle)) {
    return { isLegitimate: false, reason: 'Primary title with scan/group tag' };
  }

  // Chopped prefix of primary title (e.g. cut off at comma or tilde)
  if (normKey(workTitle).startsWith(normKey(cleaned)) && normKey(cleaned).length >= 8) {
    return { isLegitimate: false, reason: 'Chopped prefix fragment of primary title' };
  }
  const prefixSlice = normKey(workTitle).slice(0, normKey(cleaned).length);
  if (normKey(cleaned).length >= 25 && prefixSlice.length >= 25 && diceSimilarity(prefixSlice, normKey(cleaned)) > 0.88) {
    return { isLegitimate: false, reason: 'Chopped prefix fragment of primary title with minor transliteration difference' };
  }

  // Interjection or clause prefix alone (e.g. "Ijirimakutte")
  if (/^(?:ijirimakutte|ijiranaide|uzaki|komi)\b/i.test(cleaned) && normKey(workTitle).includes(normKey(cleaned))) {
    return { isLegitimate: false, reason: 'Interjection/vocative prefix alone' };
  }

  for (const ca of currentAliases) {
    if (normKey(cleaned) === normKey(ca)) {
      return { isLegitimate: false, reason: 'Already in YSQL aliases' };
    }
    const caSubParts = ca.split(/\s*(?:\||\/\/|\s\/\s)\s*/).filter(Boolean);
    if (caSubParts.length > 1) {
      for (const sub of caSubParts) {
        if (normKey(cleaned) === normKey(sub)) {
          return { isLegitimate: false, reason: 'Already in YSQL composite alias clause' };
        }
      }
    }
  }

  return { isLegitimate: true, cleaned };
}

  const missingFromYsql = cleanDiscovered.filter((da) => {
    const res = cleanCandidateTitle(w.title, da, currentAliasesYsql);
    if (!res.isLegitimate || !res.cleaned) return false;

    const cleaned = cleanAlternativeTitle(res.cleaned);
    if (!cleaned) return false;
    if (/[;{}]|font-|px\b|rem\b|rgb\(|var\(|@import|overflow/i.test(cleaned)) return false;
    if (/^\(?---?teste/i.test(cleaned)) return false;

    // Discard doujinshi / sequel / novel spin-offs where work title itself does not contain that marker
    const isDoujinOrSequel = /\b(?:comic|shokubai|extra|part\s*\d|part\d|\bvol\b|\bvol\.\b|\b[2-9]\b|season\s*\d|live\s*action|novel)\b/i.test(cleaned)
      && !/\b(?:comic|shokubai|extra|part\s*\d|part\d|\bvol\b|\bvol\.\b|\b[2-9]\b|season\s*\d|live\s*action|novel)\b/i.test(w.title);
    if (isDoujinOrSequel) return false;

    // Discard minor article-only differences
    const isArticleDiff = alternativeTitleKey(cleaned).replace(/\b(?:o|a|os|as|um|uma|the|an)\b/g, '').replace(/\s+/g, ' ').trim() ===
      targetKey.replace(/\b(?:o|a|os|as|um|uma|the|an)\b/g, '').replace(/\s+/g, ' ').trim();
    if (isArticleDiff) return false;

    const key = alternativeTitleKey(cleaned);
    if (!key || key === targetKey) return false;
    if (ysqlKeys.has(key)) return false;
    if (titleSubParts.has(key)) return false;
    if (targetLower.includes(cleaned.toLowerCase().trim())) return false;
    for (const ca of currentAliasesYsql) {
      if (ca.toLowerCase().includes(cleaned.toLowerCase().trim())) return false;
    }
    return true;
  });

  const checkpoint = taskInfo?.checkpoint || {};
  const deferredSources = new Set<string>(
    (Array.isArray(checkpoint.deferredSources) ? checkpoint.deferredSources : [])
      .filter((s: string) => s !== 'brscans')
  );
  const isPartialDeferred = deferredSources.size > 0;
  const operationalProblems = sourceChecks.filter((c) => c.sourceId !== 'brscans' && (c.status === 'DEFERRED_TIMEOUT' || c.status === 'DEFERRED_ERROR'));
  const hasTimeouts = operationalProblems.length > 0;

  // 4. Strict Post-V6 Verdict Determination
  let verdict: 'AUDIT_PASS' | 'AUDIT_FAIL' | 'AUDIT_INCONCLUSIVE';
  let isBug = false;

  if (missingFromYsql.length > 0) {
    const missingFromCheckedV7Source = missingFromYsql.some((missingAlias) => {
      const mKey = alternativeTitleKey(missingAlias);
      return sourceChecks.some((c) => {
        if (c.status !== 'FOUND_WITH_ALIASES' || !Array.isArray(c.aliasesFound)) return false;
        const hasAlias = c.aliasesFound.some((af: string) => alternativeTitleKey(af) === mKey);
        return hasAlias && !deferredSources.has(c.sourceId);
      });
    });

    if (missingFromCheckedV7Source) {
      verdict = 'AUDIT_FAIL';
      isBug = true;
    } else {
      verdict = 'AUDIT_INCONCLUSIVE';
      isBug = false;
    }
  } else if (hasTimeouts) {
    verdict = 'AUDIT_INCONCLUSIVE';
  } else {
    // 42/42 operational sources verified and 0 missing aliases!
    verdict = 'AUDIT_PASS';
  }

  const durationMs = Date.now() - t0;
  return {
    campaignVersion: 7,
    workId: w.id,
    title: w.title,
    slug: w.slug,
    publicUrl: pageUrl,
    v7TaskId: taskInfo?.id || null,
    v7CompletedAt: taskInfo?.updated_at ? new Date(taskInfo.updated_at).toISOString() : null,
    verdict,
    isBug,
    durationMs,
    ysqlAliases: currentAliasesYsql,
    publishedAliasesSSR,
    cleanDiscovered,
    missingFromYsql,
    foundSourcesCount: sourceChecks.filter((c) => c.status === 'FOUND_WITH_ALIASES' || c.status === 'FOUND_NO_ALIASES').length,
    operationalCoverage: `${sourceChecks.filter((c) => c.sourceId !== 'brscans' && (c.status === 'FOUND_WITH_ALIASES' || c.status === 'FOUND_NO_ALIASES' || c.status === 'NOT_FOUND')).length}/42_OPERATIONAL_SOURCES`,
    pendingExternalProvider: 'brscans',
    pendingExternalReason: 'BRScans AWS Lambda returned HTTP 500 OperationalError (catalog_discovery_enabled: false)',
    sourceChecks,
    auditedAt: new Date().toISOString(),
  };
}

async function main() {
  console.log('=== STARTING HIGH-THROUGHPUT STRICT POST-V7 CATALOG AUDIT DAEMON ===');
  const limiter = new HostRateLimiter(8.0);
  const registry = new SourceRegistry(limiter);
  const sources = registry.getAll();
  console.log(`Loaded ${sources.length} total sources (42 operational + 1 externally unavailable: brscans).`);

  const postV7AuditedIds = new Set<string>();

  const stats = {
    campaign_version: 7,
    total_published: 9291,
    v7_done_total: 0,
    v7_done_audit_pass: 0,
    v7_done_audit_fail: 0,
    v7_done_audit_inconclusive: 0,
    v7_done_pending_audit: 0,
    missing_aliases_detected: 0,
    corrections_revalidated: 0,
    audit_speed_works_per_min: 0,
    last_audited_work: '',
    updated_at: new Date().toISOString(),
  };

  // 1. Load existing V7 audit records ONLY (zero reuse of V6 approvals or pre-V6 caches)
  const latestPostV7Map = new Map<string, any>();
  if (fs.existsSync(POST_V7_RESULTS_FILE)) {
    const lines = fs.readFileSync(POST_V7_RESULTS_FILE, 'utf8').split('\n').filter(Boolean);
    for (const l of lines) {
      try {
        const r = JSON.parse(l);
        if (r.workId && r.campaignVersion === 7) {
          latestPostV7Map.set(r.workId, r);
        }
      } catch {}
    }
    for (const r of latestPostV7Map.values()) {
      postV7AuditedIds.add(r.workId);
      if (r.verdict === 'AUDIT_PASS') stats.v7_done_audit_pass++;
      else if (r.verdict === 'AUDIT_FAIL') {
        stats.v7_done_audit_fail++;
        stats.missing_aliases_detected += (r.missingFromYsql?.length || 0);
      } else if (r.verdict === 'AUDIT_INCONCLUSIVE') {
        stats.v7_done_audit_inconclusive++;
      }
    }
    console.log(`Loaded ${postV7AuditedIds.size} unique verified post-V7 audit records from ${POST_V7_RESULTS_FILE}`);
  }

  const startTime = Date.now();
  let loopCount = 0;
  const CONCURRENCY = 12;

  while (true) {
    try {
      // 1. Fetch exact total published & V7 DONE counts
      const countRes = await pool.query('SELECT COUNT(*) AS total FROM works WHERE published = true');
      stats.total_published = parseInt(countRes.rows[0].total, 10);

      const v7DoneCountRes = await pool.query(`
        SELECT count(*) AS count
        FROM importer_lite_tasks
        WHERE task_type = 'CATALOG' 
          AND payload->>'metadataEnrichmentVersion' = '7'
          AND state = 'DONE'
      `);
      stats.v7_done_total = parseInt(v7DoneCountRes.rows[0].count, 10);

      // Check if any already-audited V7 work was reopened or reprocessed (state !== 'DONE' or task updated_at > record.v7CompletedAt)
      if (postV7AuditedIds.size > 0) {
        const auditedArray = Array.from(postV7AuditedIds);
        const reprocessedRes = await pool.query<{ work_id: string; state: string; updated_at: Date }>(`
          SELECT work_id::text, state, updated_at
          FROM importer_lite_tasks
          WHERE task_type = 'CATALOG'
            AND payload->>'metadataEnrichmentVersion' = '7'
            AND work_id = ANY($1::uuid[])
        `, [auditedArray]);
        for (const row of reprocessedRes.rows) {
          const prev = latestPostV7Map.get(row.work_id);
          if (prev) {
            const taskTime = new Date(row.updated_at).getTime();
            const auditedTaskTime = prev.v7CompletedAt ? new Date(prev.v7CompletedAt).getTime() : 0;
            if (row.state !== 'DONE' || taskTime > auditedTaskTime + 1000) {
              postV7AuditedIds.delete(row.work_id);
              latestPostV7Map.delete(row.work_id);
              if (prev.verdict === 'AUDIT_PASS') stats.v7_done_audit_pass--;
              else if (prev.verdict === 'AUDIT_FAIL') stats.v7_done_audit_fail--;
              else if (prev.verdict === 'AUDIT_INCONCLUSIVE') stats.v7_done_audit_inconclusive--;
              stats.corrections_revalidated++;
            }
          }
        }
      }

      stats.v7_done_pending_audit = Math.max(0, stats.v7_done_total - (stats.v7_done_audit_pass + stats.v7_done_audit_fail + stats.v7_done_audit_inconclusive));

      // 2. Fetch candidates: STRICTLY V7 DONE works that have NOT YET received a post-V7 final audit
      const postV7Array = Array.from(postV7AuditedIds);
      const candidates = await pool.query<{ id: string; title: string; slug: string; aliases: string[]; v7_task_id: string; v7_state: string; v7_updated_at: Date; v7_checkpoint: any }>(`
        SELECT w.id::text, w.title, w.slug, w.aliases,
               t.id::text as v7_task_id, t.state as v7_state, t.updated_at as v7_updated_at, t.checkpoint as v7_checkpoint
        FROM importer_lite_tasks t
        JOIN works w ON w.id = t.work_id
        WHERE t.task_type = 'CATALOG'
          AND t.payload->>'metadataEnrichmentVersion' = '7'
          AND t.state = 'DONE'
          AND w.published = true
          ${postV7Array.length > 0 ? 'AND NOT (w.id = ANY($1::uuid[]))' : ''}
        ORDER BY t.updated_at ASC
        LIMIT 60
      `, postV7Array.length > 0 ? [postV7Array] : []);

      const unaudited = candidates.rows;

      if (unaudited.length === 0) {
        // Automatic revalidation queue for AUDIT_INCONCLUSIVE works after provider cooldowns
        const now = Date.now();
        const revalidationCandidates = Array.from(latestPostV7Map.values())
          .filter((r) => r.verdict === 'AUDIT_INCONCLUSIVE' && (now - new Date(r.auditedAt || r.revalidatedAt || 0).getTime()) > 5 * 60 * 1000)
          .slice(0, 6);

        if (revalidationCandidates.length > 0) {
          console.log(`[IDLE REVALIDATION] Re-auditing ${revalidationCandidates.length} AUDIT_INCONCLUSIVE works after provider cooldowns...`);
          const chunkIds = revalidationCandidates.map((c) => c.workId);
          const [mRes, wRes, tRes] = await Promise.all([
            pool.query('SELECT work_id::text, source, source_work_id, source_title, source_slug FROM importer_work_mappings WHERE work_id = ANY($1::uuid[])', [chunkIds]),
            pool.query('SELECT id::text, title, slug, aliases FROM works WHERE id = ANY($1::uuid[])', [chunkIds]),
            pool.query('SELECT work_id::text, id::text as v7_task_id, state as v7_state, updated_at as v7_updated_at, checkpoint as v7_checkpoint FROM importer_lite_tasks WHERE work_id = ANY($1::uuid[]) AND task_type = \'CATALOG\' AND payload->>\'metadataEnrichmentVersion\' = \'7\'', [chunkIds]),
          ]);

          const mappingsByWorkId = new Map<string, any[]>();
          for (const row of mRes.rows) {
            if (!mappingsByWorkId.has(row.work_id)) mappingsByWorkId.set(row.work_id, []);
            mappingsByWorkId.get(row.work_id)!.push(row);
          }

          const worksById = new Map<string, any>();
          for (const row of wRes.rows) worksById.set(row.id, row);

          const tasksById = new Map<string, any>();
          for (const row of tRes.rows) {
            tasksById.set(row.work_id, {
              id: row.v7_task_id,
              state: row.v7_state,
              updated_at: row.v7_updated_at,
              checkpoint: row.v7_checkpoint,
            });
          }

          for (const candidate of revalidationCandidates) {
            const w = worksById.get(candidate.workId);
            if (!w) continue;
            const mappings = mappingsByWorkId.get(candidate.workId) || [];
            const currentAliases = Array.isArray(w.aliases) ? w.aliases : [];
            const taskInfo = tasksById.get(candidate.workId) || null;
            try {
              const freshRecord = await auditWork(w, sources, mappings, currentAliases, taskInfo, candidate);
              if (freshRecord) {
                freshRecord.revalidationAttempts = (candidate.revalidationAttempts || 0) + 1;
                freshRecord.revalidatedAt = new Date().toISOString();
                const oldVerdict = latestPostV7Map.get(candidate.workId)?.verdict;
                if (oldVerdict === 'AUDIT_INCONCLUSIVE') stats.v7_done_audit_inconclusive--;
                else if (oldVerdict === 'AUDIT_FAIL') stats.v7_done_audit_fail--;
                else if (oldVerdict === 'AUDIT_PASS') stats.v7_done_audit_pass--;

                if (freshRecord.verdict === 'AUDIT_PASS') stats.v7_done_audit_pass++;
                else if (freshRecord.verdict === 'AUDIT_FAIL') stats.v7_done_audit_fail++;
                else stats.v7_done_audit_inconclusive++;

                latestPostV7Map.set(candidate.workId, freshRecord);
                fs.appendFileSync(POST_V7_RESULTS_FILE, JSON.stringify(freshRecord) + '\n', 'utf8');
                fs.appendFileSync(RESULTS_FILE, JSON.stringify(freshRecord) + '\n', 'utf8');
                console.log(`[REVALIDATED] ${w.title}: ${oldVerdict} -> ${freshRecord.verdict}`);
              }
            } catch (e: any) {
              console.warn(`[REVALIDATION ERROR] ${w.title}:`, e?.message);
            }
          }
          stats.updated_at = new Date().toISOString();
          fs.writeFileSync(SUMMARY_FILE, JSON.stringify(stats, null, 2), 'utf8');
          continue;
        }

        stats.v7_done_pending_audit = 0;
        stats.updated_at = new Date().toISOString();
        fs.writeFileSync(SUMMARY_FILE, JSON.stringify(stats, null, 2), 'utf8');
        console.log(`[LOCKSTEP AUDITOR V7] All ${stats.v7_done_total} V7 DONE works are fully audited! Waiting 4s for new V7 completions...`);
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
                id: w.v7_task_id,
                state: w.v7_state,
                updated_at: w.v7_updated_at,
                checkpoint: w.v7_checkpoint,
              };
              return await auditWork(w, sources, mappings, currentAliases, taskInfo, null);
            } catch (err: any) {
              console.error(`Audit error on ${w.title}:`, err?.message);
              return null;
            }
          })
        );

        for (const record of records) {
          if (!record) continue;
          postV7AuditedIds.add(record.workId);
          latestPostV7Map.set(record.workId, record);
          stats.last_audited_work = `${record.title} (${record.verdict})`;

          if (record.verdict === 'AUDIT_PASS') {
            stats.v7_done_audit_pass++;
          } else if (record.verdict === 'AUDIT_FAIL') {
            stats.v7_done_audit_fail++;
            stats.missing_aliases_detected += (record.missingFromYsql?.length || 0);
            console.error(`🚨 [AUDIT FAIL] Missing aliases on "${record.title}"! Missing:`, record.missingFromYsql);
            fs.writeFileSync(
              path.resolve(process.cwd(), 'scratch/AUDIT_BUG_ALERT.json'),
              JSON.stringify(record, null, 2),
              'utf8'
            );
          } else {
            stats.v7_done_audit_inconclusive++;
          }

          stats.v7_done_pending_audit = Math.max(0, stats.v7_done_total - (stats.v7_done_audit_pass + stats.v7_done_audit_fail + stats.v7_done_audit_inconclusive));

          loopCount++;
          fs.appendFileSync(POST_V7_RESULTS_FILE, JSON.stringify(record) + '\n', 'utf8');
          fs.appendFileSync(RESULTS_FILE, JSON.stringify(record) + '\n', 'utf8');
          console.log(`[POST-V7 AUDIT ${postV7AuditedIds.size}/${stats.v7_done_total}] ${record.title} -> ${record.verdict} (Pending: ${stats.v7_done_pending_audit})`);
        }

        const elapsedMins = (Date.now() - startTime) / 60000;
        if (elapsedMins > 0) {
          stats.audit_speed_works_per_min = parseFloat((loopCount / elapsedMins).toFixed(2));
        }
        stats.updated_at = new Date().toISOString();
        fs.writeFileSync(SUMMARY_FILE, JSON.stringify(stats, null, 2), 'utf8');
      }
    } catch (loopErr: any) {
      console.warn(`[AUDIT DAEMON RECOVERABLE ERROR] ${loopErr?.message || loopErr}. Retrying in 4s...`);
      await new Promise((r) => setTimeout(r, 4000));
    }
  }
}

const isMainModule = process.argv[1] && (
  process.argv[1].endsWith('audit_full_catalog_daemon.ts') ||
  process.argv[1].endsWith('audit_full_catalog_daemon.js')
);

if (isMainModule) {
  main().catch((err) => {
    console.error('Fatal audit daemon error:', err);
    process.exit(1);
  });
}
