import fs from 'node:fs';
import { setTimeout as sleep } from 'node:timers/promises';
import dotenv from 'dotenv';
import pg from 'pg';

const envFile = process.env.NOX_AUDIT_ENV_FILE;
if (!envFile) throw new Error('NOX_AUDIT_ENV_FILE is required');
const env = dotenv.parse(fs.readFileSync(envFile));
const minutes = Number(process.argv[2] || 10);
const output = process.argv[3];
if (!Number.isInteger(minutes) || minutes < 1 || minutes > 10) throw new Error('Window must be 1..10 minutes');
const requestedStart = process.env.NOX_AUDIT_START || null;
const requestedEnd = process.env.NOX_AUDIT_END || null;
if (Boolean(requestedStart) !== Boolean(requestedEnd)) throw new Error('NOX_AUDIT_START and NOX_AUDIT_END must be set together');

const siteOrigin = 'https://manga.project-nox-awerkori.workers.dev';
const pool = new pg.Pool({
  host: env.YUGABYTE_HOST,
  port: Number(env.YUGABYTE_PORT || 5433),
  user: env.YUGABYTE_USER,
  password: env.YUGABYTE_PASSWORD,
  database: env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: true, ca: fs.readFileSync(env.YUGABYTE_SSL_CERT) },
  max: 1,
  connectionTimeoutMillis: 5_000,
  query_timeout: 7_000,
  application_name: 'nox-public-flow-audit',
  options: '-c statement_timeout=6000 -c default_transaction_read_only=on',
});

const query = async (sql, params = []) => (await pool.query(sql, params)).rows;
const samples = [];
const observedFeedIds = new Map();
let stage = 'startup';

function numeric(value) {
  return Number.parseInt(String(value || 0), 10) || 0;
}

function quantile(values, p) {
  const sorted = values.filter(Number.isFinite).sort((a, b) => a - b);
  return sorted.length ? sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * p))] : null;
}

async function fetchText(path) {
  const started = performance.now();
  try {
    const response = await fetch(`${siteOrigin}${path}`, {
      headers: { 'User-Agent': 'NoxPublicFlowAudit/1.0' },
      signal: AbortSignal.timeout(12_000),
    });
    const text = await response.text();
    return {
      status: response.status,
      elapsedMs: Math.round(performance.now() - started),
      cacheControl: response.headers.get('cache-control'),
      cacheStatus: response.headers.get('cf-cache-status'),
      age: response.headers.get('age'),
      text,
    };
  } catch (error) {
    return { status: 0, elapsedMs: Math.round(performance.now() - started), error: error?.name || 'FETCH_ERROR', text: '' };
  }
}

async function sampleFeed() {
  const at = new Date().toISOString();
  const nonce = encodeURIComponent(`${Date.now()}-${Math.random().toString(36).slice(2, 8)}`);
  const [feedResult, heartbeatRows] = await Promise.all([
    fetchText(`/api/releases?limit=48&audit=${nonce}`),
    query("SELECT value FROM settings WHERE key = 'importer_heartbeat'"),
  ]);
  let releases = [];
  if (feedResult.status === 200) {
    try {
      const payload = JSON.parse(feedResult.text);
      releases = Array.isArray(payload.releases) ? payload.releases : [];
    } catch {
      releases = [];
    }
  }
  const ids = [];
  for (const release of releases) {
    for (const chapter of release?.chapters || []) {
      if (!chapter?.id) continue;
      ids.push(chapter.id);
      if (!observedFeedIds.has(chapter.id)) observedFeedIds.set(chapter.id, at);
    }
  }
  const rawHeartbeat = heartbeatRows[0]?.value || null;
  let heartbeat = rawHeartbeat;
  if (typeof rawHeartbeat === 'string') {
    try {
      heartbeat = JSON.parse(rawHeartbeat);
    } catch {
      heartbeat = null;
    }
  }
  samples.push({
    at,
    feed: {
      status: feedResult.status,
      elapsedMs: feedResult.elapsedMs,
      cacheControl: feedResult.cacheControl || null,
      cacheStatus: feedResult.cacheStatus || null,
      age: feedResult.age || null,
      works: releases.length,
      distinctChapters: new Set(ids).size,
      error: feedResult.error || null,
    },
    heartbeat: heartbeat && typeof heartbeat === 'object' ? {
      timestamp: heartbeat.timestamp || null,
      state: heartbeat.state || heartbeat.status || null,
      eligibleJobs: numeric(heartbeat.eligibleJobs ?? heartbeat.eligibleCount),
      claimableWorks: numeric(heartbeat.claimableWorks),
      importing: numeric(heartbeat.importing ?? heartbeat.importingCount),
      retry: numeric(heartbeat.retry),
      stagedUnique: numeric(heartbeat.stagedUnique),
      publishableStaged: numeric(heartbeat.publishableStaged),
      waitingPredecessorStaged: numeric(heartbeat.waitingPredecessorStaged),
      effectiveConcurrency: numeric(heartbeat.capacity?.concurrency),
      limitingFactor: heartbeat.capacity?.limitingFactor || null,
      pressureReason: heartbeat.capacity?.pressureReason || null,
      slots: heartbeat.pipelineCapacity?.slots || null,
      claims: heartbeat.pipelineCapacity?.claims || null,
      downloads: heartbeat.pipelineCapacity?.downloads || null,
      buffers: heartbeat.pipelineCapacity?.buffers || null,
    } : null,
  });
}

async function mapConcurrent(items, limit, task) {
  const results = new Array(items.length);
  let cursor = 0;
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (true) {
      const index = cursor++;
      if (index >= items.length) return;
      results[index] = await task(items[index]);
    }
  }));
  return results;
}

try {
  stage = 'schedule';
  let start;
  let end;
  if (requestedStart && requestedEnd) {
    start = new Date(requestedStart);
    end = new Date(requestedEnd);
    if (!Number.isFinite(start.getTime()) || !Number.isFinite(end.getTime()) || end <= start || end - start > 10 * 60_000) {
      throw new Error('Historical window must be valid and no longer than 10 minutes');
    }
    console.log(JSON.stringify({ phase: 'historical', start, end }));
  } else {
    const [{ start: startRaw }] = await query("SELECT date_trunc('minute', clock_timestamp()) + interval '1 minute' AS start");
    start = new Date(startRaw);
    end = new Date(start.getTime() + minutes * 60_000);
    console.log(JSON.stringify({ phase: 'scheduled', start, end, minutes }));
    if (Date.now() < start.getTime()) await sleep(start.getTime() - Date.now());
    let nextSample = Date.now();
    while (Date.now() < end.getTime()) {
      stage = 'sampling';
      await sampleFeed();
      const latest = samples.at(-1);
      console.log(JSON.stringify({
        phase: 'sample',
        at: latest.at,
        feed: latest.feed,
        eligible: latest.heartbeat?.eligibleJobs ?? null,
        importing: latest.heartbeat?.importing ?? null,
        effective: latest.heartbeat?.effectiveConcurrency ?? null,
      }));
      nextSample += 30_000;
      await sleep(Math.max(0, Math.min(nextSample, end.getTime()) - Date.now()));
    }
  }

  stage = 'final_feed_sample';
  await sampleFeed();

  stage = 'canonical';
  const canonical = await query(`
    SELECT c.id::text AS chapter_id, c.work_id::text AS work_id, w.slug AS work_slug,
           c.number::text AS chapter_number, c.published_at, w.published AS work_published,
           e.transition_at, e.bucket_minute
    FROM chapters c
    JOIN works w ON w.id = c.work_id
    LEFT JOIN importer_publication_events e ON e.chapter_id = c.id
    WHERE c.published_at >= $1 AND c.published_at < $2
    ORDER BY c.published_at ASC, c.id ASC
  `, [start, end]);
  const publicCanonical = canonical.filter((row) => row.work_published === true);
  stage = 'job_metrics';
  const jobMetrics = await query(`
    SELECT status, count(*)::int AS jobs, count(DISTINCT chapter_id)::int AS chapter_ids,
           coalesce(sum(page_count), 0)::bigint AS pages, coalesce(sum(total_bytes), 0)::bigint AS bytes
    FROM importer_job_metrics
    WHERE created_at >= $1 AND created_at < $2
    GROUP BY status ORDER BY status
  `, [start, end]);
  const lastHeartbeat = samples.at(-1)?.heartbeat || null;
  const queue = lastHeartbeat ? {
    eligible: lastHeartbeat.eligibleJobs,
    importing: lastHeartbeat.importing,
    retry: lastHeartbeat.retry,
  } : null;
  stage = 'minute_rows';
  const minuteRows = await query(`
    WITH minutes AS (
      SELECT generate_series($1::timestamptz, $2::timestamptz - interval '1 minute', interval '1 minute') AS minute
    ), counts AS (
      SELECT date_trunc('minute', c.published_at) AS minute, count(*)::int AS chapters
      FROM chapters c JOIN works w ON w.id = c.work_id
      WHERE c.published_at >= $1 AND c.published_at < $2 AND w.published IS TRUE
      GROUP BY 1
    )
    SELECT m.minute, coalesce(c.chapters, 0)::int AS chapters FROM minutes m LEFT JOIN counts c USING (minute) ORDER BY m.minute
  `, [start, end]);

  const workMap = new Map();
  for (const row of publicCanonical) {
    const work = workMap.get(row.work_id) || { workId: row.work_id, slug: row.work_slug, chapters: [] };
    work.chapters.push(row);
    workMap.set(row.work_id, work);
  }
  const feedWindow = publicCanonical.map((row) => ({
    chapterId: row.chapter_id,
    workId: row.work_id,
    publishedAt: row.published_at,
    eventMatches: Boolean(row.transition_at) && new Date(row.transition_at).getTime() === new Date(row.published_at).getTime(),
    feedObservedAt: observedFeedIds.get(row.chapter_id) || null,
  }));
  stage = 'reader_checks';
  const readerChecks = await mapConcurrent(publicCanonical, 4, async (row) => {
    const result = await fetchText(`/ler/${row.chapter_id}?audit=${Date.now()}`);
    return {
      chapterId: row.chapter_id,
      status: result.status,
      elapsedMs: result.elapsedMs,
      routeError: /data-error="true"|Internal Error|Internal Server Error/.test(result.text),
    };
  });
  stage = 'work_checks';
  const workChecks = await mapConcurrent([...workMap.values()], 4, async (work) => {
    const result = await fetchText(`/obra/${encodeURIComponent(work.slug)}?audit=${Date.now()}`);
    const lowerText = result.text.toLowerCase();
    return {
      workId: work.workId,
      status: result.status,
      elapsedMs: result.elapsedMs,
      routeError: /data-error="true"|Internal Error|Internal Server Error/.test(result.text),
      allWindowChapterIdsInSsr: work.chapters.every((chapter) => lowerText.includes(String(chapter.chapter_id).toLowerCase())),
    };
  });

  const perWork = [...workMap.values()]
    .map((work) => ({
      workId: work.workId,
      slug: work.slug,
      chapters: work.chapters.length,
      sharePercent: publicCanonical.length ? Math.round((work.chapters.length / publicCanonical.length) * 10_000) / 100 : 0,
      firstPublishedAt: work.chapters[0].published_at,
      lastPublishedAt: work.chapters.at(-1).published_at,
      observedInFeed: work.chapters.filter((chapter) => observedFeedIds.has(chapter.chapter_id)).length,
    }))
    .sort((a, b) => b.chapters - a.chapters || a.workId.localeCompare(b.workId));
  const values = minuteRows.map((row) => numeric(row.chapters));
  const readerSuccess = readerChecks.filter((check) => check.status === 200 && !check.routeError).length;
  const workSuccess = workChecks.filter((check) => check.status === 200 && !check.routeError && check.allWindowChapterIdsInSsr).length;
  const report = {
    observationMode: requestedStart ? 'historical_after_window' : 'live_window',
    start,
    end,
    minutes,
    publicCanonical: {
      chapters: publicCanonical.length,
      perMinute: publicCanonical.length / minutes,
      minuteRows,
      median: quantile(values, 0.5),
      min: Math.min(...values),
      max: Math.max(...values),
      nonPublicCanonical: canonical.length - publicCanonical.length,
      eventMatches: feedWindow.filter((row) => row.eventMatches).length,
    },
    pipeline: { jobMetrics, queue },
    publicFeed: {
      sampled: samples.length,
      windowChapterIdsObserved: feedWindow.filter((row) => row.feedObservedAt).length,
      windowChapterIdsNotObserved: feedWindow.filter((row) => !row.feedObservedAt).length,
      observedChapterIds: observedFeedIds.size,
      samples: samples.map((sample) => ({ at: sample.at, feed: sample.feed })),
      chapters: feedWindow,
    },
    publicAvailability: {
      readers: { checked: readerChecks.length, successful: readerSuccess, failures: readerChecks.filter((check) => check.status !== 200 || check.routeError) },
      works: { checked: workChecks.length, successful: workSuccess, failures: workChecks.filter((check) => check.status !== 200 || check.routeError || !check.allWindowChapterIdsInSsr) },
    },
    distribution: perWork,
    capacitySamples: samples.map((sample) => ({ at: sample.at, heartbeat: sample.heartbeat })),
  };
  if (output) fs.writeFileSync(output, JSON.stringify(report, null, 2), { mode: 0o600 });
  stage = 'complete';
  console.log(JSON.stringify({
    phase: 'complete',
    publicCanonical: report.publicCanonical,
    jobMetrics: report.pipeline.jobMetrics,
    feed: {
      observed: report.publicFeed.windowChapterIdsObserved,
      missing: report.publicFeed.windowChapterIdsNotObserved,
    },
    reader: { checked: readerChecks.length, successful: readerSuccess },
    works: { checked: workChecks.length, successful: workSuccess },
    distribution: perWork,
  }));
} catch (error) {
  console.log(JSON.stringify({ phase: 'error', stage, code: error?.code || error?.name || 'AUDIT_ERROR' }));
  process.exitCode = 1;
} finally {
  await pool.end();
}
