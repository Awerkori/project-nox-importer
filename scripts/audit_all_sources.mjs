import pg from 'pg';
import dotenv from 'dotenv';
import fs from 'node:fs';
import { SourceRegistry } from '../build/sources/registry.js';
import { HostRateLimiter } from '../build/core/rate-limiter.js';

dotenv.config({ path: '/home/awerkori/.Projects/project-nox-importer/.env' });

const { Client } = pg;
const DB_CONFIG = {
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false }
};

const rateLimiter = new HostRateLimiter(5.0);
const registry = new SourceRegistry(rateLimiter);

async function probeUrl(url, timeoutMs = 6000) {
  if (!url) return { status: 0, error: 'NO_URL' };
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  const t0 = Date.now();
  try {
    const res = await fetch(url, {
      method: 'GET',
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
      },
      signal: controller.signal,
      redirect: 'follow'
    });
    clearTimeout(timer);
    const text = await res.text().catch(() => '');
    const isCloudflare = (res.headers.get('server') || '').toLowerCase().includes('cloudflare') || res.headers.has('cf-ray');
    const isChallenge = text.includes('cf-browser-verification') || text.includes('turnstile') || text.includes('Checking your browser');
    return {
      status: res.status,
      durationMs: Date.now() - t0,
      isCloudflare,
      isChallenge,
      finalUrl: res.url,
      error: null
    };
  } catch (err) {
    clearTimeout(timer);
    return {
      status: 0,
      durationMs: Date.now() - t0,
      isCloudflare: false,
      isChallenge: false,
      error: err.code || err.name || err.message
    };
  }
}

async function main() {
  console.log('Connecting to database...');
  const client = new Client(DB_CONFIG);
  await client.connect();

  console.log('Fetching sources from importer_sources...');
  const dbSourcesRes = await client.query('SELECT * FROM importer_sources ORDER BY id');
  const dbSources = dbSourcesRes.rows;

  console.log('Fetching work counts from importer_work_mappings...');
  const workCountsRes = await client.query(`
    SELECT source, count(*) as work_count
    FROM importer_work_mappings
    GROUP BY source
  `);
  const workCountMap = new Map();
  workCountsRes.rows.forEach(r => workCountMap.set(r.source, parseInt(r.work_count, 10)));

  console.log('Fetching queue stats from importer_queue...');
  const queueStatsRes = await client.query(`
    SELECT source,
           count(*) FILTER (WHERE status = 'QUEUED') as queued_count,
           count(*) FILTER (WHERE status = 'COMPLETED') as completed_count,
           count(*) FILTER (WHERE status = 'FAILED') as failed_count,
           count(*) FILTER (WHERE status = 'PAUSED_BY_STAFF') as paused_staff_count,
           MAX(updated_at) FILTER (WHERE status = 'COMPLETED') as last_success,
           MAX(updated_at) FILTER (WHERE status = 'FAILED') as last_failed_at
    FROM importer_queue
    GROUP BY source
  `);
  const queueStatsMap = new Map();
  queueStatsRes.rows.forEach(r => queueStatsMap.set(r.source, r));

  console.log('Fetching recent error messages from importer_queue for failed jobs...');
  const recentErrorsRes = await client.query(`
    SELECT DISTINCT ON (source) source, last_error, updated_at
    FROM importer_queue
    WHERE status = 'FAILED' AND last_error IS NOT NULL
    ORDER BY source, updated_at DESC
  `);
  const errorMap = new Map();
  recentErrorsRes.rows.forEach(r => errorMap.set(r.source, r.last_error));

  // Get all known sources (from DB and from repo)
  const allAdapters = registry.getAll();
  const repoAdapterMap = new Map();
  allAdapters.forEach(a => repoAdapterMap.set(a.id, a));

  const allSourceIds = Array.from(new Set([
    ...dbSources.map(s => s.id),
    ...allAdapters.map(a => a.id)
  ])).sort();

  console.log(`Auditing ${allSourceIds.length} sources...\n`);

  const auditResults = [];

  for (const sourceId of allSourceIds) {
    const dbSource = dbSources.find(s => s.id === sourceId);
    const adapter = repoAdapterMap.get(sourceId);
    const registeredInImporter = Boolean(dbSource);
    const supportedInRepo = Boolean(adapter);

    const enabled = dbSource?.enabled ?? false;
    const status = dbSource?.status ?? 'NOT_REGISTERED';
    const blockedReason = dbSource?.blocked_reason ?? null;
    const chapterIngestionEnabled = dbSource?.chapter_ingestion_enabled ?? false;
    const catalogDiscoveryEnabled = dbSource?.catalog_discovery_enabled ?? false;

    const eligibleWorks = workCountMap.get(sourceId) || 0;
    const qStats = queueStatsMap.get(sourceId) || {};
    const pendingJobs = parseInt(qStats.queued_count || 0, 10);
    const completedJobs = parseInt(qStats.completed_count || 0, 10);
    const pausedStaffJobs = parseInt(qStats.paused_staff_count || 0, 10);
    const lastSuccess = qStats.last_success ? new Date(qStats.last_success).toISOString() : 'NEVER';
    const lastError = errorMap.get(sourceId) || (dbSource?.blocked_reason ? `BLOCKED: ${dbSource.blocked_reason}` : 'NONE');

    // Probe base URL if adapter exists
    let probe = { status: 0, error: 'NO_ADAPTER' };
    if (adapter && adapter.baseUrl) {
      probe = await probeUrl(adapter.baseUrl, 5000);
    }

    // Determine classification
    let classification = 'OTHER';
    let action = 'NONE';

    if (!registeredInImporter) {
      classification = 'NOT_REGISTERED_IN_IMPORTER';
      action = 'REGISTER_OR_IGNORE';
    } else if (!supportedInRepo) {
      classification = 'UNSUPPORTED_BY_IMPORTER';
      action = 'KEEP_DISABLED_NO_ADAPTER';
    } else if (enabled && status === 'ACTIVE' && chapterIngestionEnabled) {
      classification = 'ACTIVE_PRODUCTION';
      action = 'MAINTAIN_ACTIVE';
    } else if (probe.error === 'ENOTFOUND' || probe.error === 'ECONNREFUSED' || probe.status === 404 || probe.status === 521 || probe.status === 523) {
      classification = 'SOURCE_OFFLINE';
      action = 'KEEP_DISABLED_OFFLINE';
    } else if (probe.isChallenge || probe.status === 403) {
      classification = 'RATE_LIMIT/BLOCKED';
      action = 'KEEP_DISABLED_CLOUDFLARE_BLOCKED';
    } else if (eligibleWorks === 0 && pendingJobs === 0 && pausedStaffJobs === 0) {
      classification = 'NO_ELIGIBLE_WORKS';
      action = 'KEEP_DISABLED_EMPTY';
    } else if (blockedReason === 'EMERGENCY_FREEZE_MIGRATION') {
      // Check if domain is healthy (probe 200 or 301/302)
      if (probe.status >= 200 && probe.status < 400 && !probe.isChallenge) {
        classification = 'DISABLED_RESIDUAL_FLAG';
        action = 'REACTIVATE_PRODUCTION';
      } else {
        classification = 'DISABLED_VALID_REASON';
        action = `KEEP_DISABLED_UPSTREAM_HTTP_${probe.status}_${probe.error || ''}`;
      }
    } else if (status === 'PAUSED' && blockedReason === null) {
      if (probe.status >= 200 && probe.status < 400 && !probe.isChallenge) {
        classification = 'DISABLED_RESIDUAL_FLAG';
        action = 'REACTIVATE_PRODUCTION';
      } else {
        classification = 'DISABLED_VALID_REASON';
        action = `KEEP_DISABLED_PROBE_${probe.status}`;
      }
    } else {
      classification = 'DISABLED_VALID_REASON';
      action = 'KEEP_DISABLED';
    }

    auditResults.push({
      sourceId,
      registeredInImporter: registeredInImporter ? 'YES' : 'NO',
      supportedInRepo: supportedInRepo ? 'YES' : 'NO',
      baseUrl: adapter?.baseUrl || null,
      enabled,
      status,
      blockedReason,
      chapterIngestionEnabled,
      catalogDiscoveryEnabled,
      pendingJobs,
      completedJobs,
      pausedStaffJobs,
      eligibleWorks,
      lastSuccess,
      lastError: (lastError || '').slice(0, 100),
      probeStatus: probe.status,
      probeError: probe.error,
      probeIsChallenge: probe.isChallenge,
      classification,
      action
    });
    console.log(`Audited [${sourceId.padEnd(20)}] -> ${classification.padEnd(25)} (Probe: ${probe.status} ${probe.error || ''}, Works: ${eligibleWorks}, Queued: ${pendingJobs})`);
  }

  await client.end();

  fs.writeFileSync('audit_all_sources_result.json', JSON.stringify(auditResults, null, 2));
  console.log('\nSaved audit_all_sources_result.json');

  console.log('\n======================================================================');
  console.log('📊 AUDIT CLASSIFICATION SUMMARY');
  console.log('======================================================================');
  const classCount = {};
  auditResults.forEach(r => {
    classCount[r.classification] = (classCount[r.classification] || 0) + 1;
  });
  console.table(classCount);
}

main().catch(console.error);
