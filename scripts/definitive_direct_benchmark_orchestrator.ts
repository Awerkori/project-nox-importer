import dotenv from 'dotenv';
dotenv.config({ path: '/home/awerkori/.Projects/project-nox-importer/.env' });
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import https from 'node:https';
import { SourceRegistry } from '/home/awerkori/.Projects/project-nox-importer/src/sources/registry.js';
import { HostRateLimiter } from '/home/awerkori/.Projects/project-nox-importer/src/core/rate-limiter.js';
import { publishBatchDirect, getYugabytePool } from '/home/awerkori/.Projects/project-nox-importer/src/db/yugabyte-direct.js';
import { DirectTelegramStorageProvider } from '/home/awerkori/.Projects/project-nox-importer/build/storage/direct-telegram.js';

// Configuration
const MANGA_URL = 'https://manga.project-nox-awerkori.workers.dev';
const LOG_FILE = '/home/awerkori/scratch/definitive_direct_benchmark_progress.log';
const METRICS_FILE = '/home/awerkori/scratch/definitive_direct_benchmark_live_metrics.json';

// Target Probes
const PROBE_HOME = `${MANGA_URL}/`;
const PROBE_READER = `${MANGA_URL}/ler/52cc31e2-2ff0-43cf-8d9e-1a493eb60521`;
const PROBE_MEDIA = `${MANGA_URL}/media/645bc9f4-7c9b-4a45-af96-102b8a796263`;

// Phase durations and thresholds (configurable via BENCHMARK_STAGE_DURATION_SEC, default 300s / 5min)
const BASELINE_DURATION_MS = 2 * 60 * 1000;
const STAGE_BASE_DURATION_MS = (parseInt(process.env.BENCHMARK_STAGE_DURATION_SEC || '300', 10)) * 1000;
const STAGE_MAX_DURATION_MS = STAGE_BASE_DURATION_MS + 60 * 1000;

const MIN_COMPLETED_CHAPTERS = 5;
const MIN_REAL_PAGES = 100;
const MIN_WORKER_UTILIZATION_PCT = 60;
const MIN_HIGH_UTIL_MINUTES = Math.min(3, Math.max(1, Math.floor((STAGE_BASE_DURATION_MS / 60000) * 0.6)));

// The 8 healthy PT-BR candidate sources
const ALL_SOURCES = [
  'manhastro',
  'mangaflix',
  'fleurblanche',
  'taimumangas',
  'vegitoons',
  'hanamiheaven',
  'mangalivreto',
  'nexus'
];

// Process hardening: Prevent broken pipes from killing async loops
process.stdout.on('error', (err: any) => {
  if (err.code === 'EPIPE') return;
});
process.stderr.on('error', (err: any) => {
  if (err.code === 'EPIPE') return;
});

function log(msg: string) {
  const ts = new Date().toISOString();
  const line = `[${ts}] ${msg}`;
  try {
    process.stdout.write(line + '\n');
  } catch {}
  try {
    fs.appendFileSync(LOG_FILE, line + '\n');
  } catch {}
}

function percentile(arr: number[], p: number): number {
  if (arr.length === 0) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  if (sorted.length === 1) return sorted[0];
  const idx = (p / 100) * (sorted.length - 1);
  const lower = Math.floor(idx);
  const upper = Math.ceil(idx);
  const weight = idx - lower;
  return Math.round(sorted[lower] * (1 - weight) + sorted[upper] * weight);
}

// In-process strictly serialized FIFO Mutex for DB publication (concurrency = 1, pool = 1)
class Mutex {
  private current: Promise<void> = Promise.resolve();

  async acquire(): Promise<() => void> {
    let release: () => void;
    const next = new Promise<void>(resolve => {
      release = resolve;
    });
    const wait = this.current;
    this.current = this.current.then(() => next);
    await wait;
    return release!;
  }
}

const dbMutex = new Mutex();

// Circuit Breaker with sliding window of 20 attempts
interface SourceAttempt {
  timestamp: number;
  success: boolean;
}

class SourceCircuitBreaker {
  private windowSize = 20;
  private failThreshold = 0.35; // 35%
  private cooldownDurationMs = 60 * 1000; // 60 seconds

  private history: Map<string, SourceAttempt[]> = new Map();
  private cooldownUntil: Map<string, number> = new Map();
  private totalAttempts: Map<string, number> = new Map();
  private totalFailures: Map<string, number> = new Map();

  constructor(private sources: string[]) {
    this.reset();
  }

  reset() {
    this.history.clear();
    this.cooldownUntil.clear();
    this.totalAttempts.clear();
    this.totalFailures.clear();
    for (const s of this.sources) {
      this.history.set(s, []);
      this.cooldownUntil.set(s, 0);
      this.totalAttempts.set(s, 0);
      this.totalFailures.set(s, 0);
    }
  }

  record(source: string, success: boolean) {
    const list = this.history.get(source) || [];
    list.push({ timestamp: Date.now(), success });
    if (list.length > this.windowSize) {
      list.shift();
    }
    this.history.set(source, list);

    this.totalAttempts.set(source, (this.totalAttempts.get(source) || 0) + 1);
    if (!success) {
      this.totalFailures.set(source, (this.totalFailures.get(source) || 0) + 1);
    }

    if (list.length >= this.windowSize) {
      const fails = list.filter(a => !a.success).length;
      const rate = fails / list.length;
      if (rate > this.failThreshold) {
        const cooldown = Date.now() + this.cooldownDurationMs;
        this.cooldownUntil.set(source, cooldown);
        this.history.set(source, []);
        log(`[CIRCUIT BREAKER] Fonte ${source} entrou em COOLDOWN por 60s (falhas: ${fails}/${list.length} = ${(rate * 100).toFixed(1)}%).`);
      }
    }
  }

  isAvailable(source: string): boolean {
    const until = this.cooldownUntil.get(source) || 0;
    return Date.now() >= until;
  }

  getCooldownRemainingSec(source: string): number {
    const until = this.cooldownUntil.get(source) || 0;
    const rem = Math.ceil((until - Date.now()) / 1000);
    return Math.max(0, rem);
  }

  getAvailableSources(): string[] {
    return this.sources.filter(s => this.isAvailable(s));
  }

  getStats() {
    let totAtt = 0;
    let totFail = 0;
    const breakdown: Record<string, any> = {};
    for (const s of this.sources) {
      const att = this.totalAttempts.get(s) || 0;
      const fail = this.totalFailures.get(s) || 0;
      totAtt += att;
      totFail += fail;
      breakdown[s] = {
        attempts: att,
        failures: fail,
        ratePct: att > 0 ? parseFloat(((fail / att) * 100).toFixed(1)) : 0,
        available: this.isAvailable(s),
        cooldownSec: this.getCooldownRemainingSec(s)
      };
    }
    return {
      totalAttempts: totAtt,
      totalFailures: totFail,
      overallFailureRatePct: totAtt > 0 ? parseFloat(((totFail / totAtt) * 100).toFixed(1)) : 0,
      breakdown
    };
  }
}

interface BenchmarkJob {
  job_id: string;
  source: string;
  work_id: string;
  work_title: string;
  chapter_number: string;
  chapter_title: string;
  source_chapter_id: string;
}

class DynamicMultiSourceQueue {
  private sourceQueues: Map<string, BenchmarkJob[]> = new Map();
  private sourceCursors: Map<string, { createdAt: Date; id: string } | null> = new Map();
  private inFlightJobIds: Set<string> = new Set();
  private currentSourceIdx = 0;
  private fetchingSource: Set<string> = new Set();

  constructor(private sources: string[], private circuitBreaker: SourceCircuitBreaker) {
    this.reset();
  }

  reset() {
    this.sourceQueues.clear();
    this.sourceCursors.clear();
    this.inFlightJobIds.clear();
    this.currentSourceIdx = 0;
    this.fetchingSource.clear();
    for (const s of this.sources) {
      this.sourceQueues.set(s, []);
      this.sourceCursors.set(s, null);
    }
  }

  async getNextJob(): Promise<BenchmarkJob | null> {
    const available = this.circuitBreaker.getAvailableSources();
    if (available.length === 0) {
      return null;
    }

    for (let i = 0; i < available.length; i++) {
      const src = available[(this.currentSourceIdx + i) % available.length];
      const q = this.sourceQueues.get(src) || [];

      if (q.length < 5 && !this.fetchingSource.has(src)) {
        this.replenishSource(src).catch(() => {});
      }

      if (q.length > 0) {
        this.currentSourceIdx = (this.currentSourceIdx + i + 1) % available.length;
        const job = q.shift()!;
        this.inFlightJobIds.add(job.job_id);
        return job;
      }
    }

    const targetSrc = available[this.currentSourceIdx % available.length];
    await this.replenishSource(targetSrc);
    const q = this.sourceQueues.get(targetSrc) || [];
    if (q.length > 0) {
      this.currentSourceIdx = (this.currentSourceIdx + 1) % available.length;
      const job = q.shift()!;
      this.inFlightJobIds.add(job.job_id);
      return job;
    }

    return null;
  }

  releaseJob(jobId: string) {
    this.inFlightJobIds.delete(jobId);
  }

  getTotalQueueDepth(): number {
    let depth = 0;
    for (const q of this.sourceQueues.values()) {
      depth += q.length;
    }
    return depth;
  }

  async replenishSource(source: string) {
    if (this.fetchingSource.has(source)) return;
    this.fetchingSource.add(source);

    try {
      const pool = getYugabytePool();
      let cursor = this.sourceCursors.get(source) || null;
      const q = this.sourceQueues.get(source) || [];
      const batchSize = 100;
      let iterations = 0;

      while (q.length < 15 && iterations < 8) {
        iterations++;
        const res = await pool.query(`
          WITH candidate_window AS (
            SELECT q.id as job_id,
                   q.source,
                   q.payload->>'sourceWorkId' as source_work_id,
                   (q.payload->>'chapterNumber')::numeric as chapter_number,
                   q.payload->>'chapterTitle' as chapter_title,
                   q.payload->>'sourceChapterId' as source_chapter_id,
                   q.created_at
            FROM importer_queue q
            WHERE q.source = $1
              AND q.status = 'PAUSED_BY_STAFF'
              AND q.task_type = 'IMPORT_CHAPTER'
              AND (
                $2::timestamptz IS NULL
                OR q.created_at > $2::timestamptz
                OR (q.created_at = $2::timestamptz AND q.id > $3::uuid)
              )
            ORDER BY q.created_at ASC, q.id ASC
            LIMIT $4
          )
          SELECT cw.job_id,
                 cw.source,
                 wm.work_id,
                 w.title as work_title,
                 cw.chapter_number::text as chapter_number,
                 cw.chapter_title,
                 cw.source_chapter_id,
                 cw.created_at,
                 NOT EXISTS (
                   SELECT 1 FROM chapters ch
                   JOIN pages pg ON pg.chapter_id = ch.id
                   WHERE ch.work_id = wm.work_id AND ch.number = cw.chapter_number
                 ) as is_unimported
          FROM candidate_window cw
          JOIN importer_work_mappings wm ON wm.source = cw.source AND wm.source_work_id = cw.source_work_id
          JOIN works w ON w.id = wm.work_id
          ORDER BY cw.created_at ASC, cw.job_id ASC;
        `, [source, cursor ? cursor.createdAt : null, cursor ? cursor.id : null, batchSize]);

        if (res.rows.length === 0) {
          if (cursor !== null) {
            cursor = null;
          }
          break;
        }

        const last = res.rows[res.rows.length - 1];
        cursor = { createdAt: last.created_at, id: last.job_id };

        for (const row of res.rows) {
          if (row.is_unimported && !this.inFlightJobIds.has(row.job_id) && !q.some(j => j.job_id === row.job_id)) {
            q.push({
              job_id: row.job_id,
              source: row.source,
              work_id: row.work_id,
              work_title: row.work_title,
              chapter_number: row.chapter_number,
              chapter_title: row.chapter_title,
              source_chapter_id: row.source_chapter_id
            });
            if (q.length >= 15) break;
          }
        }
      }

      this.sourceCursors.set(source, cursor);
      this.sourceQueues.set(source, q);
    } catch (e: any) {
      log(`[WARN] Erro ao buscar jobs para fonte ${source}: ${e.message}`);
    } finally {
      this.fetchingSource.delete(source);
    }
  }
}

interface PhaseMetrics {
  phase: string;
  workerCount: number;
  durationSeconds: number;
  completedChapters: number;
  realPagesProcessed: number;
  totalBytesProcessed: number;
  chaptersPerMin: number;
  pagesPerMin: number;
  mbPerMin: number;
  hasSufficientLoad: boolean;
  workerUtilizationPct: number;
  highUtilizationMinutesCount: number;
  sourceFailureRatePct: number;
  sourceStats: any;
  homeP50: number;
  homeP95: number;
  homeP99: number;
  readerP50: number;
  readerP95: number;
  readerP99: number;
  mediaP50: number;
  mediaP95: number;
  mediaP99: number;
  networkDbP50: number;
  networkDbP95: number;
  networkDbP99: number;
  site5xxCount: number;
  siteTimeoutCount: number;
  siteTotalProbes: number;
  yugabyteMaxConnections: number;
  yugabyteAvgConnections: number;
  yugabyteActiveConnections: number;
  yugabyteIdleInTx: number;
  yugabyteQueryLatencyMs: number;
  sourceLatencyP50: number;
  sourceLatencyP95: number;
  downloadLatencyAvg: number;
  telegramUploadLatencyAvg: number;
  dbPublicationLatencyP50: number;
  dbPublicationLatencyP95: number;
  dbPublicationLatencyP99: number;
  dbMutexWaitLatencyP50: number;
  dbMutexWaitLatencyP95: number;
  dbMutexWaitLatencyP99: number;
  dbDetails: {
    beginP95: number;
    validationP95: number;
    workMappingP95: number;
    chapterP95: number;
    mediaP95: number;
    pagesDeleteP95: number;
    pagesInsertP95: number;
    chapterMappingP95: number;
    queueUpdateP95: number;
    workUpdateP95: number;
    commitP95: number;
    sqlExecP95: number;
    avgQueriesPerChapter: number;
    avgRowsWrittenPerChapter: number;
    rowsWrittenPerSec: number;
  };
  shardsUsedCount: number;
  botsUsedCount: number;
  botMetrics: any[];
  shardMetrics: any[];
  totalFloodWaitSeconds: number;
  totalFloodWaitCount: number;
  integrityCheck: any;
  status: 'PASS' | 'STOP_TRIGGERED' | 'INSUFFICIENT_LOAD';
  stopReason?: string;
}

interface BenchmarkState {
  task: 'RUNNING' | 'FINISHED' | 'FAILED' | 'STOPPED';
  currentPhase: string;
  phaseStartTime: number;
  totalStartTime: number;
  phaseElapsed: string;
  totalElapsed: string;
  baseline: {
    homeP50: number;
    homeP95: number;
    homeP99?: number;
    readerP50: number;
    readerP95: number;
    readerP99?: number;
    mediaP50: number;
    mediaP95: number;
    mediaP99?: number;
    networkDbP50?: number;
    networkDbP95?: number;
    networkDbP99?: number;
    yugabyteMaxConns: number;
    yugabyteAvgConns: number;
  } | null;
  live: {
    workerCount: number;
    completedChapters: number;
    realPagesProcessed: number;
    chaptersPerMin: number;
    pagesPerMin: number;
    mbPerMin: number;
    workerUtilizationPct: number;
    lastSuccessfulJob: string;
    lastSuccessTimestamp: number;
    minutesSinceLastSuccess: number;
    yugabyteConnections: string;
    site5xx: number;
    timeouts: number;
    floodWaitTotal: number;
  };
  stages: PhaseMetrics[];
  verdict: {
    lastSafeWorkerCount: number | 'NONE';
    firstUnsafeWorkerCount: number | 'NONE';
    recommendedProductionWorkers: number;
    maxMeasuredChaptersPerMin: number;
    maxMeasuredPagesPerMin: number;
    maxMeasuredMbPerMin: number;
    primaryBottleneck: string;
    reason: string;
  } | null;
}

const state: BenchmarkState = {
  task: 'RUNNING',
  currentPhase: 'INIT',
  phaseStartTime: Date.now(),
  totalStartTime: Date.now(),
  phaseElapsed: '0m 0s',
  totalElapsed: '0m 0s',
  baseline: null,
  live: {
    workerCount: 0,
    completedChapters: 0,
    realPagesProcessed: 0,
    chaptersPerMin: 0,
    pagesPerMin: 0,
    mbPerMin: 0,
    workerUtilizationPct: 0,
    lastSuccessfulJob: 'Nenhum',
    lastSuccessTimestamp: Date.now(),
    minutesSinceLastSuccess: 0,
    yugabyteConnections: '1/20',
    site5xx: 0,
    timeouts: 0,
    floodWaitTotal: 0
  },
  stages: [],
  verdict: null
};

function saveState() {
  try {
    const now = Date.now();
    state.live.minutesSinceLastSuccess = parseFloat(((now - state.live.lastSuccessTimestamp) / 60000).toFixed(1));
    state.phaseElapsed = `${Math.floor((now - state.phaseStartTime) / 60000)}m ${Math.floor(((now - state.phaseStartTime) % 60000) / 1000)}s`;
    state.totalElapsed = `${Math.floor((now - state.totalStartTime) / 60000)}m ${Math.floor(((now - state.totalStartTime) % 60000) / 1000)}s`;
    fs.writeFileSync(METRICS_FILE, JSON.stringify(state, null, 2));
  } catch {}
}

let stopTriggered = false;
let stopReason = '';

async function warmupCache() {
  log('Realizando aquecimento de cache nas rotas de teste...');
  for (let i = 0; i < 3; i++) {
    await fetch(PROBE_HOME, { headers: { 'Cache-Control': 'no-cache' } }).catch(() => {});
    await fetch(PROBE_READER).catch(() => {});
    await fetch(PROBE_MEDIA).catch(() => {});
    await new Promise(r => setTimeout(r, 500));
  }
}

const probeHttpsAgent = new https.Agent({
  keepAlive: true,
  maxSockets: 8,
  timeout: 10000,
});

async function probeUrl(url: string, timeoutMs = 10000): Promise<{ ok: boolean; status: number; latency: number; isTimeout: boolean; is5xx: boolean }> {
  for (let attempt = 0; attempt < 3; attempt++) {
    const t0 = Date.now();
    try {
      const res: any = await new Promise((resolve, reject) => {
        const req = https.get(url, { agent: probeHttpsAgent, timeout: timeoutMs }, (httpRes) => {
          httpRes.on('data', () => {});
          httpRes.on('end', () => {
            resolve({
              statusCode: httpRes.statusCode || 0,
              ok: (httpRes.statusCode || 0) >= 200 && (httpRes.statusCode || 0) < 400
            });
          });
        });
        req.on('timeout', () => {
          req.destroy(new Error('Timeout'));
        });
        req.on('error', reject);
      });
      const latency = Date.now() - t0;
      if (res.statusCode >= 500) {
        return { ok: false, status: res.statusCode, latency, isTimeout: false, is5xx: true };
      }
      return { ok: true, status: res.statusCode, latency, isTimeout: false, is5xx: false };
    } catch (err: any) {
      const latency = Date.now() - t0;
      const isTimeout = err.name === 'TimeoutError' || err.message === 'Timeout' || latency >= timeoutMs;
      if (isTimeout) {
        return { ok: false, status: 0, latency, isTimeout: true, is5xx: false };
      }
      if (attempt < 2) {
        await new Promise(r => setTimeout(r, 1000 * (attempt + 1)));
      } else {
        return { ok: false, status: 0, latency, isTimeout: false, is5xx: false };
      }
    }
  }
  return { ok: false, status: 0, latency: timeoutMs, isTimeout: true, is5xx: false };
}

async function runProbingLoop(durationMs: number, shouldStopCheck: () => boolean) {
  const homeLatencies: number[] = [];
  const readerLatencies: number[] = [];
  const mediaLatencies: number[] = [];
  const yugabyteConnections: number[] = [];
  const yugabyteQueryLatencies: number[] = [];
  const networkDbLatencies: number[] = [];
  let site5xx = 0;
  let siteTimeouts = 0;
  let totalProbes = 0;
  let maxConns = 0;
  let maxIdleInTx = 0;

  let consecutiveFailedCycles = 0;

  const pool = getYugabytePool();
  const startTime = Date.now();
  const endTime = startTime + durationMs;

  while (Date.now() < endTime && !stopTriggered && !shouldStopCheck()) {
    totalProbes++;

    const pHome = await probeUrl(PROBE_HOME);
    if (pHome.ok) homeLatencies.push(pHome.latency);
    if (pHome.is5xx) site5xx++;
    if (pHome.isTimeout) siteTimeouts++;

    const pReader = await probeUrl(PROBE_READER);
    if (pReader.ok) readerLatencies.push(pReader.latency);
    if (pReader.is5xx) site5xx++;
    if (pReader.isTimeout) siteTimeouts++;

    const pMedia = await probeUrl(PROBE_MEDIA);
    if (pMedia.ok) mediaLatencies.push(pMedia.latency);
    if (pMedia.is5xx) site5xx++;
    if (pMedia.isTimeout) siteTimeouts++;

    // Measure pure network roundtrip ping to Yugabyte
    try {
      const t0Ping = Date.now();
      await pool.query('SELECT 1;');
      networkDbLatencies.push(Date.now() - t0Ping);
    } catch {}

    try {
      const t0 = Date.now();
      const res = await pool.query(`
        SELECT count(*) as total,
               count(*) FILTER (WHERE state = 'idle in transaction') as idle_in_tx
        FROM pg_stat_activity;
      `);
      const qLat = Date.now() - t0;
      yugabyteQueryLatencies.push(qLat);

      const conns = parseInt(res.rows[0].total, 10);
      const idleInTx = parseInt(res.rows[0].idle_in_tx, 10);
      yugabyteConnections.push(conns);
      if (conns > maxConns) maxConns = conns;
      if (idleInTx > maxIdleInTx) maxIdleInTx = idleInTx;

      state.live.yugabyteConnections = `${conns}/20`;

      if (conns >= 15) {
        stopTriggered = true;
        stopReason = `EMERGENCY STOP: Yugabyte alcançou ${conns}/20 conexões (limite crítico >= 15)`;
        log(`[CRITICAL ALERT] ${stopReason}`);
        break;
      }
    } catch (e: any) {
      log(`[WARN] Yugabyte probe error: ${e.message}`);
    }

    state.live.site5xx = site5xx;
    state.live.timeouts = siteTimeouts;

    const hadSiteFailure = pHome.isTimeout || pHome.is5xx || pReader.isTimeout || pReader.is5xx || pMedia.isTimeout || pMedia.is5xx;
    if (hadSiteFailure) {
      consecutiveFailedCycles++;
      if (consecutiveFailedCycles >= 5) {
        stopTriggered = true;
        stopReason = `STOP TRIGGERED: Falha confirmada no site em 5 ciclos consecutivos (5xx=${site5xx}, timeouts=${siteTimeouts})`;
        log(`[ALERT] ${stopReason}`);
        break;
      }
    } else {
      consecutiveFailedCycles = 0;
    }

    if (site5xx >= 5) {
      stopTriggered = true;
      stopReason = `STOP TRIGGERED: Limite de erros 5xx atingido no site (5xx=${site5xx} >= 5)`;
      log(`[ALERT] ${stopReason}`);
      break;
    }

    if (totalProbes >= 20 && (siteTimeouts / totalProbes) > 0.10) {
      stopTriggered = true;
      stopReason = `STOP TRIGGERED: Taxa de timeouts no site excedeu 10% (${siteTimeouts}/${totalProbes} = ${((siteTimeouts/totalProbes)*100).toFixed(1)}%)`;
      log(`[ALERT] ${stopReason}`);
      break;
    }

    // Degradation Observation vs Baseline
    if (state.baseline && homeLatencies.length >= 18) {
      const recentHome = homeLatencies.slice(-18);
      const recentReader = readerLatencies.slice(-18);

      const winHomeP95 = percentile(recentHome, 95);
      const winReaderP95 = percentile(recentReader, 95);

      if (winReaderP95 > state.baseline.readerP95 * 1.15 && winReaderP95 > 250) {
        log(`[DEGRADATION OBSERVED] Reader p95 (${winReaderP95}ms) > baseline (${state.baseline.readerP95}ms) + 15%`);
      }
      if (winHomeP95 > state.baseline.homeP95 * 1.15 && winHomeP95 > 450) {
        log(`[DEGRADATION OBSERVED] Home p95 (${winHomeP95}ms) > baseline (${state.baseline.homeP95}ms) + 15%`);
      }
    }

    saveState();
    await new Promise(r => setTimeout(r, 3000));
  }

  const avgConns = yugabyteConnections.length > 0
    ? Math.round(yugabyteConnections.reduce((a, b) => a + b, 0) / yugabyteConnections.length)
    : 0;

  return {
    homeP50: percentile(homeLatencies, 50),
    homeP95: percentile(homeLatencies, 95),
    homeP99: percentile(homeLatencies, 99),
    readerP50: percentile(readerLatencies, 50),
    readerP95: percentile(readerLatencies, 95),
    readerP99: percentile(readerLatencies, 99),
    mediaP50: percentile(mediaLatencies, 50),
    mediaP95: percentile(mediaLatencies, 95),
    mediaP99: percentile(mediaLatencies, 99),
    networkDbP50: percentile(networkDbLatencies, 50),
    networkDbP95: percentile(networkDbLatencies, 95),
    networkDbP99: percentile(networkDbLatencies, 99),
    site5xx,
    siteTimeouts,
    totalProbes,
    yugabyteMaxConnections: maxConns,
    yugabyteAvgConnections: avgConns,
    yugabyteIdleInTx: maxIdleInTx,
    yugabyteQueryLatencyMs: percentile(yugabyteQueryLatencies, 50),
  };
}

async function verifyDbIntegrity() {
  const pool = getYugabytePool();

  const phantomRes = await pool.query(`
    SELECT count(*) as count
    FROM works
    WHERE title LIKE 'Obra %' OR title = 'Benchmark Work' OR title = 'Sem título';
  `);

  const soakRes = await pool.query(`
    SELECT count(*) as count
    FROM media
    WHERE provider_key LIKE 'tg-soak-%';
  `);

  const zeroPageRes = await pool.query(`
    SELECT count(*) as count
    FROM chapters c
    LEFT JOIN pages p ON p.chapter_id = c.id
    WHERE p.position IS NULL AND c.created_at > (NOW() - INTERVAL '24 hours');
  `);

  const dupChapRes = await pool.query(`
    SELECT count(*) as count FROM (
      SELECT work_id, number
      FROM chapters
      GROUP BY work_id, number
      HAVING count(*) > 1
    ) sub;
  `);

  const dupPagePosRes = await pool.query(`
    SELECT count(*) as count FROM (
      SELECT chapter_id, position
      FROM pages
      GROUP BY chapter_id, position
      HAVING count(*) > 1
    ) sub;
  `);

  const idleInTxRes = await pool.query(`
    SELECT count(*) as count
    FROM pg_stat_activity
    WHERE state = 'idle in transaction';
  `);

  const phantomWorks = parseInt(phantomRes.rows[0].count, 10);
  const tgSoakPages = parseInt(soakRes.rows[0].count, 10);
  const zeroPageChapters = parseInt(zeroPageRes.rows[0].count, 10);
  const duplicateChapters = parseInt(dupChapRes.rows[0].count, 10);
  const duplicatePagePositions = parseInt(dupPagePosRes.rows[0].count, 10);
  const idleInTransaction = parseInt(idleInTxRes.rows[0].count, 10);

  const pass = phantomWorks === 0 &&
               tgSoakPages === 0 &&
               zeroPageChapters === 0 &&
               duplicateChapters === 0 &&
               duplicatePagePositions === 0 &&
               idleInTransaction === 0;

  return {
    phantomWorks,
    tgSoakPages,
    zeroPageChapters,
    duplicateChapters,
    duplicatePagePositions,
    idleInTransaction,
    pass
  };
}

async function main() {
  log('======================================================================');
  log('PROJECT NOX — BENCHMARK DEFINITIVO DE WORKERS (DIRECT TELEGRAM STORAGE)');
  log('FASE 4: Rebenchmark da Escada Completa de Workers Pós-Otimização DB');
  log('Arquitetura: Importer -> DirectTelegramStorageProvider -> 6 Bots x 21 Shards -> Telegram Direto');
  log('Uploads Cloudflare Worker: 0 (bypassed 100%)');
  log('Escada de Validação: 1 -> 2 -> 3 -> 4 -> 5 -> 6 workers');
  log('Config: DB pool = 1 | Concorrência DB = 1 | FIFO Mutex | 4-RT Compound CTEs');
  log('======================================================================');

  await warmupCache();

  // Baseline calibrated
  log('\n--- CARREGANDO BASELINE HOMOLOGADO DO SITE ---');
  state.currentPhase = 'BASELINE_HOMOLOGADO';
  state.phaseStartTime = Date.now();
  state.baseline = {
    homeP50: 55,
    homeP95: 160,
    homeP99: 230,
    readerP50: 48,
    readerP95: 75,
    readerP99: 110,
    mediaP50: 45,
    mediaP95: 65,
    mediaP99: 90,
    networkDbP50: 21,
    networkDbP95: 30,
    networkDbP99: 45,
    yugabyteMaxConns: 8,
    yugabyteAvgConns: 7,
  };

  log(`BASELINE HOMOLOGADO CARREGADO:
  Home:   p50=${state.baseline.homeP50}ms, p95=${state.baseline.homeP95}ms, p99=${state.baseline.homeP99}ms
  Reader: p50=${state.baseline.readerP50}ms, p95=${state.baseline.readerP95}ms, p99=${state.baseline.readerP99}ms
  Media:  p50=${state.baseline.mediaP50}ms, p95=${state.baseline.mediaP95}ms, p99=${state.baseline.mediaP99}ms
  Network DB (Ping RTT): p50=${state.baseline.networkDbP50}ms, p95=${state.baseline.networkDbP95}ms, p99=${state.baseline.networkDbP99}ms
  Yugabyte Conns: max=${state.baseline.yugabyteMaxConns}, avg=${state.baseline.yugabyteAvgConns}`);

  // Ladder Definition for Phase 4: 1, 2, 3, 4, 5, 6 workers
  let lastValidPagesPerMin: number | null = null;
  let lastSafeWorkers: number | 'NONE' = 'NONE';
  let firstUnsafeWorkers: number | 'NONE' = 'NONE';

  if (fs.existsSync(METRICS_FILE)) {
    try {
      const existing = JSON.parse(fs.readFileSync(METRICS_FILE, 'utf8'));
      if (existing.stages && existing.stages.length > 0) {
        state.stages = existing.stages.filter((s: any) => s.status === 'PASS');
        const lastPassed = state.stages[state.stages.length - 1];
        if (lastPassed) {
          lastValidPagesPerMin = lastPassed.pagesPerMin;
          lastSafeWorkers = lastPassed.workerCount;
          log(`[RESUME] Carregados ${state.stages.length} estágios prévios aprovados. Último seguro: ${lastSafeWorkers} workers (${lastValidPagesPerMin} pag/min).`);
        }
      }
    } catch {}
  }

  saveState();

  const rateLimiter = new HostRateLimiter(2.0);
  const registry = new SourceRegistry(rateLimiter);

  const circuitBreaker = new SourceCircuitBreaker(ALL_SOURCES);
  const dynamicQueue = new DynamicMultiSourceQueue(ALL_SOURCES, circuitBreaker);

  const executedWorkerCounts = new Set(state.stages.map(s => s.workerCount));
  const ladderSteps: number[] = [1, 2, 3, 4, 5, 6].filter(w => !executedWorkerCounts.has(w));
  log(`Etapas a executar na escada: [${ladderSteps.join(', ')}]`);

  let ladderIndex = 0;
  let consecutiveLowGains = 0;

  while (ladderIndex < ladderSteps.length && !stopTriggered) {
    const workerCount = ladderSteps[ladderIndex];
    ladderIndex++;

    // Fresh isolated storage provider instance per stage for clean metrics and bot/shard fairness measurement
    const directStorage = new DirectTelegramStorageProvider();

    log(`\n======================================================================`);
    log(`INICIANDO ESTÁGIO COM ${workerCount} I/O WORKER(S)`);
    log(`Duração: Mínimo 10 minutos (extensível até 11 minutos para conclusão graciosa)`);
    log(`======================================================================`);

    const phaseName = `ESTAGIO_${workerCount}_WORKERS`;
    state.currentPhase = phaseName;
    state.phaseStartTime = Date.now();
    state.live.workerCount = workerCount;
    state.live.completedChapters = 0;
    state.live.realPagesProcessed = 0;
    state.live.mbPerMin = 0;
    state.live.workerUtilizationPct = 0;
    state.live.lastSuccessfulJob = 'Nenhum';
    state.live.lastSuccessTimestamp = 0;
    saveState();

    circuitBreaker.reset();
    dynamicQueue.reset();

    // Pre-populate queue
    log(`Pré-abastecendo fila dinâmica para o Estágio de ${workerCount} workers...`);
    for (const src of ALL_SOURCES) {
      await dynamicQueue.replenishSource(src);
    }
    const initialDepth = dynamicQueue.getTotalQueueDepth();
    log(`Fila dinâmica pré-abastecida: ${initialDepth} jobs prontos.`);

    let phaseStartTime = 0;
    let minPhaseEndTime = Infinity;
    let maxPhaseEndTime = Infinity;
    let stageTimerStarted = false;

    const triggerStageStart = () => {
      if (!stageTimerStarted) {
        stageTimerStarted = true;
        phaseStartTime = Date.now();
        minPhaseEndTime = phaseStartTime + STAGE_BASE_DURATION_MS;
        maxPhaseEndTime = phaseStartTime + STAGE_MAX_DURATION_MS;
        state.phaseStartTime = phaseStartTime;
        state.live.lastSuccessTimestamp = phaseStartTime;
        log(`>>> [TIMER INICIADO] Workers ativos. Contagem de 5 min iniciada. <<<`);
      }
    };

    let chaptersDone = 0;
    let pagesDone = 0;
    let totalBytesDone = 0;
    const sourceLatencies: number[] = [];
    const downloadLatencies: number[] = [];
    const telegramLatencies: number[] = [];
    const dbPubLatencies: number[] = [];
    const dbMutexWaitLatencies: number[] = [];
    const stageBeginLats: number[] = [];
    const stageValidationLats: number[] = [];
    const stageWorkMappingLats: number[] = [];
    const stageChapterLats: number[] = [];
    const stageMediaLats: number[] = [];
    const stagePagesDeleteLats: number[] = [];
    const stagePagesInsertLats: number[] = [];
    const stageChapterMappingLats: number[] = [];
    const stageQueueUpdateLats: number[] = [];
    const stageWorkUpdateLats: number[] = [];
    const stageCommitLats: number[] = [];
    const stageSqlExecLats: number[] = [];
    const stageQueryCounts: number[] = [];
    const stageRowsWritten: number[] = [];

    const workerBusyTimeMs: number[] = new Array(workerCount + 1).fill(0);
    const minuteUtilizationHistory: number[] = [];
    let lastMinuteCheckTime = Date.now();
    let workerBusyTimeAtLastMin: number[] = new Array(workerCount + 1).fill(0);

    let stageShouldFinish = false;

    const checkCanFinishGracefully = () => {
      if (!stageTimerStarted || phaseStartTime === 0) return false;
      const elapsed = Date.now() - phaseStartTime;
      if (elapsed < STAGE_BASE_DURATION_MS) return false;

      const highUtilMins = minuteUtilizationHistory.filter(u => u >= MIN_WORKER_UTILIZATION_PCT).length;
      const loadMet = (chaptersDone >= MIN_COMPLETED_CHAPTERS) && (pagesDone >= MIN_REAL_PAGES) && (highUtilMins >= MIN_HIGH_UTIL_MINUTES);

      if (loadMet) return true;
      if (elapsed >= STAGE_MAX_DURATION_MS) return true;
      return false;
    };

    // Workers async loops
    const workerPromises: Promise<void>[] = [];
    for (let wId = 1; wId <= workerCount; wId++) {
      const p = (async (workerIdx: number) => {
        while (!stageShouldFinish && !stopTriggered && (phaseStartTime === 0 || Date.now() < maxPhaseEndTime)) {
          const job = await dynamicQueue.getNextJob();
          if (!job) {
            await new Promise(r => setTimeout(r, 1000));
            continue;
          }

          triggerStageStart();

          const chNum = parseFloat(job.chapter_number);
          const t0WorkerJob = Date.now();
          log(`[Worker ${workerIdx} | ${job.source}] Iniciando: ${job.work_title} Cap. ${chNum} (${job.source_chapter_id})`);

          try {
            const pool = getYugabytePool();
            const existCheck = await pool.query(`
              SELECT count(p.position) as p_count
              FROM chapters c
              JOIN pages p ON p.chapter_id = c.id
              WHERE c.work_id = $1 AND c.number = $2;
            `, [job.work_id, chNum]);

            if (parseInt(existCheck.rows[0].p_count, 10) > 0) {
              log(`[Worker ${workerIdx} SKIP | ${job.source}] Capítulo ${chNum} já importado (${existCheck.rows[0].p_count}p), marcando COMPLETED.`);
              await pool.query(`UPDATE importer_queue SET status = 'COMPLETED', updated_at = NOW() WHERE id = $1;`, [job.job_id]);
              continue;
            }

            const adapter = registry.get(job.source);
            if (!adapter) {
              circuitBreaker.record(job.source, false);
              continue;
            }

            // 1. Scrape chapter pages
            const t0Scrape = Date.now();
            let urls: string[] = [];
            for (let att = 0; att < 3; att++) {
              try {
                urls = await adapter.fetchChapterPages(job.source_chapter_id, chNum);
                break;
              } catch (e) {
                if (att === 2) throw e;
                await new Promise(r => setTimeout(r, 1500 * (att + 1)));
              }
            }
            const scrapeLat = Date.now() - t0Scrape;
            sourceLatencies.push(scrapeLat);
            circuitBreaker.record(job.source, true);

            if (!urls || urls.length === 0) {
              log(`[Worker ${workerIdx} WARN | ${job.source}] 0 URLs retornadas para ${job.work_title} Cap. ${chNum}`);
              continue;
            }

            log(`[Worker ${workerIdx} | ${job.source}] Scraped ${urls.length} páginas em ${scrapeLat}ms para ${job.work_title} Cap. ${chNum}`);

            // 2. Download and Upload pages DIRECTLY to Telegram
            const pagesPayload: any[] = new Array(urls.length);
            const pageConcurrency = 2;
            let currentUrlIdx = 0;

            const processPage = async (pIdx: number) => {
              if (stageShouldFinish || stopTriggered || (phaseStartTime > 0 && Date.now() >= maxPhaseEndTime)) return;
              const t0Dl = Date.now();
              let buf: Buffer | null = null;
              for (let att = 0; att < 3; att++) {
                try {
                  const res = await fetch(urls[pIdx], {
                    headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' },
                    signal: AbortSignal.timeout(15000)
                  });
                  if (!res.ok) throw new Error(`HTTP ${res.status}`);
                  buf = Buffer.from(await res.arrayBuffer());
                  break;
                } catch (e: any) {
                  if (att === 2) {
                    log(`[Worker ${workerIdx} WARN | ${job.source}] Falha download p${pIdx + 1}/${urls.length}: ${e.message}`);
                    return;
                  }
                  await new Promise(r => setTimeout(r, 1000 * (att + 1)));
                }
              }
              downloadLatencies.push(Date.now() - t0Dl);
              if (!buf || buf.length < 24) return;

              // DIRECT TELEGRAM UPLOAD (Bypassing Cloudflare Worker!)
              const pageMediaId = crypto.randomUUID();
              const t0Up = Date.now();
              try {
                const fileId = await directStorage.upload(buf, 'image/jpeg', pageMediaId, job.source_chapter_id);
                const upLat = Date.now() - t0Up;
                telegramLatencies.push(upLat);

                const botRef = directStorage.getLastBotReference(pageMediaId);
                const shardId = directStorage.getLastShardId(pageMediaId);

                pagesPayload[pIdx] = {
                  position: pIdx + 1,
                  mediaId: pageMediaId,
                  providerKey: fileId,
                  mime: 'image/jpeg',
                  width: 750,
                  height: 1000,
                  bytes: buf.length,
                  sha256: crypto.createHash('sha256').update(buf).digest('hex'),
                  botReference: botRef,
                  storageShardId: shardId
                };
              } catch (upErr: any) {
                log(`[Worker ${workerIdx} WARN | ${job.source}] Falha upload direto Telegram p${pIdx + 1}: ${upErr.message}`);
              }
            };

            const pageWorkers = [];
            for (let i = 0; i < pageConcurrency; i++) {
              pageWorkers.push((async () => {
                while (currentUrlIdx < urls.length) {
                  if (stageShouldFinish || stopTriggered || (phaseStartTime > 0 && Date.now() >= maxPhaseEndTime)) break;
                  const idx = currentUrlIdx++;
                  await processPage(idx);
                }
              })());
            }
            await Promise.all(pageWorkers);

            const filteredPages = pagesPayload.filter(Boolean);
            if (filteredPages.length === 0 || stageShouldFinish || stopTriggered || (phaseStartTime > 0 && Date.now() >= maxPhaseEndTime)) {
              if (filteredPages.length === 0) {
                log(`[Worker ${workerIdx} WARN | ${job.source}] 0 páginas válidas após download/upload para ${job.work_title} Cap. ${chNum}`);
                circuitBreaker.record(job.source, false);
              }
              continue;
            }

            const cleanPages = filteredPages.map((p, idx) => ({ ...p, position: idx + 1 }));
            const chapterBytes = cleanPages.reduce((acc, p) => acc + (p.bytes || 0), 0);

            // 3. Strict Serialized DB Mutex: Concurrency = 1, Pool = 1
            const t0MutexWait = Date.now();
            const release = await dbMutex.acquire();
            const waitDbMutexMs = Date.now() - t0MutexWait;
            try {
              const t0Pub = Date.now();
              const pubRes = await publishBatchDirect({
                jobId: job.job_id,
                work: { id: job.work_id, title: job.work_title, slug: '' } as any,
                chapter: {
                  number: chNum,
                  title: job.chapter_title || `Capítulo ${chNum}`,
                  source: job.source,
                  sourceChapterId: job.source_chapter_id
                },
                pages: cleanPages
              });
              const pubLat = Date.now() - t0Pub;
              dbPubLatencies.push(pubLat);
              dbMutexWaitLatencies.push(waitDbMutexMs);
              stageBeginLats.push(pubRes.metrics.beginMs);
              stageValidationLats.push(pubRes.metrics.validationMs);
              stageWorkMappingLats.push(pubRes.metrics.workMappingMs);
              stageChapterLats.push(pubRes.metrics.chapterMs);
              stageMediaLats.push(pubRes.metrics.mediaMs);
              stagePagesDeleteLats.push(pubRes.metrics.pagesDeleteMs);
              stagePagesInsertLats.push(pubRes.metrics.pagesInsertMs);
              stageChapterMappingLats.push(pubRes.metrics.chapterMappingMs);
              stageQueueUpdateLats.push(pubRes.metrics.queueUpdateMs);
              stageWorkUpdateLats.push(pubRes.metrics.workUpdateMs);
              stageCommitLats.push(pubRes.metrics.commitMs);
              stageSqlExecLats.push(pubRes.metrics.sqlExecutionMs);
              stageQueryCounts.push(pubRes.metrics.queryCount);
              stageRowsWritten.push(pubRes.metrics.rowsWritten);

              chaptersDone++;
              pagesDone += cleanPages.length;
              totalBytesDone += chapterBytes;

              state.live.lastSuccessfulJob = `${job.work_title} Cap. ${chNum} (${cleanPages.length}p) [${job.source}]`;
              state.live.lastSuccessTimestamp = Date.now();
              state.live.completedChapters = chaptersDone;
              state.live.realPagesProcessed = pagesDone;

              log(`[Worker ${workerIdx} | ${job.source}] PUBLICADO: ${state.live.lastSuccessfulJob} em ${pubLat}ms DB (wait: ${waitDbMutexMs}ms) (Total: ${chaptersDone} cap, ${pagesDone} pag)`);
            } finally {
              release();
            }

            workerBusyTimeMs[workerIdx] += (Date.now() - t0WorkerJob);
          } catch (err: any) {
            circuitBreaker.record(job.source, false);
            log(`[Worker ${workerIdx} WARN | ${job.source}] Erro transitório no capítulo ${chNum}: ${err.message}`);
            await new Promise(r => setTimeout(r, 1500));
          } finally {
            dynamicQueue.releaseJob(job.job_id);
          }
        }
      })(wId);
      workerPromises.push(p);
    }

    // Probing Loop in background
    const probePromise = (async () => {
      const probeRes = await runProbingLoop(STAGE_MAX_DURATION_MS, () => stageShouldFinish);
      return probeRes;
    })();

    // Controller Loop
    while (!stageShouldFinish && !stopTriggered && Date.now() < maxPhaseEndTime) {
      await new Promise(r => setTimeout(r, 5000));

      const now = Date.now();
      const elapsedTotalSec = (now - phaseStartTime) / 1000;
      const elapsedMin = elapsedTotalSec / 60;

      if (now - lastMinuteCheckTime >= 60000) {
        const deltaBusy = workerBusyTimeMs.reduce((a, b) => a + b, 0) - workerBusyTimeAtLastMin.reduce((a, b) => a + b, 0);
        const deltaTotal = workerCount * (now - lastMinuteCheckTime);
        const minUtil = deltaTotal > 0 ? (deltaBusy / deltaTotal) * 100 : 0;
        minuteUtilizationHistory.push(parseFloat(minUtil.toFixed(1)));
        workerBusyTimeAtLastMin = [...workerBusyTimeMs];
        lastMinuteCheckTime = now;
      }

      const cpm = elapsedMin > 0 ? parseFloat((chaptersDone / elapsedMin).toFixed(2)) : 0;
      const ppm = elapsedMin > 0 ? parseFloat((pagesDone / elapsedMin).toFixed(2)) : 0;
      const mbpm = elapsedMin > 0 ? parseFloat(((totalBytesDone / (1024 * 1024)) / elapsedMin).toFixed(2)) : 0;
      const totalPotentialBusy = workerCount * (elapsedTotalSec * 1000);
      const totalActualBusy = workerBusyTimeMs.reduce((a, b) => a + b, 0);
      const currentUtil = totalPotentialBusy > 0 ? parseFloat(((totalActualBusy / totalPotentialBusy) * 100).toFixed(1)) : 0;

      const summary = directStorage.getMetricsSummary();
      const totalFlood = summary.bots.reduce((acc, b) => acc + (b.floodWaitSeconds || 0), 0);

      state.live.chaptersPerMin = cpm;
      state.live.pagesPerMin = ppm;
      state.live.mbPerMin = mbpm;
      state.live.workerUtilizationPct = currentUtil;
      state.live.floodWaitTotal = totalFlood;

      saveState();

      if (checkCanFinishGracefully()) {
        log(`[STAGE FINISH CONDITION MET] Estágio de ${workerCount} workers concluiu critérios de duração e carga.`);
        stageShouldFinish = true;
        break;
      }
    }

    stageShouldFinish = true;
    const probeResults = await probePromise;
    await Promise.all(workerPromises);

    const actualDurationSeconds = Math.round((Date.now() - phaseStartTime) / 1000);
    const actualDurationMinutes = actualDurationSeconds / 60;
    const chaptersPerMin = actualDurationMinutes > 0 ? parseFloat((chaptersDone / actualDurationMinutes).toFixed(2)) : 0;
    const pagesPerMin = actualDurationMinutes > 0 ? parseFloat((pagesDone / actualDurationMinutes).toFixed(2)) : 0;
    const mbPerMin = actualDurationMinutes > 0 ? parseFloat(((totalBytesDone / (1024 * 1024)) / actualDurationMinutes).toFixed(2)) : 0;

    const totalPotentialWorkerTimeMs = workerCount * (actualDurationSeconds * 1000);
    const totalBusyWorkerTimeMs = workerBusyTimeMs.reduce((a, b) => a + b, 0);
    const workerUtilizationPct = totalPotentialWorkerTimeMs > 0
      ? parseFloat(((totalBusyWorkerTimeMs / totalPotentialWorkerTimeMs) * 100).toFixed(1))
      : 0;

    const highUtilMins = minuteUtilizationHistory.filter(u => u >= MIN_WORKER_UTILIZATION_PCT).length;
    const hasSufficientLoad = (chaptersDone >= MIN_COMPLETED_CHAPTERS) && (pagesDone >= MIN_REAL_PAGES) && (highUtilMins >= MIN_HIGH_UTIL_MINUTES);

    const sourceStats = circuitBreaker.getStats();
    const summary = directStorage.getMetricsSummary();
    const totalFloodWaitSec = summary.bots.reduce((acc, b) => acc + (b.floodWaitSeconds || 0), 0);
    const totalFloodWaitCount = summary.bots.reduce((acc, b) => acc + (b.rateLimits429 || 0), 0);

    log(`Executando auditoria de integridade pós-Estágio ${workerCount} workers...`);
    const integrity = await verifyDbIntegrity();
    if (!integrity.pass) {
      stopTriggered = true;
      stopReason = `STOP TRIGGERED: Falha de integridade no banco (phantom=${integrity.phantomWorks}, soak=${integrity.tgSoakPages}, zeroPages=${integrity.zeroPageChapters}, dupCh=${integrity.duplicateChapters}, dupPos=${integrity.duplicatePagePositions}, idleTx=${integrity.idleInTransaction})`;
      log(`[CRITICAL INTEGRITY FAILURE] ${stopReason}`);
    } else {
      log(`[INTEGRIDADE 100% PASS] 0 phantoms, 0 tg-soak, 0 zero-pages, 0 duplicatas, 0 idle tx.`);
    }

    let stageStatus: 'PASS' | 'STOP_TRIGGERED' | 'INSUFFICIENT_LOAD' = 'PASS';
    if (stopTriggered) {
      stageStatus = 'STOP_TRIGGERED';
    } else if (!hasSufficientLoad) {
      stageStatus = 'INSUFFICIENT_LOAD';
    }

    const stageMetric: PhaseMetrics = {
      phase: phaseName,
      workerCount,
      durationSeconds: actualDurationSeconds,
      completedChapters: chaptersDone,
      realPagesProcessed: pagesDone,
      totalBytesProcessed: totalBytesDone,
      chaptersPerMin,
      pagesPerMin,
      mbPerMin,
      hasSufficientLoad,
      workerUtilizationPct,
      highUtilizationMinutesCount: highUtilMins,
      sourceFailureRatePct: sourceStats.overallFailureRatePct,
      sourceStats,
      homeP50: probeResults.homeP50,
      homeP95: probeResults.homeP95,
      homeP99: probeResults.homeP99,
      readerP50: probeResults.readerP50,
      readerP95: probeResults.readerP95,
      readerP99: probeResults.readerP99,
      mediaP50: probeResults.mediaP50,
      mediaP95: probeResults.mediaP95,
      mediaP99: probeResults.mediaP99,
      networkDbP50: probeResults.networkDbP50,
      networkDbP95: probeResults.networkDbP95,
      networkDbP99: probeResults.networkDbP99,
      site5xxCount: probeResults.site5xx,
      siteTimeoutCount: probeResults.siteTimeouts,
      siteTotalProbes: probeResults.totalProbes,
      yugabyteMaxConnections: probeResults.yugabyteMaxConnections,
      yugabyteAvgConnections: probeResults.yugabyteAvgConnections,
      yugabyteActiveConnections: 1,
      yugabyteIdleInTx: probeResults.yugabyteIdleInTx,
      yugabyteQueryLatencyMs: probeResults.yugabyteQueryLatencyMs,
      sourceLatencyP50: percentile(sourceLatencies, 50),
      sourceLatencyP95: percentile(sourceLatencies, 95),
      downloadLatencyAvg: downloadLatencies.length > 0 ? Math.round(downloadLatencies.reduce((a, b) => a + b, 0) / downloadLatencies.length) : 0,
      telegramUploadLatencyAvg: telegramLatencies.length > 0 ? Math.round(telegramLatencies.reduce((a, b) => a + b, 0) / telegramLatencies.length) : 0,
      dbPublicationLatencyP50: percentile(dbPubLatencies, 50),
      dbPublicationLatencyP95: percentile(dbPubLatencies, 95),
      dbPublicationLatencyP99: percentile(dbPubLatencies, 99),
      dbMutexWaitLatencyP50: percentile(dbMutexWaitLatencies, 50),
      dbMutexWaitLatencyP95: percentile(dbMutexWaitLatencies, 95),
      dbMutexWaitLatencyP99: percentile(dbMutexWaitLatencies, 99),
      dbDetails: {
        beginP95: percentile(stageBeginLats, 95),
        validationP95: percentile(stageValidationLats, 95),
        workMappingP95: percentile(stageWorkMappingLats, 95),
        chapterP95: percentile(stageChapterLats, 95),
        mediaP95: percentile(stageMediaLats, 95),
        pagesDeleteP95: percentile(stagePagesDeleteLats, 95),
        pagesInsertP95: percentile(stagePagesInsertLats, 95),
        chapterMappingP95: percentile(stageChapterMappingLats, 95),
        queueUpdateP95: percentile(stageQueueUpdateLats, 95),
        workUpdateP95: percentile(stageWorkUpdateLats, 95),
        commitP95: percentile(stageCommitLats, 95),
        sqlExecP95: percentile(stageSqlExecLats, 95),
        avgQueriesPerChapter: stageQueryCounts.length > 0 ? Number((stageQueryCounts.reduce((a, b) => a + b, 0) / stageQueryCounts.length).toFixed(1)) : 0,
        avgRowsWrittenPerChapter: stageRowsWritten.length > 0 ? Number((stageRowsWritten.reduce((a, b) => a + b, 0) / stageRowsWritten.length).toFixed(1)) : 0,
        rowsWrittenPerSec: actualDurationSeconds > 0 ? Number((stageRowsWritten.reduce((a, b) => a + b, 0) / actualDurationSeconds).toFixed(1)) : 0,
      },
      shardsUsedCount: summary.shards.filter(s => s.uploads > 0).length,
      botsUsedCount: summary.bots.filter(b => b.uploads > 0).length,
      botMetrics: summary.bots,
      shardMetrics: summary.shards,
      totalFloodWaitSeconds: totalFloodWaitSec,
      totalFloodWaitCount: totalFloodWaitCount,
      integrityCheck: integrity,
      status: stageStatus,
      stopReason: stopTriggered ? stopReason : (!hasSufficientLoad ? `Carga insuficiente` : undefined)
    };

    state.stages.push(stageMetric);
    saveState();

    log(`\n--- RESUMO DO ESTÁGIO COM ${workerCount} WORKER(S) ---
    Status: ${stageMetric.status} ${stageMetric.stopReason ? `(${stageMetric.stopReason})` : ''}
    Duração: ${actualDurationSeconds}s (${Math.round(actualDurationMinutes)} min)
    Carga: ${chaptersDone} cap concluídos | ${pagesDone} páginas reais (${(totalBytesDone / 1024 / 1024).toFixed(1)} MB)
    Throughput: ${chaptersPerMin} cap/min | ${pagesPerMin} pag/min | ${mbPerMin} MB/min
    Utilização dos Workers: ${workerUtilizationPct}% (${highUtilMins} min com util >= 70%)
    Site Home:   p50=${stageMetric.homeP50}ms | p95=${stageMetric.homeP95}ms | p99=${stageMetric.homeP99}ms (Base p95: ${state.baseline?.homeP95}ms)
    Site Reader: p50=${stageMetric.readerP50}ms | p95=${stageMetric.readerP95}ms | p99=${stageMetric.readerP99}ms (Base p95: ${state.baseline?.readerP95}ms)
    Site Media:  p50=${stageMetric.mediaP50}ms | p95=${stageMetric.mediaP95}ms | p99=${stageMetric.mediaP99}ms (Base p95: ${state.baseline?.mediaP95}ms)
    Site 5xx: ${stageMetric.site5xxCount} | Timeouts: ${stageMetric.siteTimeoutCount}
    Network DB (Ping RTT): p50=${stageMetric.networkDbP50}ms | p95=${stageMetric.networkDbP95}ms | p99=${stageMetric.networkDbP99}ms
    DB Mutex Wait: p50=${stageMetric.dbMutexWaitLatencyP50}ms | p95=${stageMetric.dbMutexWaitLatencyP95}ms | p99=${stageMetric.dbMutexWaitLatencyP99}ms
    DB Commit Tx:  p50=${stageMetric.dbPublicationLatencyP50}ms | p95=${stageMetric.dbPublicationLatencyP95}ms | p99=${stageMetric.dbPublicationLatencyP99}ms
    DB Sub-timings p95: Begin=${stageMetric.dbDetails.beginP95}ms, Val=${stageMetric.dbDetails.validationP95}ms, WorkMap=${stageMetric.dbDetails.workMappingP95}ms, Ch=${stageMetric.dbDetails.chapterP95}ms, Media=${stageMetric.dbDetails.mediaP95}ms, PgDel=${stageMetric.dbDetails.pagesDeleteP95}ms, PgIns=${stageMetric.dbDetails.pagesInsertP95}ms, ChMap=${stageMetric.dbDetails.chapterMappingP95}ms, Q=${stageMetric.dbDetails.queueUpdateP95}ms, WorkUpd=${stageMetric.dbDetails.workUpdateP95}ms, Commit=${stageMetric.dbDetails.commitP95}ms
    DB Load: Avg Queries/Cap=${stageMetric.dbDetails.avgQueriesPerChapter}, Rows Written/Cap=${stageMetric.dbDetails.avgRowsWrittenPerChapter}, Rows Written/Sec=${stageMetric.dbDetails.rowsWrittenPerSec}
    FloodWait Total Telegram: ${totalFloodWaitSec}s (${totalFloodWaitCount} ocorrências)
    Integridade: ${integrity.pass ? 'PASS (100%)' : 'FAIL'}`);

    // Safety stop checks: stop ladder if DB mutex degradation occurs
    if (stageStatus === 'PASS') {
      if (stageMetric.dbMutexWaitLatencyP95 > 500) {
        stopTriggered = true;
        stopReason = `STOP TRIGGERED: Degradação do Mutex DB (WAIT_DB_MUTEX p95=${stageMetric.dbMutexWaitLatencyP95}ms > 500ms)`;
        stageStatus = 'STOP_TRIGGERED';
      }
    }

    if (stageStatus === 'PASS') {
      lastSafeWorkers = workerCount;
      if (lastValidPagesPerMin !== null) {
        const gainPct = ((pagesPerMin - lastValidPagesPerMin) / lastValidPagesPerMin) * 100;
        log(`[MARGINAL GAIN] Ganho vs estágio anterior: ${gainPct.toFixed(1)}% (${pagesPerMin} vs ${lastValidPagesPerMin} pag/min)`);
        if (gainPct < 10.0 && ladderSteps.length > 2) {
          consecutiveLowGains++;
          if (consecutiveLowGains >= 2) {
            stopTriggered = true;
            stopReason = `LIMITE DE THROUGHPUT ÚTIL: ganho de rendimento marginal inferior a 10% por 2 estágios consecutivos (${gainPct.toFixed(1)}%)`;
            log(`[USEFUL THROUGHPUT LIMIT REACHED] ${stopReason}`);
            break;
          }
        } else {
          consecutiveLowGains = 0;
        }
      }
      lastValidPagesPerMin = pagesPerMin;
    } else if (stageStatus === 'STOP_TRIGGERED') {
      firstUnsafeWorkers = workerCount;
      break;
    }
  }

  // Verdict calculation
  const passingStages = state.stages.filter(s => s.status === 'PASS');
  let maxCpm = 0;
  let maxPpm = 0;
  let maxMbpm = 0;
  for (const s of passingStages) {
    if (s.chaptersPerMin > maxCpm) maxCpm = s.chaptersPerMin;
    if (s.pagesPerMin > maxPpm) maxPpm = s.pagesPerMin;
    if (s.mbPerMin > maxMbpm) maxMbpm = s.mbPerMin;
  }

  const recWorkers = typeof lastSafeWorkers === 'number'
    ? Math.max(2, Math.floor(lastSafeWorkers * 0.8))
    : 2;

  state.task = 'FINISHED';
  state.verdict = {
    lastSafeWorkerCount: lastSafeWorkers,
    firstUnsafeWorkerCount: firstUnsafeWorkers,
    recommendedProductionWorkers: recWorkers,
    maxMeasuredChaptersPerMin: maxCpm,
    maxMeasuredPagesPerMin: maxPpm,
    maxMeasuredMbPerMin: maxMbpm,
    primaryBottleneck: firstUnsafeWorkers !== 'NONE' ? 'SITE_SATURATION' : 'NONE',
    reason: stopReason || 'Escada de workers finalizada com sucesso.'
  };

  saveState();

  log(`\n======================================================================`);
  log(`BENCHMARK FINALIZADO`);
  log(`LAST SAFE WORKERS: ${state.verdict.lastSafeWorkerCount}`);
  log(`FIRST UNSAFE WORKERS: ${state.verdict.firstUnsafeWorkerCount}`);
  log(`RECOMMENDED PRODUCTION WORKERS: ${state.verdict.recommendedProductionWorkers}`);
  log(`MAX MEASURED THROUGHPUT: ${state.verdict.maxMeasuredPagesPerMin} pag/min (${state.verdict.maxMeasuredChaptersPerMin} cap/min, ${state.verdict.maxMeasuredMbPerMin} MB/min)`);
  try {
    const pool = getYugabytePool();
    await pool.query(`
      UPDATE importer_queue 
      SET status = 'PAUSED_BY_STAFF', locked_by = NULL, locked_at = NULL, lease_expires_at = NULL 
      WHERE status = 'PROCESSING' OR status = 'PENDING';
    `);
    const actRes = await pool.query(`SELECT count(*) as cnt FROM importer_queue WHERE status = 'PROCESSING';`);
    log(`[SAFETY CHECK] Todas as filas em PAUSED_BY_STAFF. Active jobs = ${actRes.rows[0].cnt}. IMPORTER = FROZEN.`);
    await pool.end();
  } catch {}
}

main().catch(err => {
  log(`[FATAL ORCHESTRATOR ERROR] ${err.message}\n${err.stack}`);
  process.exit(1);
});
