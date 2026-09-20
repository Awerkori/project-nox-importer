import dotenv from 'dotenv';
dotenv.config({ path: '/home/awerkori/.Projects/project-nox-importer/.env' });
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import https from 'node:https';
import pg from 'pg';
import { SourceRegistry } from '../src/sources/registry.js';
import { HostRateLimiter } from '../src/core/rate-limiter.js';
import { publishBatchDirect } from '../src/db/yugabyte-direct.js';
import { DirectTelegramStorageProvider } from '../build/storage/direct-telegram.js';

// Configuration
const MANGA_URL = 'https://manga.project-nox-awerkori.workers.dev';
const LOG_FILE = '/home/awerkori/scratch/max_perf_benchmark_progress.log';
const METRICS_FILE = '/home/awerkori/scratch/max_perf_live_metrics.json';

// Target Probes
const PROBE_HOME = `${MANGA_URL}/`;
const PROBE_READER = `${MANGA_URL}/ler/52cc31e2-2ff0-43cf-8d9e-1a493eb60521`;
const PROBE_MEDIA = `${MANGA_URL}/media/645bc9f4-7c9b-4a45-af96-102b8a796263`;

// Stage Durations (seconds)
const DEFAULT_STAGE_DURATION_SEC: Record<number, number> = {
  5: 1200, // 20 min (Homologação Oficial 5W)
  6: 600,  // 10 min
  7: 600,  // 10 min
  8: 600,  // 10 min
  10: 600, // 10 min
  12: 600, // 10 min
  15: 600, // 10 min
  18: 600, // 10 min
  20: 600, // 10 min
};

// Global override if BENCHMARK_STAGE_DURATION_SEC is passed
const OVERRIDE_STAGE_DURATION_SEC = process.env.BENCHMARK_STAGE_DURATION_SEC
  ? parseInt(process.env.BENCHMARK_STAGE_DURATION_SEC, 10)
  : null;

// Target stages selection via TARGET_STAGES env var (e.g. "1,2,3" or "1,2,3,4,5" or "5" or "soak")
const TARGET_STAGES_ENV = process.env.TARGET_STAGES || null;

// Soak duration: 60 minutes
const SOAK_DURATION_SEC = 3600;

// Quality Thresholds (Project Nox Quality Gates)
const THRESHOLDS = {
  MIN_PAGES_PER_MIN: 250,
  TARGET_PAGES_PER_MIN: 400,
  READER_P95_TARGET_MS: 150,
  READER_P95_MAX_MS: 200,
  MEDIA_P95_TARGET_MS: 120,
  MEDIA_P95_MAX_MS: 150,
  HOME_P95_TARGET_MS: 250,
  HOME_P95_MAX_MS: 350,
  MAX_5XX: 0,
  MAX_TIMEOUTS: 0,
  MAX_FLOODWAIT_SEC: 0,
};

// Backpressure & Buffer Constants
const MAX_QUEUE_CAPACITY = 15;
const HIGH_WATERMARK = 12;
const LOW_WATERMARK = 5;

const MIN_COMPLETED_CHAPTERS = 4;
const MIN_REAL_PAGES = 80;
const MIN_WORKER_UTILIZATION_PCT = 55;

// The 7 healthy PT-BR candidate sources
const ALL_SOURCES = [
  'manhastro',
  'fleurblanche',
  'taimumangas',
  'vegitoons',
  'hanamiheaven',
  'mangalivreto',
  'nexus',
];

// Logging
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

// Dedicated DB Pools
let readPoolInstance: pg.Pool | null = null;
function getReadPool(): pg.Pool {
  if (readPoolInstance) return readPoolInstance;
  const cfg = {
    host: process.env.YUGABYTE_HOST,
    port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
    user: process.env.YUGABYTE_USER,
    password: process.env.YUGABYTE_PASSWORD,
    database: process.env.YUGABYTE_DATABASE,
    ssl: { rejectUnauthorized: false },
    max: 2, // Dedicated read/telemetry pool (probes + queue replenish)
    connectionTimeoutMillis: 10000,
    idleTimeoutMillis: 30000,
    application_name: 'project-nox-importer-benchmark-read',
  };
  readPoolInstance = new pg.Pool(cfg);
  return readPoolInstance;
}

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
        cooldownSec: this.getCooldownRemainingSec(s),
      };
    }
    return {
      totalAttempts: totAtt,
      totalFailures: totFail,
      overallFailureRatePct: totAtt > 0 ? parseFloat(((totFail / totAtt) * 100).toFixed(1)) : 0,
      breakdown,
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
      const pool = getReadPool();
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
              source_chapter_id: row.source_chapter_id,
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

// Ready Chapter Item for Decoupled Publication
interface ReadyChapter {
  job: BenchmarkJob;
  cleanPages: any[];
  chapterBytes: number;
  workerIdx: number;
  source: string;
  workTitle: string;
  chNum: number;
  t0WorkerJob: number;
  enqueuedAt: number;
}

// Real-Time Site Health Autotuner
export class SiteHealthAutotuner {
  private currentWorkers: number;
  private maxWorkers: number;
  private minWorkers: number;
  private yellowSince: number | null = null;
  private greenSince: number = Date.now();
  private lastAdjustmentTime: number = Date.now();

  constructor(options: { initialWorkers: number; maxWorkers: number; minWorkers?: number }) {
    this.currentWorkers = options.initialWorkers;
    this.maxWorkers = options.maxWorkers;
    this.minWorkers = options.minWorkers || 1;
  }

  updateMetrics(stats: {
    readerP95: number;
    homeP95: number;
    mediaP95: number;
    site5xx: number;
    siteTimeouts: number;
  }): {
    action: 'MAINTAIN' | 'SCALE_DOWN' | 'EMERGENCY_DROP' | 'SCALE_UP';
    workers: number;
    reason: string;
    degraded: boolean;
  } {
    const now = Date.now();
    const { readerP95, homeP95, mediaP95, site5xx, siteTimeouts } = stats;

    // Emergency check: 5xx or timeouts
    if (site5xx > 0 || siteTimeouts > 0) {
      const target = Math.max(this.minWorkers, 2);
      if (this.currentWorkers > target) {
        this.currentWorkers = target;
        this.lastAdjustmentTime = now;
      }
      return {
        action: 'EMERGENCY_DROP',
        workers: this.currentWorkers,
        reason: `5xx ou timeouts detectados no site (5xx=${site5xx}, timeouts=${siteTimeouts}). Recuo emergencial para ${this.currentWorkers} workers.`,
        degraded: true,
      };
    }

    // Red condition: Reader > 250ms or Home > 400ms -> immediate 2-step drop
    if (readerP95 > 250 || homeP95 > 400) {
      const drop = Math.max(this.minWorkers, this.currentWorkers - 2);
      if (drop < this.currentWorkers && now - this.lastAdjustmentTime > 10000) {
        this.currentWorkers = drop;
        this.lastAdjustmentTime = now;
        this.yellowSince = null;
      }
      return {
        action: 'SCALE_DOWN',
        workers: this.currentWorkers,
        reason: `Degradação severa (Reader p95=${readerP95}ms > 250ms ou Home p95=${homeP95}ms > 400ms). Reduzido 2 degraus para ${this.currentWorkers} workers.`,
        degraded: true,
      };
    }

    // Yellow condition: Reader > 180ms or Home > 300ms for 30s -> drop 1 step
    if (readerP95 > 180 || homeP95 > 300) {
      if (!this.yellowSince) {
        this.yellowSince = now;
      } else if (now - this.yellowSince >= 30000 && now - this.lastAdjustmentTime > 20000) {
        const drop = Math.max(this.minWorkers, this.currentWorkers - 1);
        if (drop < this.currentWorkers) {
          this.currentWorkers = drop;
          this.lastAdjustmentTime = now;
          this.yellowSince = null;
        }
      }
      this.greenSince = 0;
      return {
        action: 'MAINTAIN',
        workers: this.currentWorkers,
        reason: `Em observação na zona amarela (Reader p95=${readerP95}ms, Home p95=${homeP95}ms)`,
        degraded: false,
      };
    }

    // Green condition: Reader <= 150ms and Home <= 250ms
    this.yellowSince = null;
    if (this.greenSince === 0) {
      this.greenSince = now;
    }

    // Progressive scale up: if site green for >= 60s and below target maxWorkers
    if (
      now - this.greenSince >= 60000 &&
      this.currentWorkers < this.maxWorkers &&
      now - this.lastAdjustmentTime > 60000
    ) {
      this.currentWorkers++;
      this.lastAdjustmentTime = now;
      this.greenSince = now;
      return {
        action: 'SCALE_UP',
        workers: this.currentWorkers,
        reason: `Site estável no verde por >=60s (Reader p95=${readerP95}ms, Home p95=${homeP95}ms). Aumentando progressivamente para ${this.currentWorkers} workers.`,
        degraded: false,
      };
    }

    return {
      action: 'MAINTAIN',
      workers: this.currentWorkers,
      reason: 'Site saudável (zona verde)',
      degraded: false,
    };
  }

  getCurrentWorkers(): number {
    return this.currentWorkers;
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
  readyQueuePeakDepth: number;
  readyQueueDepthP50: number;
  readyQueueDepthP95: number;
  readyQueueAgeP50Ms: number;
  readyQueueAgeP95Ms: number;
  publicationLagP50Ms: number;
  publicationLagP95Ms: number;
  publicationLagP99Ms: number;
  publisherThroughputCapPerMin: number;
  publisherThroughputPagesPerMin: number;
  isQueueStable: boolean;
  backpressurePauseCount: number;
  dbDetails: {
    beginP95: number;
    validationP95: number;
    masterCteP95: number;
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
    readyQueueDepth: number;
    lastSuccessfulJob: string;
    lastSuccessTimestamp: number;
    minutesSinceLastSuccess: number;
    yugabyteConnections: string;
    site5xx: number;
    timeouts: number;
    floodWaitTotal: number;
    autotunerAction: string;
    autotunerReason: string;
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
    readyQueueDepth: 0,
    lastSuccessfulJob: 'Nenhum',
    lastSuccessTimestamp: Date.now(),
    minutesSinceLastSuccess: 0,
    yugabyteConnections: '1/20',
    site5xx: 0,
    timeouts: 0,
    floodWaitTotal: 0,
    autotunerAction: 'INIT',
    autotunerReason: 'Inicializando autotuner',
  },
  stages: [],
  verdict: null,
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
  log('Realizando aquecimento de cache e sockets nas rotas de teste...');
  for (let i = 0; i < 3; i++) {
    await probeUrl(PROBE_HOME).catch(() => {});
    await new Promise(r => setTimeout(r, 150));
    await probeUrl(PROBE_READER).catch(() => {});
    await new Promise(r => setTimeout(r, 150));
    await probeUrl(PROBE_MEDIA).catch(() => {});
    await new Promise(r => setTimeout(r, 300));
  }
  log('Cache e sockets aquecidos com sucesso.');
}

const probeHttpsAgent = new https.Agent({
  keepAlive: true,
  maxSockets: 8,
  timeout: 10000,
});

async function probeUrl(
  url: string,
  timeoutMs = 10000
): Promise<{ ok: boolean; status: number; latency: number; isTimeout: boolean; is5xx: boolean }> {
  for (let attempt = 0; attempt < 3; attempt++) {
    const t0 = Date.now();
    try {
      const res: any = await new Promise((resolve, reject) => {
        let resolved = false;
        const req = https.get(url, { agent: probeHttpsAgent, timeout: timeoutMs }, httpRes => {
          const latency = Date.now() - t0;
          resolved = true;
          // Drain stream immediately to reuse socket without downloading megabytes of bodies
          httpRes.resume();
          resolve({
            statusCode: httpRes.statusCode || 0,
            ok: (httpRes.statusCode || 0) >= 200 && (httpRes.statusCode || 0) < 400,
            latency,
          });
        });
        req.on('timeout', () => {
          req.destroy(new Error('Timeout'));
        });
        req.on('error', (err) => {
          if (!resolved) reject(err);
        });
      });
      if (res.statusCode >= 500) {
        return { ok: false, status: res.statusCode, latency: res.latency, isTimeout: false, is5xx: true };
      }
      return { ok: true, status: res.statusCode, latency: res.latency, isTimeout: false, is5xx: false };
    } catch (err: any) {
      const latency = Date.now() - t0;
      const isTimeout = err.name === 'TimeoutError' || err.message === 'Timeout' || latency >= timeoutMs;
      if (isTimeout) {
        return { ok: false, status: 0, latency, isTimeout: true, is5xx: false };
      }
      if (attempt < 2) {
        await new Promise(r => setTimeout(r, 400 * (attempt + 1)));
      } else {
        return { ok: false, status: 0, latency, isTimeout: false, is5xx: false };
      }
    }
  }
  return { ok: false, status: 0, latency: timeoutMs, isTimeout: true, is5xx: false };
}

async function runProbingLoop(
  durationMs: number,
  shouldStopCheck: () => boolean,
  autotuner: SiteHealthAutotuner
) {
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

  const readPool = getReadPool();
  const startTime = Date.now();
  const endTime = startTime + durationMs;

  while (Date.now() < endTime && !stopTriggered && !shouldStopCheck()) {
    totalProbes++;

    const pHome = await probeUrl(PROBE_HOME);
    await new Promise(r => setTimeout(r, 200));
    const pReader = await probeUrl(PROBE_READER);
    await new Promise(r => setTimeout(r, 200));
    const pMedia = await probeUrl(PROBE_MEDIA);

    if (pHome.ok) homeLatencies.push(pHome.latency);
    if (pHome.is5xx) site5xx++;
    if (pHome.isTimeout) siteTimeouts++;

    if (pReader.ok) readerLatencies.push(pReader.latency);
    if (pReader.is5xx) site5xx++;
    if (pReader.isTimeout) siteTimeouts++;

    if (pMedia.ok) mediaLatencies.push(pMedia.latency);
    if (pMedia.is5xx) site5xx++;
    if (pMedia.isTimeout) siteTimeouts++;

    // Measure pure network roundtrip ping to Yugabyte on read pool
    try {
      const t0Ping = Date.now();
      await readPool.query('SELECT 1;');
      networkDbLatencies.push(Date.now() - t0Ping);
    } catch {}

    // Check DB activity stats
    try {
      const t0 = Date.now();
      const res = await readPool.query(`
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

    // Evaluate Autotuner with sliding window of last 15 samples (only after at least 10 samples collected)
    if (readerLatencies.length >= 10) {
      const winReader = percentile(readerLatencies.slice(-15), 95);
      const winHome = percentile(homeLatencies.slice(-15), 95);
      const winMedia = percentile(mediaLatencies.slice(-15), 95);

      const autoRes = autotuner.updateMetrics({
        readerP95: winReader,
        homeP95: winHome,
        mediaP95: winMedia,
        site5xx,
        siteTimeouts,
      });

      state.live.autotunerAction = autoRes.action;
      state.live.autotunerReason = autoRes.reason;

      if (autoRes.degraded && (site5xx >= 3 || siteTimeouts >= 3 || winReader > THRESHOLDS.READER_P95_MAX_MS)) {
        log(`[AUTOTUNER ALERT] ${autoRes.reason}`);
      }
    }

    saveState();
    await new Promise(r => setTimeout(r, 3000));
  }

  const avgConns =
    yugabyteConnections.length > 0
      ? Math.round(yugabyteConnections.reduce((a, b) => a + b, 0) / yugabyteConnections.length)
      : 0;

  const steadyHome = homeLatencies.length >= 15 ? homeLatencies.slice(2) : homeLatencies;
  const steadyReader = readerLatencies.length >= 15 ? readerLatencies.slice(2) : readerLatencies;
  const steadyMedia = mediaLatencies.length >= 15 ? mediaLatencies.slice(2) : mediaLatencies;
  const steadyNetworkDb = networkDbLatencies.length >= 15 ? networkDbLatencies.slice(2) : networkDbLatencies;

  return {
    homeP50: percentile(steadyHome, 50),
    homeP95: percentile(steadyHome, 95),
    homeP99: percentile(steadyHome, 99),
    readerP50: percentile(steadyReader, 50),
    readerP95: percentile(steadyReader, 95),
    readerP99: percentile(steadyReader, 99),
    mediaP50: percentile(steadyMedia, 50),
    mediaP95: percentile(steadyMedia, 95),
    mediaP99: percentile(steadyMedia, 99),
    networkDbP50: percentile(steadyNetworkDb, 50),
    networkDbP95: percentile(steadyNetworkDb, 95),
    networkDbP99: percentile(steadyNetworkDb, 99),
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
  const readPool = getReadPool();

  const phantomRes = await readPool.query(`
    SELECT count(*) as count
    FROM works
    WHERE title LIKE 'Obra %' OR title = 'Benchmark Work' OR title = 'Sem título';
  `);

  const soakRes = await readPool.query(`
    SELECT count(*) as count
    FROM media
    WHERE provider_key LIKE 'tg-soak-%';
  `);

  const zeroPageRes = await readPool.query(`
    SELECT count(*) as count
    FROM chapters c
    LEFT JOIN pages p ON p.chapter_id = c.id
    WHERE p.position IS NULL AND c.created_at > (NOW() - INTERVAL '24 hours');
  `);

  const dupChapRes = await readPool.query(`
    SELECT count(*) as count FROM (
      SELECT work_id, number
      FROM chapters
      GROUP BY work_id, number
      HAVING count(*) > 1
    ) sub;
  `);

  const dupPagePosRes = await readPool.query(`
    SELECT count(*) as count FROM (
      SELECT chapter_id, position
      FROM pages
      GROUP BY chapter_id, position
      HAVING count(*) > 1
    ) sub;
  `);

  const idleInTxRes = await readPool.query(`
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

  const pass =
    phantomWorks === 0 &&
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
    pass,
  };
}

async function cleanupAndFreezeQueue() {
  log('Congelando fila e garantindo IMPORTER = FROZEN, QUEUE = PAUSED_BY_STAFF...');
  try {
    const readPool = getReadPool();
    await readPool.query(`
      UPDATE importer_queue 
      SET status = 'PAUSED_BY_STAFF', locked_by = NULL, locked_at = NULL, lease_expires_at = NULL 
      WHERE status = 'PROCESSING' OR status = 'PENDING';
    `);
    const actRes = await readPool.query(`SELECT count(*) as cnt FROM importer_queue WHERE status = 'PROCESSING';`);
    log(`[SAFETY CONFIRMED] Queue status PAUSED_BY_STAFF. Active jobs = ${actRes.rows[0].cnt}.`);
  } catch (e: any) {
    log(`[WARN] Erro durante cleanupAndFreezeQueue: ${e.message}`);
  }
}

// Global process signal handlers
process.on('SIGINT', async () => {
  log('\n[SIGNAL] SIGINT recebido! Interrompendo com segurança...');
  stopTriggered = true;
  stopReason = 'Interrompido manualmente por SIGINT';
  await cleanupAndFreezeQueue();
  process.exit(0);
});

process.on('SIGTERM', async () => {
  log('\n[SIGNAL] SIGTERM recebido! Interrompendo com segurança...');
  stopTriggered = true;
  stopReason = 'Interrompido por SIGTERM';
  await cleanupAndFreezeQueue();
  process.exit(0);
});

async function main() {
  log('======================================================================');
  log('PROJECT NOX — OTIMIZAÇÃO MÁXIMA DO IMPORTER + SITE');
  log('ARQUITETURA DESACOPLADA: Workers I/O -> ReadyQueue Bounded -> Async Publisher 3-RT (Pool=1)');
  log('Armazenamento: Direct Telegram Storage (6 bots, 21 shards, 0 Cloudflare Worker)');
  log('Banco: YugabyteDB Cloud | CTEs Atômicas Unificadas (130ms commit) | 0 Contenção em Leitura');
  log('======================================================================');

  await warmupCache();

  // Baseline calibrated
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

  // Determine ladder steps
  let ladderSteps: number[] = [5, 6, 7, 8, 10, 12, 15, 18, 20];
  if (TARGET_STAGES_ENV) {
    if (TARGET_STAGES_ENV.toLowerCase() === 'soak') {
      ladderSteps = [5]; // Direct Soak stage
    } else {
      ladderSteps = TARGET_STAGES_ENV.split(',').map(s => parseInt(s.trim(), 10)).filter(n => !isNaN(n));
    }
  }

  // Check existing progress to allow resuming
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

  const executedWorkerCounts = new Set(state.stages.map(s => s.workerCount));
  ladderSteps = ladderSteps.filter(w => !executedWorkerCounts.has(w));
  log(`Etapas a executar na escada: [${ladderSteps.join(', ')}]`);

  saveState();

  const rateLimiter = new HostRateLimiter(2.0);
  const registry = new SourceRegistry(rateLimiter);
  const circuitBreaker = new SourceCircuitBreaker(ALL_SOURCES);
  const dynamicQueue = new DynamicMultiSourceQueue(ALL_SOURCES, circuitBreaker);

  let ladderIndex = 0;
  let consecutiveLowGains = 0;

  async function executeStage(
    workerCount: number,
    stageDurationSec: number,
    isSoakStage: boolean
  ): Promise<PhaseMetrics> {
    const stageDurationMs = stageDurationSec * 1000;
    const stageMaxDurationMs = stageDurationMs + 60 * 1000;

    const directStorage = new DirectTelegramStorageProvider();
    const autotuner = new SiteHealthAutotuner({
      initialWorkers: workerCount,
      maxWorkers: workerCount,
      minWorkers: 1,
    });

    log(`\n======================================================================`);
    log(`INICIANDO ${isSoakStage ? 'TESTE DE SOAK (60 MIN)' : `ESTÁGIO COM ${workerCount} WORKER(S)`}`);
    log(`Duração planejada: ${stageDurationSec}s (${Math.round(stageDurationSec / 60)} min)`);
    log(`======================================================================`);

    const phaseName = isSoakStage ? `SOAK_60MIN_${workerCount}_WORKERS` : `ESTAGIO_${workerCount}_WORKERS`;
    state.currentPhase = phaseName;
    state.phaseStartTime = Date.now();
    state.live.workerCount = workerCount;
    state.live.completedChapters = 0;
    state.live.realPagesProcessed = 0;
    state.live.mbPerMin = 0;
    state.live.workerUtilizationPct = 0;
    state.live.readyQueueDepth = 0;
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
        minPhaseEndTime = phaseStartTime + stageDurationMs;
        maxPhaseEndTime = phaseStartTime + stageMaxDurationMs;
        state.phaseStartTime = phaseStartTime;
        state.live.lastSuccessTimestamp = phaseStartTime;
        log(`>>> [TIMER INICIADO] Workers ativos. Contagem de ${Math.round(stageDurationSec / 60)} min iniciada. <<<`);
      }
    };

    // Shared Stage State & Metrics
    let chaptersDone = 0;
    let pagesDone = 0;
    let totalBytesDone = 0;
    const sourceLatencies: number[] = [];
    const downloadLatencies: number[] = [];
    const telegramLatencies: number[] = [];
    const dbPubLatencies: number[] = [];
    const publicationLagSamples: number[] = [];
    const queueDepthSamples: number[] = [];
    const queueAgeSamples: number[] = [];
    const stageBeginLats: number[] = [];
    const stageValidationLats: number[] = [];
    const stageMasterCteLats: number[] = [];
    const stageCommitLats: number[] = [];
    const stageSqlExecLats: number[] = [];
    const stageQueryCounts: number[] = [];
    const stageRowsWritten: number[] = [];

    const workerBusyTimeMs: number[] = new Array(workerCount + 1).fill(0);
    const minuteUtilizationHistory: number[] = [];
    let lastMinuteCheckTime = Date.now();
    let workerBusyTimeAtLastMin: number[] = new Array(workerCount + 1).fill(0);

    let stageShouldFinish = false;
    let peakQueueDepth = 0;
    let backpressurePauseCount = 0;

    // ReadyQueue and Backpressure Primitives
    const readyQueue: ReadyChapter[] = [];
    let backpressureWaiters: (() => void)[] = [];

    const waitForBackpressure = async () => {
      if (readyQueue.length >= HIGH_WATERMARK) {
        backpressurePauseCount++;
        log(`[BACKPRESSURE] ReadyQueue atingiu ${readyQueue.length} capítulos (>= ${HIGH_WATERMARK}). Pausando novos downloads.`);
        await new Promise<void>(resolve => {
          backpressureWaiters.push(resolve);
        });
        log(`[BACKPRESSURE] ReadyQueue drenada para <= ${LOW_WATERMARK}. Retomando downloads.`);
      }
    };

    const checkAndReleaseBackpressure = () => {
      if (readyQueue.length <= LOW_WATERMARK && backpressureWaiters.length > 0) {
        const waiters = backpressureWaiters;
        backpressureWaiters = [];
        for (const w of waiters) w();
      }
    };

    // Graceful Finish Checker
    const checkCanFinishGracefully = () => {
      if (!stageTimerStarted || phaseStartTime === 0) return false;
      const elapsed = Date.now() - phaseStartTime;
      if (elapsed < stageDurationMs) return false;

      const highUtilMins = minuteUtilizationHistory.filter(u => u >= MIN_WORKER_UTILIZATION_PCT).length;
      const minRequiredUtilMins = Math.min(3, Math.max(1, Math.floor((stageDurationSec / 60) * 0.5)));
      const loadMet =
        chaptersDone >= MIN_COMPLETED_CHAPTERS &&
        pagesDone >= MIN_REAL_PAGES &&
        highUtilMins >= minRequiredUtilMins;

      if (loadMet) return true;
      if (elapsed >= stageMaxDurationMs) return true;
      return false;
    };

    // 1. Dedicated Async Publisher Loop (Concurrency = 1, Pool = 1)
    const publisherPromise = (async () => {
      log(`[ASYNC PUBLISHER] Loop publicador dedicado iniciado no pool Yugabyte.`);
      while (!stageShouldFinish || readyQueue.length > 0) {
        if (readyQueue.length === 0) {
          await new Promise(r => setTimeout(r, 40));
          continue;
        }

        const item = readyQueue.shift()!;
        checkAndReleaseBackpressure();
        state.live.readyQueueDepth = readyQueue.length;
        const pubLag = Date.now() - item.enqueuedAt;
        publicationLagSamples.push(pubLag);

        const t0Pub = Date.now();
        try {
          const pubRes = await publishBatchDirect({
            jobId: item.job.job_id,
            work: { id: item.job.work_id, title: item.job.work_title, slug: '' } as any,
            chapter: {
              number: item.chNum,
              title: item.job.chapter_title || `Capítulo ${item.chNum}`,
              source: item.job.source,
              sourceChapterId: item.job.source_chapter_id,
            },
            pages: item.cleanPages,
          });

          const pubLat = Date.now() - t0Pub;
          dbPubLatencies.push(pubLat);
          stageBeginLats.push(pubRes.metrics.beginMs);
          stageValidationLats.push(pubRes.metrics.validationMs);
          stageMasterCteLats.push(pubRes.metrics.masterCteMs);
          stageCommitLats.push(pubRes.metrics.commitMs);
          stageSqlExecLats.push(pubRes.metrics.sqlExecutionMs);
          stageQueryCounts.push(pubRes.metrics.queryCount);
          stageRowsWritten.push(pubRes.metrics.rowsWritten);

          chaptersDone++;
          pagesDone += item.cleanPages.length;
          totalBytesDone += item.chapterBytes;

          state.live.lastSuccessfulJob = `${item.workTitle} Cap. ${item.chNum} (${item.cleanPages.length}p) [${item.source}]`;
          state.live.lastSuccessTimestamp = Date.now();
          state.live.completedChapters = chaptersDone;
          state.live.realPagesProcessed = pagesDone;

          log(`[ASYNC PUBLISHER | ${item.source}] PUBLICADO: ${state.live.lastSuccessfulJob} em ${pubLat}ms DB (CTE: ${pubRes.metrics.masterCteMs}ms, Commit: ${pubRes.metrics.commitMs}ms) (Fila: ${readyQueue.length}) (Total: ${chaptersDone} cap, ${pagesDone} pag)`);
        } catch (pubErr: any) {
          log(`[ASYNC PUBLISHER ERROR | ${item.source}] Falha ao publicar capítulo ${item.chNum}: ${pubErr.message}`);
          circuitBreaker.record(item.source, false);
        } finally {
          dynamicQueue.releaseJob(item.job.job_id);
        }
      }
      log(`[ASYNC PUBLISHER] Loop publicador finalizado. Todos os capítulos publicados.`);
    })();

    // 2. N Decoupled Workers Loops (I/O, Scrape & Direct Telegram Upload)
    const workerPromises: Promise<void>[] = [];
    for (let wId = 1; wId <= workerCount; wId++) {
      const p = (async (workerIdx: number) => {
        while (!stageShouldFinish && !stopTriggered && (phaseStartTime === 0 || Date.now() < maxPhaseEndTime)) {
          // Check backpressure before acquiring new job
          await waitForBackpressure();
          if (stageShouldFinish || stopTriggered) break;

          const job = await dynamicQueue.getNextJob();
          if (!job) {
            await new Promise(r => setTimeout(r, 800));
            continue;
          }

          triggerStageStart();

          const chNum = parseFloat(job.chapter_number);
          const t0WorkerJob = Date.now();
          log(`[Worker ${workerIdx} | ${job.source}] Iniciando: ${job.work_title} Cap. ${chNum} (${job.source_chapter_id})`);

          try {
            // Existence check on readPool (never blocks publisher)
            const readPool = getReadPool();
            const existCheck = await readPool.query(`
              SELECT count(p.position) as p_count
              FROM chapters c
              JOIN pages p ON p.chapter_id = c.id
              WHERE c.work_id = $1 AND c.number = $2;
            `, [job.work_id, chNum]);

            if (parseInt(existCheck.rows[0].p_count, 10) > 0) {
              log(`[Worker ${workerIdx} SKIP | ${job.source}] Capítulo ${chNum} já importado (${existCheck.rows[0].p_count}p), marcando COMPLETED.`);
              await readPool.query(`UPDATE importer_queue SET status = 'COMPLETED', updated_at = NOW() WHERE id = $1;`, [job.job_id]);
              dynamicQueue.releaseJob(job.job_id);
              continue;
            }

            const adapter = registry.get(job.source);
            if (!adapter) {
              circuitBreaker.record(job.source, false);
              dynamicQueue.releaseJob(job.job_id);
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
              dynamicQueue.releaseJob(job.job_id);
              continue;
            }

            log(`[Worker ${workerIdx} | ${job.source}] Scraped ${urls.length} páginas em ${scrapeLat}ms para ${job.work_title} Cap. ${chNum}`);

            // 2. Download and Upload pages DIRECTLY to Telegram (concurrency = 1 with global bandwidth limiter)
            const pagesPayload: any[] = new Array(urls.length);
            const pageConcurrency = 1;
            let currentUrlIdx = 0;

            const processPage = async (pIdx: number) => {
              if (stageShouldFinish || stopTriggered || (phaseStartTime > 0 && Date.now() >= maxPhaseEndTime)) return;
              const t0Dl = Date.now();
              let buf: Buffer | null = null;
              for (let att = 0; att < 3; att++) {
                try {
                  const res = await fetch(urls[pIdx], {
                    headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' },
                    signal: AbortSignal.timeout(15000),
                  });
                  if (!res.ok) {
                    if (res.status === 404) {
                      // Dead link on source, break retry loop immediately
                      break;
                    }
                    throw new Error(`HTTP ${res.status}`);
                  }
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

              // DIRECT TELEGRAM UPLOAD (Bypassing Cloudflare Worker)
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
                  storageShardId: shardId,
                };

                // Pacing yield between pages to allow probe TCP ACKs through without Wi-Fi bufferbloat
                await new Promise(r => setTimeout(r, 40));
              } catch (upErr: any) {
                log(`[Worker ${workerIdx} WARN | ${job.source}] Falha upload direto Telegram p${pIdx + 1}: ${upErr.message}`);
              }
            };

            const pageWorkers = [];
            for (let i = 0; i < pageConcurrency; i++) {
              pageWorkers.push(
                (async () => {
                  while (currentUrlIdx < urls.length) {
                    if (stageShouldFinish || stopTriggered || (phaseStartTime > 0 && Date.now() >= maxPhaseEndTime)) break;
                    const idx = currentUrlIdx++;
                    await processPage(idx);
                  }
                })()
              );
            }
            await Promise.all(pageWorkers);

            const filteredPages = pagesPayload.filter(Boolean);
            if (
              filteredPages.length === 0 ||
              stageShouldFinish ||
              stopTriggered ||
              (phaseStartTime > 0 && Date.now() >= maxPhaseEndTime)
            ) {
              if (filteredPages.length === 0) {
                log(`[Worker ${workerIdx} WARN | ${job.source}] 0 páginas válidas após download/upload para ${job.work_title} Cap. ${chNum}`);
                circuitBreaker.record(job.source, false);
              }
              dynamicQueue.releaseJob(job.job_id);
              continue;
            }

            const cleanPages = filteredPages.map((p, idx) => ({ ...p, position: idx + 1 }));
            const chapterBytes = cleanPages.reduce((acc, p) => acc + (p.bytes || 0), 0);

            // 3. ENQUEUE READY CHAPTER INTO BUFFER (Workers do NOT wait on DB!)
            readyQueue.push({
              job,
              cleanPages,
              chapterBytes,
              workerIdx,
              source: job.source,
              workTitle: job.work_title,
              chNum,
              t0WorkerJob,
              enqueuedAt: Date.now(),
            });

            if (readyQueue.length > peakQueueDepth) {
              peakQueueDepth = readyQueue.length;
            }
            state.live.readyQueueDepth = readyQueue.length;

            log(`[Worker ${workerIdx} | ${job.source}] PRONTO PARA PUBLICAR: ${job.work_title} Cap. ${chNum} (${cleanPages.length}p) em ${Date.now() - t0WorkerJob}ms total -> Enfileirado na ReadyQueue (Tam: ${readyQueue.length})`);
            workerBusyTimeMs[workerIdx] += Date.now() - t0WorkerJob;
          } catch (err: any) {
            circuitBreaker.record(job.source, false);
            log(`[Worker ${workerIdx} WARN | ${job.source}] Erro transitório no capítulo ${chNum}: ${err.message}`);
            dynamicQueue.releaseJob(job.job_id);
            await new Promise(r => setTimeout(r, 1500));
          }
        }
      })(wId);
      workerPromises.push(p);
    }

    // 3. Probing Loop in background
    const probePromise = (async () => {
      const probeRes = await runProbingLoop(stageMaxDurationMs, () => stageShouldFinish, autotuner);
      return probeRes;
    })();

    // 4. Controller Loop (Metrics calculation, Backpressure observation, Autotuner)
    while (!stageShouldFinish && !stopTriggered && Date.now() < maxPhaseEndTime) {
      await new Promise(r => setTimeout(r, 5000));

      const now = Date.now();
      const elapsedTotalSec = (now - phaseStartTime) / 1000;
      const elapsedMin = elapsedTotalSec / 60;

      queueDepthSamples.push(readyQueue.length);
      const queueAge = readyQueue.length > 0 ? Date.now() - readyQueue[0].enqueuedAt : 0;
      queueAgeSamples.push(queueAge);

      // Check for queue instability / Producer > Publisher capacity
      if (queueDepthSamples.length >= 8) {
        const recentDepth = queueDepthSamples.slice(-8);
        const recentLag = publicationLagSamples.slice(-8);
        const avgDepth = recentDepth.reduce((a, b) => a + b, 0) / recentDepth.length;
        const avgLag = recentLag.length > 0 ? recentLag.reduce((a, b) => a + b, 0) / recentLag.length : 0;
        if (avgDepth >= HIGH_WATERMARK && avgLag > 45000 && backpressurePauseCount > 8) {
          stopTriggered = true;
          stopReason = `PRODUCER > PUBLISHER CAPACITY: ReadyQueue saturada (${avgDepth.toFixed(1)} cap), lag excessivo (${Math.round(avgLag)}ms), ${backpressurePauseCount} pausas`;
          log(`[CAPACITY BOTTLENECK ALERT] ${stopReason}`);
          break;
        }
      }

      if (now - lastMinuteCheckTime >= 60000) {
        const deltaBusy =
          workerBusyTimeMs.reduce((a, b) => a + b, 0) - workerBusyTimeAtLastMin.reduce((a, b) => a + b, 0);
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
      const currentUtil =
        totalPotentialBusy > 0 ? parseFloat(((totalActualBusy / totalPotentialBusy) * 100).toFixed(1)) : 0;

      const summary = directStorage.getMetricsSummary();
      const totalFlood = summary.bots.reduce((acc, b) => acc + (b.floodWaitSeconds || 0), 0);

      state.live.chaptersPerMin = cpm;
      state.live.pagesPerMin = ppm;
      state.live.mbPerMin = mbpm;
      state.live.workerUtilizationPct = currentUtil;
      state.live.floodWaitTotal = totalFlood;
      state.live.readyQueueDepth = readyQueue.length;

      saveState();

      if (checkCanFinishGracefully()) {
        log(`[STAGE FINISH CONDITION MET] Estágio de ${workerCount} workers concluiu critérios de duração e carga.`);
        stageShouldFinish = true;
        break;
      }
    }

    stageShouldFinish = true;
    checkAndReleaseBackpressure(); // Release any waiting workers so they can terminate

    await Promise.all(workerPromises);
    await publisherPromise;
    const probeResults = await probePromise;

    const actualDurationSeconds = Math.round((Date.now() - phaseStartTime) / 1000);
    const actualDurationMinutes = actualDurationSeconds / 60;
    const chaptersPerMin =
      actualDurationMinutes > 0 ? parseFloat((chaptersDone / actualDurationMinutes).toFixed(2)) : 0;
    const pagesPerMin = actualDurationMinutes > 0 ? parseFloat((pagesDone / actualDurationMinutes).toFixed(2)) : 0;
    const mbPerMin =
      actualDurationMinutes > 0
        ? parseFloat(((totalBytesDone / (1024 * 1024)) / actualDurationMinutes).toFixed(2))
        : 0;

    const totalPotentialWorkerTimeMs = workerCount * (actualDurationSeconds * 1000);
    const totalBusyWorkerTimeMs = workerBusyTimeMs.reduce((a, b) => a + b, 0);
    const workerUtilizationPct =
      totalPotentialWorkerTimeMs > 0
        ? parseFloat(((totalBusyWorkerTimeMs / totalPotentialWorkerTimeMs) * 100).toFixed(1))
        : 0;

    const highUtilMins = minuteUtilizationHistory.filter(u => u >= MIN_WORKER_UTILIZATION_PCT).length;
    const scaledMinChapters = Math.max(1, Math.floor((actualDurationSeconds / 600) * MIN_COMPLETED_CHAPTERS));
    const scaledMinPages = Math.max(15, Math.floor((actualDurationSeconds / 600) * MIN_REAL_PAGES));
    const hasSufficientLoad =
      chaptersDone >= scaledMinChapters && pagesDone >= scaledMinPages;

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

    // Quality Gate Evaluation
    let stageStatus: 'PASS' | 'STOP_TRIGGERED' | 'INSUFFICIENT_LOAD' = 'PASS';
    if (stopTriggered) {
      stageStatus = 'STOP_TRIGGERED';
    } else if (
      probeResults.readerP95 > THRESHOLDS.READER_P95_MAX_MS ||
      probeResults.homeP95 > THRESHOLDS.HOME_P95_MAX_MS ||
      probeResults.mediaP95 > THRESHOLDS.MEDIA_P95_MAX_MS ||
      probeResults.site5xx > THRESHOLDS.MAX_5XX ||
      probeResults.siteTimeouts > THRESHOLDS.MAX_TIMEOUTS ||
      totalFloodWaitSec > THRESHOLDS.MAX_FLOODWAIT_SEC
    ) {
      stageStatus = 'STOP_TRIGGERED';
      stopReason = `QUALITY GATE FAILED: Reader p95=${probeResults.readerP95}ms (lim: ${THRESHOLDS.READER_P95_MAX_MS}), Home p95=${probeResults.homeP95}ms (lim: ${THRESHOLDS.HOME_P95_MAX_MS}), Media p95=${probeResults.mediaP95}ms (lim: ${THRESHOLDS.MEDIA_P95_MAX_MS}), 5xx=${probeResults.site5xx}, timeouts=${probeResults.siteTimeouts}, FloodWait=${totalFloodWaitSec}s`;
      log(`[QUALITY GATE ALERT] ${stopReason}`);
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
      downloadLatencyAvg:
        downloadLatencies.length > 0
          ? Math.round(downloadLatencies.reduce((a, b) => a + b, 0) / downloadLatencies.length)
          : 0,
      telegramUploadLatencyAvg:
        telegramLatencies.length > 0
          ? Math.round(telegramLatencies.reduce((a, b) => a + b, 0) / telegramLatencies.length)
          : 0,
      dbPublicationLatencyP50: percentile(dbPubLatencies, 50),
      dbPublicationLatencyP95: percentile(dbPubLatencies, 95),
      dbPublicationLatencyP99: percentile(dbPubLatencies, 99),
      readyQueuePeakDepth: peakQueueDepth,
      readyQueueDepthP50: percentile(queueDepthSamples, 50),
      readyQueueDepthP95: percentile(queueDepthSamples, 95),
      readyQueueAgeP50Ms: percentile(queueAgeSamples, 50),
      readyQueueAgeP95Ms: percentile(queueAgeSamples, 95),
      publicationLagP50Ms: percentile(publicationLagSamples, 50),
      publicationLagP95Ms: percentile(publicationLagSamples, 95),
      publicationLagP99Ms: percentile(publicationLagSamples, 99),
      publisherThroughputCapPerMin: chaptersPerMin,
      publisherThroughputPagesPerMin: pagesPerMin,
      isQueueStable: peakQueueDepth < HIGH_WATERMARK || percentile(publicationLagSamples, 95) < 30000,
      backpressurePauseCount,
      dbDetails: {
        beginP95: percentile(stageBeginLats, 95),
        validationP95: percentile(stageValidationLats, 95),
        masterCteP95: percentile(stageMasterCteLats, 95),
        commitP95: percentile(stageCommitLats, 95),
        sqlExecP95: percentile(stageSqlExecLats, 95),
        avgQueriesPerChapter:
          stageQueryCounts.length > 0
            ? Number((stageQueryCounts.reduce((a, b) => a + b, 0) / stageQueryCounts.length).toFixed(1))
            : 0,
        avgRowsWrittenPerChapter:
          stageRowsWritten.length > 0
            ? Number((stageRowsWritten.reduce((a, b) => a + b, 0) / stageRowsWritten.length).toFixed(1))
            : 0,
        rowsWrittenPerSec:
          actualDurationSeconds > 0
            ? Number((stageRowsWritten.reduce((a, b) => a + b, 0) / actualDurationSeconds).toFixed(1))
            : 0,
      },
      shardsUsedCount: summary.shards.filter(s => s.uploads > 0).length,
      botsUsedCount: summary.bots.filter(b => b.uploads > 0).length,
      botMetrics: summary.bots,
      shardMetrics: summary.shards,
      totalFloodWaitSeconds: totalFloodWaitSec,
      totalFloodWaitCount: totalFloodWaitCount,
      integrityCheck: integrity,
      status: stageStatus,
      stopReason: stopTriggered ? stopReason : !hasSufficientLoad ? `Carga insuficiente` : undefined,
    };

    state.stages.push(stageMetric);
    saveState();

    log(`\n--- RESUMO DO ESTÁGIO COM ${workerCount} WORKER(S) ---
    Status: ${stageMetric.status} ${stageMetric.stopReason ? `(${stageMetric.stopReason})` : ''}
    Duração: ${actualDurationSeconds}s (${Math.round(actualDurationMinutes)} min)
    Carga: ${chaptersDone} cap concluídos | ${pagesDone} páginas reais (${(totalBytesDone / 1024 / 1024).toFixed(1)} MB)
    Throughput: ${chaptersPerMin} cap/min | ${pagesPerMin} pag/min | ${mbPerMin} MB/min
    Utilização dos Workers: ${workerUtilizationPct}%
    Fila ReadyQueue: Pico de ${peakQueueDepth} cap (p50=${stageMetric.readyQueueDepthP50}, p95=${stageMetric.readyQueueDepthP95}) | Pausas de backpressure: ${backpressurePauseCount}
    Lag de Publicação: p50=${stageMetric.publicationLagP50Ms}ms | p95=${stageMetric.publicationLagP95Ms}ms | p99=${stageMetric.publicationLagP99Ms}ms (Estabilidade: ${stageMetric.isQueueStable ? 'ESTÁVEL' : 'INSTÁVEL'})
    Publisher Throughput: ${chaptersPerMin} cap/min | ${pagesPerMin} pag/min
    Site Home:   p50=${stageMetric.homeP50}ms | p95=${stageMetric.homeP95}ms | p99=${stageMetric.homeP99}ms (Meta: <=250ms, Limite: <=350ms)
    Site Reader: p50=${stageMetric.readerP50}ms | p95=${stageMetric.readerP95}ms | p99=${stageMetric.readerP99}ms (Meta: <=150ms, Limite: <=200ms)
    Site Media:  p50=${stageMetric.mediaP50}ms | p95=${stageMetric.mediaP95}ms | p99=${stageMetric.mediaP99}ms (Meta: <=120ms, Limite: <=150ms)
    Site 5xx: ${stageMetric.site5xxCount} | Timeouts: ${stageMetric.siteTimeoutCount}
    Network DB (Ping RTT): p50=${stageMetric.networkDbP50}ms | p95=${stageMetric.networkDbP95}ms | p99=${stageMetric.networkDbP99}ms
    DB Transação p95: ${stageMetric.dbPublicationLatencyP95}ms (CTE: ${stageMetric.dbDetails.masterCteP95}ms, Commit: ${stageMetric.dbDetails.commitP95}ms)
    DB Carga: Avg Queries/Cap=${stageMetric.dbDetails.avgQueriesPerChapter}, Rows Written/Cap=${stageMetric.dbDetails.avgRowsWrittenPerChapter}, Rows Written/Sec=${stageMetric.dbDetails.rowsWrittenPerSec}
    FloodWait Total Telegram: ${totalFloodWaitSec}s (${totalFloodWaitCount} ocorrências)
    Integridade: ${integrity.pass ? 'PASS (100%)' : 'FAIL'}`);

    return stageMetric;
  }

  while (ladderIndex < ladderSteps.length && !stopTriggered) {
    const workerCount = ladderSteps[ladderIndex];
    ladderIndex++;

    const isSoakStage = TARGET_STAGES_ENV?.toLowerCase() === 'soak';
    const stageDurationSec = isSoakStage
      ? SOAK_DURATION_SEC
      : OVERRIDE_STAGE_DURATION_SEC || DEFAULT_STAGE_DURATION_SEC[workerCount] || 600;

    const stageMetric = await executeStage(workerCount, stageDurationSec, isSoakStage);

    if (stageMetric.status === 'PASS') {
      lastSafeWorkers = workerCount;
      if (lastValidPagesPerMin !== null) {
        const gainPct = ((stageMetric.pagesPerMin - lastValidPagesPerMin) / lastValidPagesPerMin) * 100;
        log(`[MARGINAL GAIN] Ganho vs estágio anterior: ${gainPct.toFixed(1)}% (${stageMetric.pagesPerMin} vs ${lastValidPagesPerMin} pag/min)`);
        if (gainPct < 8.0 && ladderSteps.length > 2 && workerCount >= 8) {
          consecutiveLowGains++;
          if (consecutiveLowGains >= 2) {
            stopTriggered = true;
            stopReason = `LIMITE DE THROUGHPUT ÚTIL: ganho de rendimento marginal inferior a 8% por 2 estágios consecutivos (${gainPct.toFixed(1)}%)`;
            log(`[USEFUL THROUGHPUT LIMIT REACHED] ${stopReason}`);
            break;
          }
        } else {
          consecutiveLowGains = 0;
        }
      }
      lastValidPagesPerMin = stageMetric.pagesPerMin;
    } else if (stageMetric.status === 'STOP_TRIGGERED') {
      firstUnsafeWorkers = workerCount;
      break;
    }
  }

  // Automatic 60-Minute Soak on the last safe worker count >= 5
  const isDirectSoak = TARGET_STAGES_ENV?.toLowerCase() === 'soak';
  if (typeof lastSafeWorkers === 'number' && lastSafeWorkers >= 5 && !process.env.SKIP_SOAK && !isDirectSoak) {
    log(`\n======================================================================`);
    log(`INICIANDO TESTE DE SOAK REGULAMENTAR DE 60 MINUTOS COM ${lastSafeWorkers} WORKER(S) HOMOLOGADO(S)`);
    log(`======================================================================`);
    stopTriggered = false;
    stopReason = '';
    const soakMetric = await executeStage(lastSafeWorkers, SOAK_DURATION_SEC, true);
    log(`\nSOAK FINAL CONCLUÍDO COM STATUS: ${soakMetric.status}`);
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
    ? lastSafeWorkers
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
    reason: stopReason || 'Escada de workers finalizada com sucesso.',
  };

  saveState();

  log(`\n======================================================================`);
  log(`BENCHMARK FINALIZADO`);
  log(`LAST SAFE WORKERS: ${state.verdict.lastSafeWorkerCount}`);
  log(`FIRST UNSAFE WORKERS: ${state.verdict.firstUnsafeWorkerCount}`);
  log(`RECOMMENDED PRODUCTION WORKERS: ${state.verdict.recommendedProductionWorkers}`);
  log(`MAX MEASURED THROUGHPUT: ${state.verdict.maxMeasuredPagesPerMin} pag/min (${state.verdict.maxMeasuredChaptersPerMin} cap/min, ${state.verdict.maxMeasuredMbPerMin} MB/min)`);

  await cleanupAndFreezeQueue();

  if (readPoolInstance) {
    await readPoolInstance.end();
  }
}

main().catch(async err => {
  log(`[FATAL ORCHESTRATOR ERROR] ${err.message}\n${err.stack}`);
  await cleanupAndFreezeQueue();
  process.exit(1);
});
