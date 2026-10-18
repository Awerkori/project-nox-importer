// scripts/retire-blocked-sources-production.ts
import dotenv2 from "dotenv";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

// src/db/yugabyte-direct.ts
import pg from "pg";
import fs2 from "fs";
import path2 from "path";
import { fileURLToPath as fileURLToPath2 } from "url";

// src/config.ts
import { z } from "zod";
import dotenv from "dotenv";
import path from "path";
import fs from "fs";
import { fileURLToPath } from "url";
var candidateEnvPaths = [
  path.resolve(process.cwd(), ".env"),
  "/home/node/.env",
  "/home/node/app/.env"
];
try {
  const currentDir = path.dirname(fileURLToPath(import.meta.url));
  candidateEnvPaths.push(path.resolve(currentDir, "../.env"));
  candidateEnvPaths.push(path.resolve(currentDir, "../../.env"));
} catch {
}
for (const envPath of candidateEnvPaths) {
  if (fs.existsSync(envPath)) {
    dotenv.config({ path: envPath });
  }
}
dotenv.config();
var ConfigSchema = z.object({
  SUPABASE_URL: z.string().url().default("https://placeholder.supabase.co"),
  SUPABASE_SERVICE_ROLE_KEY: z.string().min(1).default("placeholder-service-role-key"),
  STORAGE_PROVIDER: z.enum(["worker", "telegram", "mock", "direct_telegram"]).default("direct_telegram"),
  NOX_STORAGE_BRIDGE_TOKEN: z.string().optional(),
  NOX_MANGA_URL: z.string().url().default("https://manga.project-nox-awerkori.workers.dev"),
  NOX_IMPORTER_GATEWAY_URL: z.string().url().default("https://project-nox-importer-gateway.project-nox-awerkori.workers.dev"),
  TELEGRAM_BOT_TOKEN: z.string().optional(),
  TELEGRAM_CHAT_ID: z.string().optional(),
  IMPORTER_USER_ID: z.string().uuid().optional(),
  WORKER_ID: z.string().min(1).default(() => `nox-worker-${process.pid}-${Math.random().toString(36).slice(2, 7)}`),
  POLL_INTERVAL_SECONDS: z.coerce.number().int().min(5).default(60),
  QUEUE_LEASE_DURATION_SECONDS: z.coerce.number().int().min(30).default(600),
  QUEUE_HEARTBEAT_INTERVAL_SECONDS: z.coerce.number().int().min(10).default(60),
  MAX_CONCURRENT_CHAPTERS: z.coerce.number().int().min(1).max(128).default(10),
  // Lite does not inherit the legacy chapter-worker setting. The controlled
  // eighth-slot ramp remains inside the 24-request/page-buffer fences: it
  // fills gaps between bounded six-page waves rather than multiplying
  // download, buffer, Telegram or DB concurrency. An explicit protected
  // value still wins immediately (including a one-worker rollback) without
  // reviving legacy behavior.
  LITE_CHAPTER_WORKERS: z.coerce.number().int().min(1).max(32).default(8),
  ADAPTIVE_INITIAL_CONCURRENCY: z.coerce.number().int().min(1).max(128).default(3),
  // Raising this ceiling requires a measured production ramp. Legacy MAX=32 cannot override it.
  TESTED_CONCURRENCY_CEILING: z.coerce.number().int().min(1).max(128).default(32),
  BATCH_PAGE_DOWNLOAD_CONCURRENCY: z.coerce.number().int().min(1).max(32).default(16),
  // Keep the previously measured pool=3 ceiling. A controlled max=4 window
  // lowered acquisition wait but raised SQL p95 to 411ms, transaction p95 to
  // 1.70s, RSS to 261MiB and CPU to 22.5%, without increasing publications.
  DIRECT_DB_POOL_MAX: z.coerce.number().int().min(1).max(30).default(3),
  // At the measured 52.75 MiB median chapter size, the former 4 MiB/s global
  // ceiling could never reach the 8/min acceptance floor (4.55/min before
  // network and publication overhead). Eight MiB/s is the first reversible
  // ramp; bot, shard, request and global-media limits still apply.
  UPLOAD_RATE_LIMIT_BYTES_PER_SEC: z.coerce.number().int().default(8 * 1024 * 1024),
  // These are global pipeline ceilings, not per-chapter multipliers.  Keep
  // them aligned with the small production container so a chapter ramp cannot
  // create a hidden 24+ request/media burst behind five chapter slots.
  TELEGRAM_MEDIA_CONCURRENCY: z.coerce.number().int().min(1).max(32).default(24),
  // Production ramp 2026-10-11: real P1 media flow sustained only 70–128
  // pages/min at the former 16-page fence, while Telegram stayed below
  // 1.6 MiB/s with no 429 and the 24-upload/storage fences remained unused.
  // A 32-page production ramp regressed to 143 pages/min and increased RSS
  // without Telegram 429. Keep the measured 24-page fence until a distinct
  // bottleneck is proven; uploads remain bounded at 24 and buffers at 48.
  DOWNLOAD_INFLIGHT_CONCURRENCY: z.coerce.number().int().min(1).max(32).default(24),
  BUFFERED_PAGE_CONCURRENCY: z.coerce.number().int().min(1).max(64).default(48),
  // Upstream adapter calls are network work, not a reason to monopolize a
  // chapter slot. The timeout is deliberately independent from the durable
  // task lease and results in capped retry/cooldown handling.
  SOURCE_OPERATION_TIMEOUT_MS: z.coerce.number().int().min(5e3).max(12e4).default(3e4),
  LOG_LEVEL: z.enum(["debug", "info", "warn", "error"]).default("info"),
  IMPORTER_DB_MODE: z.enum(["direct", "gateway"]).default("direct"),
  // This branch is the approved Lite cutover artifact.  An explicit `legacy`
  // setting remains available only for the documented rollback procedure; a
  // fresh deployment must never silently revive the v1 scheduler because an
  // environment variable was omitted by the native DIScloud Git integration.
  IMPORTER_RUNTIME_MODE: z.enum(["legacy", "lite"]).default("lite"),
  YUGABYTE_HOST: z.string().default("sa-east-1.b49305ea-8536-43e6-936e-b2fd77fc07b0.aws.yugabyte.cloud"),
  YUGABYTE_PORT: z.coerce.number().default(5433),
  YUGABYTE_USER: z.string().default("admin"),
  YUGABYTE_PASSWORD: z.string().min(1, "YUGABYTE_PASSWORD environment variable is required"),
  YUGABYTE_DATABASE: z.string().default("project_nox_prod"),
  YUGABYTE_SSL_CERT: z.string().default("certs/yugabyte-root.crt")
});
var _config = null;
function getConfig() {
  if (!_config) {
    const result = ConfigSchema.safeParse(process.env);
    if (!result.success) {
      console.error("Invalid configuration:", result.error.format());
      throw new Error("Invalid environment configuration");
    }
    _config = result.data;
  }
  return _config;
}

// src/core/logger.ts
var LEVEL_PRIORITY = {
  debug: 10,
  info: 20,
  warn: 30,
  error: 40
};
function sanitize(val) {
  if (typeof val === "string") {
    return val.replace(/bot[0-9]+:[a-zA-Z0-9_-]{20,}/g, "bot[REDACTED]").replace(/eyJ[a-zA-Z0-9_-]{20,}\.[a-zA-Z0-9_-]{20,}\.[a-zA-Z0-9_-]{20,}/g, "[JWT_REDACTED]").replace(/(apikey|token|password|secret|bearer)\s*[:=]\s*["']?[^"'\s,]+["']?/gi, "$1=[REDACTED]");
  }
  if (val && typeof val === "object") {
    if (Array.isArray(val)) {
      return val.map(sanitize);
    }
    const sanitizedObj = {};
    for (const [k, v] of Object.entries(val)) {
      const lowerKey = k.toLowerCase();
      if (lowerKey.includes("token") || lowerKey.includes("secret") || lowerKey.includes("password") || lowerKey.includes("apikey") || lowerKey.includes("auth")) {
        sanitizedObj[k] = "[REDACTED]";
      } else {
        sanitizedObj[k] = sanitize(v);
      }
    }
    return sanitizedObj;
  }
  return val;
}
var Logger = class _Logger {
  constructor(context, minLevel = "info") {
    this.context = context;
    this.minLevel = minLevel;
  }
  context;
  minLevel;
  shouldLog(level) {
    return LEVEL_PRIORITY[level] >= LEVEL_PRIORITY[this.minLevel];
  }
  format(level, message, meta) {
    const timestamp = (/* @__PURE__ */ new Date()).toISOString();
    const entry = {
      timestamp,
      level: level.toUpperCase(),
      context: this.context,
      message
    };
    if (meta !== void 0) {
      entry.meta = sanitize(meta);
    }
    return JSON.stringify(entry);
  }
  debug(message, meta) {
    if (this.shouldLog("debug")) {
      console.debug(this.format("debug", message, meta));
    }
  }
  info(message, meta) {
    if (this.shouldLog("info")) {
      console.info(this.format("info", message, meta));
    }
  }
  warn(message, meta) {
    if (this.shouldLog("warn")) {
      console.warn(this.format("warn", message, meta));
    }
  }
  error(message, meta) {
    if (this.shouldLog("error")) {
      console.error(this.format("error", message, meta));
    }
  }
  child(subContext) {
    return new _Logger(`${this.context}:${subContext}`, this.minLevel);
  }
};
var rootLogger = new Logger("NoxImporter");

// src/core/telemetry-collector.ts
import { performance, PerformanceObserver } from "node:perf_hooks";

// src/core/bounded-samples.ts
var BoundedSamples = class extends Array {
  constructor(capacity = 2048) {
    super();
    this.capacity = capacity;
  }
  capacity;
  cursor = 0;
  static get [Symbol.species]() {
    return Array;
  }
  push(...items) {
    for (const item of items) {
      if (this.length < this.capacity) super.push(item);
      else {
        this[this.cursor] = item;
        this.cursor = (this.cursor + 1) % this.capacity;
      }
    }
    return this.length;
  }
};

// src/core/maintenance-scheduler.ts
var MaintenanceScheduler = class {
  tasks = /* @__PURE__ */ new Map();
  timer = null;
  running = false;
  stopped = false;
  register(name, intervalMs, initialDelayMs, run) {
    if (this.tasks.has(name)) return;
    this.stopped = false;
    this.tasks.set(name, {
      intervalMs,
      run,
      nextRun: Date.now() + initialDelayMs,
      lastStarted: null,
      lastFinished: null,
      durationMs: 0,
      rowsTouched: null,
      running: false,
      failures: 0
    });
    this.schedule();
  }
  schedule() {
    if (this.stopped || this.running || !this.tasks.size) return;
    if (this.timer) clearTimeout(this.timer);
    const next = Math.min(...[...this.tasks.values()].map((t) => t.nextRun));
    this.timer = setTimeout(() => {
      this.timer = null;
      void this.tick();
    }, Math.max(0, next - Date.now()));
    this.timer.unref();
  }
  async tick() {
    if (this.stopped || this.running) return;
    const task = [...this.tasks.values()].filter((t) => t.nextRun <= Date.now()).sort((a, b) => a.nextRun - b.nextRun)[0];
    if (!task) {
      this.schedule();
      return;
    }
    this.running = task.running = true;
    task.lastStarted = Date.now();
    try {
      task.rowsTouched = await task.run() ?? null;
    } catch {
      task.failures++;
    } finally {
      task.lastFinished = Date.now();
      task.durationMs = task.lastFinished - task.lastStarted;
      task.nextRun = Date.now() + task.intervalMs + Math.floor(Math.random() * Math.min(2e3, task.intervalMs * 0.1));
      this.running = task.running = false;
      this.schedule();
    }
  }
  snapshot() {
    return Object.fromEntries([...this.tasks].map(([name, t]) => [name, {
      lastStarted: t.lastStarted,
      lastFinished: t.lastFinished,
      durationMs: t.durationMs,
      rowsTouched: t.rowsTouched,
      nextRun: t.nextRun,
      currentlyRunning: t.running,
      failures: t.failures
    }]));
  }
  stop() {
    this.stopped = true;
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
    this.tasks.clear();
  }
};
var maintenanceScheduler = new MaintenanceScheduler();

// src/core/telemetry-collector.ts
import { Session as InspectorSession } from "node:inspector";
function percentile(arr, p) {
  if (!arr || arr.length === 0) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const idx = Math.min(sorted.length - 1, Math.floor(sorted.length * p));
  return Math.round(sorted[idx] * 10) / 10;
}
function avg(arr) {
  if (!arr || arr.length === 0) return 0;
  const sum = arr.reduce((a, b) => a + b, 0);
  return Math.round(sum / arr.length * 10) / 10;
}
var TelemetryCollector = class _TelemetryCollector {
  static instance;
  logger = new Logger("TelemetryCollector");
  activeSessionId = null;
  sessionStartTime = 0;
  sessionExpiresAt = 0;
  configuredChapterSlots = 0;
  effectiveCapacity = () => this.configuredChapterSlots;
  flushing = false;
  nextSessionCheck = 0;
  lastFlushWarningAt = 0;
  gcObserver = null;
  runtimeFingerprint = {};
  cpuProfile = null;
  profiledSession = null;
  async captureBoundedCpuProfile(sessionId) {
    if (this.profiledSession === sessionId) return;
    this.profiledSession = sessionId;
    const session = new InspectorSession();
    const post = (method, params = {}) => new Promise((resolve2, reject) => session.post(method, params, (error, result) => error ? reject(error) : resolve2(result)));
    try {
      session.connect();
      await post("Profiler.enable");
      await post("Profiler.setSamplingInterval", { interval: 2e3 });
      await post("Profiler.start");
      await new Promise((resolve2) => setTimeout(resolve2, 5e3));
      const { profile } = await post("Profiler.stop");
      const nodes = new Map(profile.nodes.map((n) => [n.id, n]));
      const stacks = /* @__PURE__ */ new Map();
      let totalUs = 0;
      for (let i = 0; i < (profile.samples?.length || 0); i++) {
        const node = nodes.get(profile.samples[i]);
        const frame = node?.callFrame;
        const name = String(frame?.functionName || "(anonymous)").slice(0, 120);
        const file = String(frame?.url || "").split(/[/?#]/).pop()?.slice(0, 100) || "";
        const key = `${name} @ ${file}:${frame?.lineNumber ?? 0}`;
        const us = profile.timeDeltas?.[i] || 0;
        totalUs += us;
        stacks.set(key, (stacks.get(key) || 0) + us);
      }
      this.cpuProfile = {
        sessionId,
        durationMs: (profile.endTime - profile.startTime) / 1e3,
        samples: profile.samples?.length || 0,
        topStacks: [...stacks].sort((a, b) => b[1] - a[1]).slice(0, 20).map(([frame, us]) => ({ frame, percent: totalUs ? Math.round(us / totalUs * 1e3) / 10 : 0 }))
      };
    } catch (error) {
      this.cpuProfile = { sessionId, error: error?.code || error?.name };
    } finally {
      session.disconnect();
    }
  }
  setRuntimeFingerprint(value) {
    this.runtimeFingerprint = value;
  }
  limiterProviders = /* @__PURE__ */ new Map();
  configureChapterSlots(count2, effectiveCapacity = () => count2) {
    this.configuredChapterSlots = Math.max(0, Math.floor(count2));
    this.effectiveCapacity = effectiveCapacity;
    for (const index of this.slots.keys()) if (index >= count2) this.slots.delete(index);
  }
  registerLimiter(name, snapshot) {
    if (this.limiterProviders.size < 128 || this.limiterProviders.has(name)) this.limiterProviders.set(name, snapshot);
  }
  unregisterLimiter(name) {
    this.limiterProviders.delete(name);
    this.limiters.delete(name);
  }
  stop() {
    if (this.samplerTimer) clearTimeout(this.samplerTimer);
    if (this.flushTimer) clearInterval(this.flushTimer);
    this.gcObserver?.disconnect();
    this.samplerTimer = this.flushTimer = null;
  }
  // 1. Slot Utilization & Worker State Tracking
  slots = /* @__PURE__ */ new Map();
  activeWorkersSamples = new BoundedSamples();
  activeWorkersDistribution = {};
  sourceActiveSamples = /* @__PURE__ */ new Map();
  slotStateDistributionSamples = new BoundedSamples();
  samplerTimer = null;
  // 2. DB Pool Telemetry
  dbPoolWaitSamples = new BoundedSamples();
  dbPoolQueuedSamples = new BoundedSamples();
  dbPoolActiveQueries = 0;
  dbPoolTotalWaitMs = 0;
  dbPoolMaxWaitMs = 0;
  dbSqlSamples = new BoundedSamples();
  dbHoldSamples = new BoundedSamples();
  dbTransactionSamples = new BoundedSamples();
  dbQueryCount = 0;
  dbSqlTotalMs = 0;
  completedChapterCount = 0;
  // Query classes are intentionally bounded and parameter-free. This is
  // diagnostic attribution, not a query log.
  dbQueryFingerprints = /* @__PURE__ */ new Map();
  // Claim attempts that do not yield a chapter never reach ChapterMetricRecord.
  // Keep them separately so pool pressure can be attributed to useful work
  // versus exhausted scheduler scans. These samples are bounded like the
  // other runtime measurements and carry no work/source identifiers.
  schedulerClaimedAcquireSamples = new BoundedSamples();
  schedulerEmptyAcquireSamples = new BoundedSamples();
  schedulerErrorAcquireSamples = new BoundedSamples();
  recordDbQuery(ms, fingerprint = "UNKNOWN") {
    this.dbSqlSamples.push(ms);
    this.dbQueryCount++;
    this.dbSqlTotalMs += ms;
    const label = fingerprint.slice(0, 120) || "UNKNOWN";
    let bucket = this.dbQueryFingerprints.get(label);
    if (!bucket) {
      const key = this.dbQueryFingerprints.size < 64 ? label : "OTHER";
      bucket = this.dbQueryFingerprints.get(key);
      if (!bucket) {
        bucket = { count: 0, totalMs: 0, maxMs: 0, samples: new BoundedSamples() };
        this.dbQueryFingerprints.set(key, bucket);
      }
    }
    bucket.count++;
    bucket.totalMs += ms;
    bucket.maxMs = Math.max(bucket.maxMs, ms);
    bucket.samples.push(ms);
  }
  recordDbHold(ms) {
    this.dbHoldSamples.push(ms);
  }
  recordDbTransaction(ms) {
    this.dbTransactionSamples.push(ms);
  }
  recordSchedulerAcquireAttempt(ms, outcome) {
    const durationMs = Math.max(0, ms);
    if (outcome === "CLAIMED") this.schedulerClaimedAcquireSamples.push(durationMs);
    else if (outcome === "EMPTY") this.schedulerEmptyAcquireSamples.push(durationMs);
    else this.schedulerErrorAcquireSamples.push(durationMs);
  }
  // 3. Telegram Storage Telemetry
  telegramActiveUploads = 0;
  telegramActiveUploadsSamples = new BoundedSamples();
  telegramPageUploadMsSamples = new BoundedSamples();
  telegramSemaphoreWaitSamples = new BoundedSamples();
  telegramTotalBytesUploaded = 0;
  // 4. Image Download Telemetry
  downloadActiveRequests = 0;
  downloadActiveSamples = new BoundedSamples();
  downloadPageMsSamples = new BoundedSamples();
  downloadSemaphoreWaitSamples = new BoundedSamples();
  downloadTotalBytes = 0;
  downloadErrorsCount = 0;
  downloadRetriesCount = 0;
  // 5. Host Rate Limiter Telemetry
  hostRateLimitWaitSamples = /* @__PURE__ */ new Map();
  // 6. Internal Limiters & Semaphores Audit
  limiters = /* @__PURE__ */ new Map();
  // 7. Chapter Jobs
  chapters = new BoundedSamples(256);
  // 8. Event Loop & Node Runtime Telemetry
  eventLoopLagSamples = new BoundedSamples();
  lastELU = performance.eventLoopUtilization ? performance.eventLoopUtilization() : null;
  eluHistory = new BoundedSamples();
  gcPauseSamples = new BoundedSamples();
  lastCpuUsage = process.cpuUsage();
  lastCpuTime = performance.now();
  cpuPercentSamples = new BoundedSamples();
  // Persistence
  poolRef = null;
  flushTimer = null;
  constructor() {
    this.startRuntimeSampling();
    this.initGcObserver();
  }
  static getInstance() {
    if (!_TelemetryCollector.instance) {
      _TelemetryCollector.instance = new _TelemetryCollector();
    }
    return _TelemetryCollector.instance;
  }
  setPool(pool2) {
    this.poolRef = pool2;
  }
  startSession(sessionId, expiresAt = Date.now() + 5 * 6e4) {
    this.activeSessionId = sessionId;
    this.sessionExpiresAt = Math.min(expiresAt, Date.now() + 5 * 6e4);
    this.sessionStartTime = performance.now();
    this.activeWorkersSamples = new BoundedSamples();
    this.activeWorkersDistribution = {};
    this.limiters.clear();
    this.telegramTotalBytesUploaded = this.downloadTotalBytes = 0;
    this.dbPoolWaitSamples = new BoundedSamples();
    this.dbPoolQueuedSamples = new BoundedSamples();
    this.dbPoolTotalWaitMs = 0;
    this.dbPoolMaxWaitMs = 0;
    this.dbQueryCount = this.dbSqlTotalMs = this.completedChapterCount = 0;
    this.dbQueryFingerprints.clear();
    this.dbSqlSamples = new BoundedSamples();
    this.dbHoldSamples = new BoundedSamples();
    this.dbTransactionSamples = new BoundedSamples();
    this.telegramActiveUploadsSamples = new BoundedSamples();
    this.telegramPageUploadMsSamples = new BoundedSamples();
    this.telegramSemaphoreWaitSamples = new BoundedSamples();
    this.downloadActiveSamples = new BoundedSamples();
    this.downloadPageMsSamples = new BoundedSamples();
    this.downloadSemaphoreWaitSamples = new BoundedSamples();
    this.downloadErrorsCount = 0;
    this.downloadRetriesCount = 0;
    this.hostRateLimitWaitSamples.clear();
    this.chapters = new BoundedSamples(256);
    this.gcPauseSamples = new BoundedSamples();
    this.eventLoopLagSamples = new BoundedSamples();
    this.eluHistory = new BoundedSamples();
    this.cpuPercentSamples = new BoundedSamples();
    this.sourceActiveSamples.clear();
    this.slotStateDistributionSamples = new BoundedSamples();
    const now = performance.now();
    for (const [_, slot] of this.slots.entries()) {
      slot.stateEnteredAt = now;
      slot.stateDurationMs = {
        WAITING_MUTEX: 0,
        WAITING_CLAIM_DB: 0,
        WAITING_SOURCE_PERMIT: 0,
        ACTIVE_SOURCE: 0,
        ACTIVE_DOWNLOAD: 0,
        ACTIVE_ENCODE: 0,
        ACTIVE_TELEGRAM: 0,
        ACTIVE_DB: 0,
        WAITING_BARRIER: 0,
        IDLE: 0
      };
    }
    this.logger.info(`Started diagnostic telemetry session: ${sessionId}`);
  }
  getSessionId() {
    return this.activeSessionId;
  }
  // --- Slot State Tracking ---
  registerSlot(slotIndex) {
    if (!this.slots.has(slotIndex)) {
      this.slots.set(slotIndex, {
        slotIndex,
        currentState: "IDLE",
        stateEnteredAt: performance.now(),
        stateDurationMs: {
          WAITING_MUTEX: 0,
          WAITING_CLAIM_DB: 0,
          WAITING_SOURCE_PERMIT: 0,
          ACTIVE_SOURCE: 0,
          ACTIVE_DOWNLOAD: 0,
          ACTIVE_ENCODE: 0,
          ACTIVE_TELEGRAM: 0,
          ACTIVE_DB: 0,
          WAITING_BARRIER: 0,
          IDLE: 0
        }
      });
    }
  }
  setSlotState(slotIndex, newState, context) {
    let slot = this.slots.get(slotIndex);
    if (!slot) {
      this.registerSlot(slotIndex);
      slot = this.slots.get(slotIndex);
    }
    let normalizedState = newState;
    if (newState === "WAITING_FOR_JOB") normalizedState = "WAITING_MUTEX";
    else if (newState === "WAITING_FOR_SOURCE" || newState === "WAITING_FOR_SOURCE_RATE_LIMIT") normalizedState = "WAITING_SOURCE_PERMIT";
    else if (newState === "WAITING_FOR_DOWNLOAD") normalizedState = "ACTIVE_DOWNLOAD";
    else if (newState === "WAITING_FOR_TELEGRAM") normalizedState = "ACTIVE_TELEGRAM";
    else if (newState === "WAITING_FOR_DATABASE" || newState === "WAITING_FOR_DB_POOL") normalizedState = "ACTIVE_DB";
    else if (newState === "WAITING_FOR_PUBLICATION_BARRIER" || newState === "PROTECTIVE_STOP") normalizedState = "WAITING_BARRIER";
    else if (newState === "ACTIVE_PROCESSING") normalizedState = "ACTIVE_SOURCE";
    const now = performance.now();
    const elapsed = now - slot.stateEnteredAt;
    slot.stateDurationMs[slot.currentState] = (slot.stateDurationMs[slot.currentState] || 0) + elapsed;
    slot.currentState = normalizedState;
    slot.context = context;
    slot.stateEnteredAt = now;
  }
  /**
   * Authoritative calculation of productive vs busy vs idle slot occupancy.
   * Productive slots: ACTIVE_DOWNLOAD, ACTIVE_TELEGRAM, ACTIVE_DB, ACTIVE_SOURCE, ACTIVE_ENCODE.
   * Busy slots: all non-IDLE slots (including mutex/permit/db wait).
   */
  getSlotProductivitySnapshot() {
    const configuredSlots = this.configuredChapterSlots || this.slots.size;
    const effectiveSlots = configuredSlots > 0 ? Math.max(1, Math.min(configuredSlots, Math.floor(this.effectiveCapacity()))) : 0;
    let busySlots = 0;
    let productiveSlots = 0;
    let idleSlots = Math.max(0, configuredSlots - this.slots.size);
    for (const slot of this.slots.values()) {
      if (slot.currentState === "IDLE") {
        idleSlots++;
      } else {
        busySlots++;
        if (slot.currentState === "ACTIVE_DOWNLOAD" || slot.currentState === "ACTIVE_TELEGRAM" || slot.currentState === "ACTIVE_DB" || slot.currentState === "ACTIVE_SOURCE" || slot.currentState === "ACTIVE_ENCODE") {
          productiveSlots++;
        }
      }
    }
    const productiveSlotRatio = effectiveSlots > 0 ? Math.round(Math.min(productiveSlots, effectiveSlots) / effectiveSlots * 1e3) / 10 : 0;
    return {
      configuredSlots,
      effectiveSlots,
      busySlots,
      productiveSlots,
      idleSlots,
      productiveSlotRatio
    };
  }
  // --- DB Pool Telemetry ---
  recordDbPoolWait(waitMs, waitingCount) {
    this.dbPoolWaitSamples.push(waitMs);
    this.dbPoolQueuedSamples.push(waitingCount);
    this.dbPoolTotalWaitMs += waitMs;
    if (waitMs > this.dbPoolMaxWaitMs) this.dbPoolMaxWaitMs = waitMs;
  }
  trackActiveDbQuery(delta) {
    this.dbPoolActiveQueries = Math.max(0, this.dbPoolActiveQueries + delta);
  }
  // --- Telegram Telemetry ---
  trackActiveTelegramUpload(delta) {
    this.telegramActiveUploads = Math.max(0, this.telegramActiveUploads + delta);
  }
  recordTelegramUpload(latencyMs, bytes) {
    this.telegramPageUploadMsSamples.push(latencyMs);
    this.telegramTotalBytesUploaded += bytes;
  }
  recordTelegramSemaphoreWait(waitMs) {
    this.telegramSemaphoreWaitSamples.push(waitMs);
  }
  // --- Image Download Telemetry ---
  trackActiveDownload(delta) {
    this.downloadActiveRequests = Math.max(0, this.downloadActiveRequests + delta);
  }
  recordImageDownload(source, latencyMs, bytes) {
    this.downloadPageMsSamples.push(latencyMs);
    this.downloadTotalBytes += bytes;
  }
  recordDownloadSemaphoreWait(waitMs) {
    this.downloadSemaphoreWaitSamples.push(waitMs);
  }
  recordDownloadError(retried) {
    if (retried) this.downloadRetriesCount++;
    else this.downloadErrorsCount++;
  }
  // --- Rate Limiter Telemetry ---
  recordRateLimitWait(host, waitMs) {
    let list = this.hostRateLimitWaitSamples.get(host);
    if (!list) {
      if (this.hostRateLimitWaitSamples.size >= 128) return;
      list = new BoundedSamples();
      this.hostRateLimitWaitSamples.set(host, list);
    }
    list.push(waitMs);
    this.recordLimiterWait(`host_rate_limiter:${host}`, waitMs, "dynamic");
  }
  // --- Generic Limiter Audit ---
  recordLimiterWait(name, waitMs, limit = "unknown") {
    let rec = this.limiters.get(name);
    if (!rec) {
      if (this.limiters.size >= 128) return;
      rec = {
        name,
        configuredLimit: limit,
        observedConcurrencyPeak: 0,
        observedConcurrencyAvg: 0,
        hitCount: 0,
        waitSamples: new BoundedSamples(),
        totalWaitMs: 0,
        maxWaitMs: 0
      };
      this.limiters.set(name, rec);
    }
    rec.waitSamples.push(waitMs);
    rec.totalWaitMs += waitMs;
    if (waitMs > 0) rec.hitCount++;
    if (waitMs > rec.maxWaitMs) rec.maxWaitMs = waitMs;
  }
  updateLimiterConcurrency(name, current, limit) {
    let rec = this.limiters.get(name);
    if (!rec) {
      if (this.limiters.size >= 128) return;
      rec = {
        name,
        configuredLimit: limit ?? "unknown",
        observedConcurrencyPeak: current,
        observedConcurrencyAvg: current,
        hitCount: 0,
        waitSamples: new BoundedSamples(),
        totalWaitMs: 0,
        maxWaitMs: 0
      };
      this.limiters.set(name, rec);
    }
    if (current > rec.observedConcurrencyPeak) {
      rec.observedConcurrencyPeak = current;
    }
    if (limit !== void 0) {
      rec.configuredLimit = limit;
    }
  }
  // --- Chapter Profile Recording ---
  recordChapterMetric(record) {
    this.completedChapterCount++;
    this.chapters.push(record);
    this.logger.info("CHAPTER_DIAGNOSTIC", record);
  }
  // --- Background Sampling ---
  startRuntimeSampling() {
    const sample = () => {
      if (this.activeSessionId && Date.now() >= this.sessionExpiresAt) this.activeSessionId = null;
      let activeCount = 0;
      const sourceCounts = /* @__PURE__ */ new Map();
      const currentStatesCount = {
        WAITING_MUTEX: 0,
        WAITING_CLAIM_DB: 0,
        WAITING_SOURCE_PERMIT: 0,
        ACTIVE_SOURCE: 0,
        ACTIVE_DOWNLOAD: 0,
        ACTIVE_ENCODE: 0,
        ACTIVE_TELEGRAM: 0,
        ACTIVE_DB: 0,
        WAITING_BARRIER: 0,
        IDLE: 0
      };
      for (let i = 0; i < (this.configuredChapterSlots || this.slots.size); i++) {
        const slot = this.slots.get(i);
        const st = slot?.currentState || "IDLE";
        currentStatesCount[st] = (currentStatesCount[st] || 0) + 1;
        if (st !== "IDLE") {
          activeCount++;
        }
        if (slot?.context) {
          const src = slot.context.split(" ")[0];
          if (src) {
            sourceCounts.set(src, (sourceCounts.get(src) || 0) + 1);
          }
        }
      }
      this.slotStateDistributionSamples.push(currentStatesCount);
      this.activeWorkersSamples.push(activeCount);
      const bucket = Math.max(0, activeCount);
      this.activeWorkersDistribution[bucket] = (this.activeWorkersDistribution[bucket] || 0) + 1;
      for (const src of /* @__PURE__ */ new Set([...this.sourceActiveSamples.keys(), ...sourceCounts.keys()])) {
        const c = sourceCounts.get(src) || 0;
        let arr = this.sourceActiveSamples.get(src);
        if (!arr) {
          if (this.sourceActiveSamples.size >= 128) continue;
          arr = new BoundedSamples();
          this.sourceActiveSamples.set(src, arr);
        }
        arr.push(c);
      }
      this.telegramActiveUploadsSamples.push(this.telegramActiveUploads);
      this.downloadActiveSamples.push(this.downloadActiveRequests);
      for (const [name, provider] of this.limiterProviders) {
        const state = provider();
        this.updateLimiterConcurrency(name, state.active, state.configuredCapacity);
        const limiter = this.limiters.get(name);
        if (limiter) (limiter.concurrencySamples ||= new BoundedSamples()).push(state.active);
      }
      const cpuNow = process.cpuUsage();
      const timeNow = performance.now();
      const elapsedMs = timeNow - this.lastCpuTime;
      if (elapsedMs > 500) {
        const userDiff = (cpuNow.user - this.lastCpuUsage.user) / 1e3;
        const sysDiff = (cpuNow.system - this.lastCpuUsage.system) / 1e3;
        const totalCpuMs = userDiff + sysDiff;
        const percent = Math.round(totalCpuMs / elapsedMs * 100 * 10) / 10;
        this.cpuPercentSamples.push(percent);
        this.lastCpuUsage = cpuNow;
        this.lastCpuTime = timeNow;
      }
      if (performance.eventLoopUtilization && this.lastELU) {
        const currentELU = performance.eventLoopUtilization();
        const elu = performance.eventLoopUtilization(currentELU, this.lastELU);
        this.lastELU = currentELU;
        this.eluHistory.push(Math.round(elu.utilization * 1e3) / 10);
      }
      this.samplerTimer = setTimeout(sample, this.activeSessionId ? 200 : 1e3);
      this.samplerTimer.unref();
    };
    this.samplerTimer = setTimeout(sample, 1e3);
    this.samplerTimer.unref();
    this.flushTimer = setInterval(async () => {
      await this.flushTelemetryToDb();
    }, 5e3);
    this.flushTimer.unref();
  }
  initGcObserver() {
    try {
      const obs = new PerformanceObserver((list) => {
        for (const entry of list.getEntries()) {
          this.gcPauseSamples.push(entry.duration);
        }
      });
      obs.observe({ entryTypes: ["gc"] });
      this.gcObserver = obs;
    } catch {
    }
  }
  recordEventLoopLag(lagMs) {
    this.eventLoopLagSamples.push(lagMs);
  }
  // --- Snapshot Generation ---
  getSnapshotReport() {
    const mem = process.memoryUsage();
    const totalSlotSamples = this.activeWorkersSamples.length || 1;
    const configuredSlots = this.configuredChapterSlots || this.slots.size;
    const distribution = {};
    for (let i = 0; i <= configuredSlots; i++) distribution[i] = 0;
    for (const active of this.activeWorkersSamples) distribution[active] = (distribution[active] || 0) + 1;
    const timeAtConfiguredCapacityPercent = Math.round((distribution[configuredSlots] || 0) / totalSlotSamples * 1e3) / 10;
    const totalSlotDistributionSamples = this.slotStateDistributionSamples.length || 1;
    const rawSums = {
      WAITING_MUTEX: 0,
      WAITING_CLAIM_DB: 0,
      WAITING_SOURCE_PERMIT: 0,
      ACTIVE_SOURCE: 0,
      ACTIVE_DOWNLOAD: 0,
      ACTIVE_ENCODE: 0,
      ACTIVE_TELEGRAM: 0,
      ACTIVE_DB: 0,
      WAITING_BARRIER: 0,
      IDLE: 0
    };
    for (const sample of this.slotStateDistributionSamples) {
      for (const [st, count2] of Object.entries(sample)) {
        rawSums[st] = (rawSums[st] || 0) + count2;
      }
    }
    const avgSlotStates = {
      WAITING_MUTEX: Math.round(rawSums.WAITING_MUTEX / totalSlotDistributionSamples * 100) / 100,
      WAITING_CLAIM_DB: Math.round(rawSums.WAITING_CLAIM_DB / totalSlotDistributionSamples * 100) / 100,
      WAITING_SOURCE_PERMIT: Math.round(rawSums.WAITING_SOURCE_PERMIT / totalSlotDistributionSamples * 100) / 100,
      ACTIVE_SOURCE: Math.round(rawSums.ACTIVE_SOURCE / totalSlotDistributionSamples * 100) / 100,
      ACTIVE_DOWNLOAD: Math.round(rawSums.ACTIVE_DOWNLOAD / totalSlotDistributionSamples * 100) / 100,
      ACTIVE_ENCODE: Math.round(rawSums.ACTIVE_ENCODE / totalSlotDistributionSamples * 100) / 100,
      ACTIVE_TELEGRAM: Math.round(rawSums.ACTIVE_TELEGRAM / totalSlotDistributionSamples * 100) / 100,
      ACTIVE_DB: Math.round(rawSums.ACTIVE_DB / totalSlotDistributionSamples * 100) / 100,
      WAITING_BARRIER: Math.round(rawSums.WAITING_BARRIER / totalSlotDistributionSamples * 100) / 100,
      IDLE: Math.round(rawSums.IDLE / totalSlotDistributionSamples * 100) / 100
    };
    const sumStates = Object.values(avgSlotStates).reduce((a, b) => a + b, 0);
    const slotStatesAggregated = {
      WAITING_MUTEX: 0,
      WAITING_CLAIM_DB: 0,
      WAITING_SOURCE_PERMIT: 0,
      ACTIVE_SOURCE: 0,
      ACTIVE_DOWNLOAD: 0,
      ACTIVE_ENCODE: 0,
      ACTIVE_TELEGRAM: 0,
      ACTIVE_DB: 0,
      WAITING_BARRIER: 0,
      IDLE: 0
    };
    let totalBusyMs = 0;
    let totalIdleMs = 0;
    let totalBlockedMs = 0;
    let totalAllMs = 0;
    const now = performance.now();
    for (const [_, slot] of this.slots.entries()) {
      const currentElapsed = now - slot.stateEnteredAt;
      for (const st of Object.keys(slot.stateDurationMs)) {
        let ms = slot.stateDurationMs[st] || 0;
        if (st === slot.currentState) ms += currentElapsed;
        slotStatesAggregated[st] = (slotStatesAggregated[st] || 0) + ms;
        totalAllMs += ms;
        if (st === "IDLE") {
          totalIdleMs += ms;
        } else if (st.startsWith("ACTIVE_")) {
          totalBusyMs += ms;
        } else {
          totalBlockedMs += ms;
        }
      }
    }
    const safeTotalAll = totalAllMs || 1;
    const workerBusyPercent = Math.round(totalBusyMs / safeTotalAll * 1e3) / 10;
    const workerIdlePercent = Math.round(totalIdleMs / safeTotalAll * 1e3) / 10;
    const workerBlockedPercent = Math.round(totalBlockedMs / safeTotalAll * 1e3) / 10;
    const mutexWaitTimes = this.chapters.map((c) => c.mutex_wait_ms || 0);
    const schedulerAcquireTimes = this.chapters.map((c) => c.scheduler_acquire_ms ?? c.claim_db_ms ?? 0);
    const poolWaitTimes = this.chapters.map((c) => c.pool_wait_ms ?? 0);
    const sqlExecTimes = this.chapters.map((c) => c.sql_exec_ms ?? 0);
    const claimSqlTimes = this.chapters.map((c) => c.claim_sql_ms ?? c.sql_exec_ms ?? 0);
    const claimLockPoolWaitTimes = this.chapters.map((c) => c.claim_lock_pool_wait_ms ?? 0);
    const claimLockSqlExecTimes = this.chapters.map((c) => c.claim_lock_sql_exec_ms ?? c.claim_sql_ms ?? 0);
    const queriesPerClaim = this.chapters.map((c) => c.total_queries ?? 1);
    const worksTestedList = this.chapters.map((c) => c.works_tested ?? 1);
    const staffCheckTimes = this.chapters.map((c) => c.staff_check_ms ?? 0);
    const p0ProbeTimes = this.chapters.map((c) => c.p0_probe_ms ?? 0);
    const criticalWorkTimes = this.chapters.map((c) => c.critical_work_time_ms ?? 0);
    const p1WorkTimes = this.chapters.map((c) => c.p1_work_time_ms ?? 0);
    const p2WorkTimes = this.chapters.map((c) => c.p2_work_time_ms ?? 0);
    const activeFallbackTimes = this.chapters.map((c) => c.active_fallback_ms ?? 0);
    const admissionOnDemandTimes = this.chapters.map((c) => c.admission_on_demand_ms ?? 0);
    const catalogFallbackTimes = this.chapters.map((c) => c.catalog_fallback_ms ?? 0);
    const claimDbTimes = schedulerAcquireTimes;
    const claimTimes = this.chapters.map((c) => c.claim_acquire_ms);
    const metadataTimes = this.chapters.map((c) => c.metadata_load_ms);
    const sourceFetchTimes = this.chapters.map((c) => c.source_fetch_ms);
    const pageResTimes = this.chapters.map((c) => c.page_resolution_ms);
    const downloadTimes = this.chapters.map((c) => c.download_ms);
    const encodeTimes = this.chapters.map((c) => c.encode_ms || 0);
    const telegramTimes = this.chapters.map((c) => c.telegram_upload_ms);
    const dbWaitTimes = this.chapters.map((c) => c.db_wait_ms);
    const dbPublishTimes = this.chapters.map((c) => c.db_publish_ms);
    const rateLimitTimes = this.chapters.map((c) => c.rate_limit_wait_ms);
    const semWaitTimes = this.chapters.map((c) => c.semaphore_wait_ms);
    const bufferReservationWaitTimes = this.chapters.map((c) => c.buffer_reservation_wait_aggregate_ms || 0);
    const bufferedPagePermitWaitTimes = this.chapters.map((c) => c.buffered_page_permit_wait_aggregate_ms || 0);
    const bufferReservationHoldTimes = this.chapters.map((c) => c.buffer_reservation_hold_aggregate_ms || 0);
    const readyQueueDwellTimes = this.chapters.map((c) => c.ready_queue_dwell_aggregate_ms || 0);
    const bufferReservationWaitEvents = this.chapters.map((c) => c.buffer_reservation_wait_events || 0);
    const bufferedPagePermitWaitEvents = this.chapters.map((c) => c.buffered_page_permit_wait_events || 0);
    const otherWaitTimes = this.chapters.map((c) => c.other_wait_ms);
    const totalJobTimes = this.chapters.map((c) => c.totalDurationMs);
    const totalSlotOccupancyTimes = this.chapters.map((c) => c.totalSlotOccupancyMs || c.totalDurationMs + c.claim_acquire_ms);
    const occupancyTimesSec = totalSlotOccupancyTimes.map((ms) => ms / 1e3);
    const meanSlotOccupancySec = avg(occupancyTimesSec);
    const p50SlotOccupancySec = percentile(occupancyTimesSec, 0.5);
    const p95SlotOccupancySec = percentile(occupancyTimesSec, 0.95);
    const avgBusyWorkers = avg(this.activeWorkersSamples);
    const theoreticalCapacityPerMin = meanSlotOccupancySec > 0 ? avgBusyWorkers * 60 / meanSlotOccupancySec : 0;
    const sourceDist = {};
    for (const ch of this.chapters) {
      if (!sourceDist[ch.source]) {
        sourceDist[ch.source] = {
          count: 0,
          totalPages: 0,
          totalBytes: 0,
          totalDurationMs: 0,
          avgDurationMs: 0,
          avgDownloadMs: 0,
          avgUploadMs: 0,
          avgDbMs: 0,
          avgSemWaitMs: 0,
          avgRateLimitWaitMs: 0
        };
      }
      const s = sourceDist[ch.source];
      s.count++;
      s.totalPages += ch.pageCount;
      s.totalBytes += ch.totalBytes;
      s.totalDurationMs += ch.totalDurationMs;
    }
    for (const [src, s] of Object.entries(sourceDist)) {
      const srcChapters = this.chapters.filter((c) => c.source === src);
      s.avgDurationMs = avg(srcChapters.map((c) => c.totalDurationMs));
      s.avgDownloadMs = avg(srcChapters.map((c) => c.download_ms));
      s.avgUploadMs = avg(srcChapters.map((c) => c.telegram_upload_ms));
      s.avgDbMs = avg(srcChapters.map((c) => c.db_publish_ms));
      s.avgSemWaitMs = avg(srcChapters.map((c) => c.semaphore_wait_ms));
      s.avgRateLimitWaitMs = avg(srcChapters.map((c) => c.rate_limit_wait_ms));
    }
    const slowestChapters = [...this.chapters].sort((a, b) => b.totalDurationMs - a.totalDurationMs).slice(0, 10).map((c) => {
      let reason = "Normal execution";
      if (c.semaphore_wait_ms > c.totalDurationMs * 0.4) {
        reason = `Blocked on source/global semaphore (${c.semaphore_wait_ms}ms)`;
      } else if (c.telegram_upload_ms > c.totalDurationMs * 0.5) {
        reason = `Telegram upload latency (${c.telegram_upload_ms}ms for ${c.pageCount} pages)`;
      } else if (c.download_ms > c.totalDurationMs * 0.5) {
        reason = `Source image download CDN latency (${c.download_ms}ms for ${c.pageCount} pages)`;
      } else if (c.rate_limit_wait_ms > c.totalDurationMs * 0.3) {
        reason = `Host rate limiter throttle (${c.rate_limit_wait_ms}ms)`;
      } else if (c.db_publish_ms > c.totalDurationMs * 0.4) {
        reason = `Database publish/lock wait (${c.db_publish_ms}ms)`;
      }
      return {
        ...c,
        slowReason: reason
      };
    });
    const limitersSummary = {};
    for (const [name, rec] of this.limiters.entries()) {
      limitersSummary[name] = {
        configuredLimit: rec.configuredLimit,
        observedConcurrencyPeak: rec.observedConcurrencyPeak,
        observedConcurrencyAvg: avg(rec.concurrencySamples || []),
        hitCount: rec.hitCount,
        waitAvgMs: avg(rec.waitSamples),
        waitP50Ms: percentile(rec.waitSamples, 0.5),
        waitP95Ms: percentile(rec.waitSamples, 0.95),
        waitMaxMs: rec.maxWaitMs
      };
    }
    for (const [name, provider] of this.limiterProviders) {
      const state = provider();
      limitersSummary[name] = {
        waitP50Ms: 0,
        waitP95Ms: 0,
        ...limitersSummary[name],
        ...state,
        saturationPercent: state.currentCapacity > 0 ? state.active / state.currentCapacity * 100 : 0
      };
    }
    const avgActiveProcessing = Math.round((avgSlotStates.ACTIVE_SOURCE + avgSlotStates.ACTIVE_DOWNLOAD + avgSlotStates.ACTIVE_ENCODE + avgSlotStates.ACTIVE_TELEGRAM + avgSlotStates.ACTIVE_DB) * 100) / 100;
    const avgBlocked = Math.round((avgSlotStates.WAITING_MUTEX + avgSlotStates.WAITING_CLAIM_DB + avgSlotStates.WAITING_SOURCE_PERMIT + avgSlotStates.WAITING_BARRIER) * 100) / 100;
    const avgIdle = avgSlotStates.IDLE;
    return {
      sessionId: this.activeSessionId,
      timestamp: (/* @__PURE__ */ new Date()).toISOString(),
      telemetryMode: this.activeSessionId ? "DIAGNOSTIC" : "NORMAL",
      runtimeFingerprint: this.runtimeFingerprint,
      maintenance: maintenanceScheduler.snapshot(),
      boundedCpuProfile: this.cpuProfile,
      diagnosticExpiresAt: this.activeSessionId ? new Date(this.sessionExpiresAt).toISOString() : null,
      sampleCapacity: 2048,
      slotsConfigured: configuredSlots,
      effectiveConcurrency: this.effectiveCapacity(),
      database: {
        queries: this.dbQueryCount,
        sqlTotalMs: Math.round(this.dbSqlTotalMs),
        completedChapters: this.completedChapterCount,
        amortizedQueriesPerCompletedChapter: this.completedChapterCount ? this.dbQueryCount / this.completedChapterCount : null,
        amortizedSqlMsPerCompletedChapter: this.completedChapterCount ? this.dbSqlTotalMs / this.completedChapterCount : null,
        sqlP50Ms: percentile(this.dbSqlSamples, 0.5),
        sqlP95Ms: percentile(this.dbSqlSamples, 0.95),
        clientHoldP50Ms: percentile(this.dbHoldSamples, 0.5),
        clientHoldP95Ms: percentile(this.dbHoldSamples, 0.95),
        transactionP50Ms: percentile(this.dbTransactionSamples, 0.5),
        transactionP95Ms: percentile(this.dbTransactionSamples, 0.95),
        poolMax: this.poolRef?.options.max,
        totalConnections: this.poolRef?.totalCount,
        idleConnections: this.poolRef?.idleCount,
        waitingClients: this.poolRef?.waitingCount,
        topQueryClasses: [...this.dbQueryFingerprints.entries()].sort(([, a], [, b]) => b.totalMs - a.totalMs).slice(0, 20).map(([fingerprint, stat]) => ({
          fingerprint,
          count: stat.count,
          totalMs: Math.round(stat.totalMs),
          avgMs: avg(stat.samples),
          p50Ms: percentile(stat.samples, 0.5),
          p95Ms: percentile(stat.samples, 0.95),
          maxMs: Math.round(stat.maxMs)
        }))
      },
      avgSlotStates: {
        ...avgSlotStates,
        ACTIVE_PROCESSING: avgActiveProcessing,
        BLOCKED: avgBlocked,
        SUM: Math.round(sumStates * 100) / 100
      },
      slotOccupancy: {
        meanSec: Math.round(meanSlotOccupancySec * 100) / 100,
        p50Sec: Math.round(p50SlotOccupancySec * 100) / 100,
        p95Sec: Math.round(p95SlotOccupancySec * 100) / 100,
        avgBusyWorkers: avgActiveProcessing,
        avgActiveProcessing,
        avgBlocked,
        avgIdle,
        theoreticalCapacityPerMin: meanSlotOccupancySec > 0 ? Math.round(this.effectiveCapacity() * 60 / meanSlotOccupancySec * 100) / 100 : 0
      },
      activeWorkers: {
        avg: avg(this.activeWorkersSamples),
        p50: percentile(this.activeWorkersSamples, 0.5),
        p75: percentile(this.activeWorkersSamples, 0.75),
        p95: percentile(this.activeWorkersSamples, 0.95),
        peak: this.activeWorkersSamples.length ? Math.max(...this.activeWorkersSamples) : 0,
        distribution,
        timeAtConfiguredCapacityPercent
      },
      perSourceActive: Object.fromEntries([...this.sourceActiveSamples].map(([source, samples]) => [source, { avg: avg(samples), peak: samples.length ? Math.max(...samples) : 0 }])),
      workerTimeBreakdown: {
        workerBusyPercent,
        workerIdlePercent,
        workerBlockedPercent,
        statesAggregatedMs: slotStatesAggregated
      },
      jobProfile: {
        wallTimeBreakdownMs: Object.fromEntries([
          "metadata_load_ms",
          "source_fetch_ms",
          "page_resolution_ms",
          "media_pipeline_wall_ms",
          "cover_check_ms",
          "db_publish_ms",
          "other_wait_ms"
        ].map((key) => [key, avg(this.chapters.map((c) => Number(c[key] || 0)))])),
        totalCompleted: this.chapters.length,
        totalDuration: {
          avg: avg(totalJobTimes),
          p50: percentile(totalJobTimes, 0.5),
          p95: percentile(totalJobTimes, 0.95),
          max: totalJobTimes.length ? Math.max(...totalJobTimes) : 0
        },
        stages: {
          schedulerAcquire: { avg: avg(schedulerAcquireTimes), p50: percentile(schedulerAcquireTimes, 0.5), p95: percentile(schedulerAcquireTimes, 0.95), p99: percentile(schedulerAcquireTimes, 0.99), max: schedulerAcquireTimes.length ? Math.max(...schedulerAcquireTimes) : 0 },
          claimDb: { avg: avg(schedulerAcquireTimes), p50: percentile(schedulerAcquireTimes, 0.5), p95: percentile(schedulerAcquireTimes, 0.95), p99: percentile(schedulerAcquireTimes, 0.99), max: schedulerAcquireTimes.length ? Math.max(...schedulerAcquireTimes) : 0 },
          poolWait: { avg: avg(poolWaitTimes), p50: percentile(poolWaitTimes, 0.5), p95: percentile(poolWaitTimes, 0.95), max: poolWaitTimes.length ? Math.max(...poolWaitTimes) : 0 },
          sqlExec: { avg: avg(sqlExecTimes), p50: percentile(sqlExecTimes, 0.5), p95: percentile(sqlExecTimes, 0.95), max: sqlExecTimes.length ? Math.max(...sqlExecTimes) : 0 },
          schedulerSqlTotal: { avg: avg(sqlExecTimes), p50: percentile(sqlExecTimes, 0.5), p95: percentile(sqlExecTimes, 0.95), max: sqlExecTimes.length ? Math.max(...sqlExecTimes) : 0 },
          claimSql: { avg: avg(claimSqlTimes), p50: percentile(claimSqlTimes, 0.5), p95: percentile(claimSqlTimes, 0.95), max: claimSqlTimes.length ? Math.max(...claimSqlTimes) : 0 },
          claimLockSql: { avg: avg(claimLockSqlExecTimes), p50: percentile(claimLockSqlExecTimes, 0.5), p95: percentile(claimLockSqlExecTimes, 0.95), max: claimLockSqlExecTimes.length ? Math.max(...claimLockSqlExecTimes) : 0 },
          claimLockPoolWait: { avg: avg(claimLockPoolWaitTimes), p50: percentile(claimLockPoolWaitTimes, 0.5), p95: percentile(claimLockPoolWaitTimes, 0.95), max: claimLockPoolWaitTimes.length ? Math.max(...claimLockPoolWaitTimes) : 0 },
          claimLockSqlExec: { avg: avg(claimLockSqlExecTimes), p50: percentile(claimLockSqlExecTimes, 0.5), p95: percentile(claimLockSqlExecTimes, 0.95), max: claimLockSqlExecTimes.length ? Math.max(...claimLockSqlExecTimes) : 0 },
          queriesPerClaim: { avg: avg(queriesPerClaim), p50: percentile(queriesPerClaim, 0.5), p95: percentile(queriesPerClaim, 0.95) },
          worksTested: { avg: avg(worksTestedList), p50: percentile(worksTestedList, 0.5), p95: percentile(worksTestedList, 0.95) },
          mutexWait: { avg: avg(mutexWaitTimes), p50: percentile(mutexWaitTimes, 0.5), p95: percentile(mutexWaitTimes, 0.95), p99: percentile(mutexWaitTimes, 0.99), max: mutexWaitTimes.length ? Math.max(...mutexWaitTimes) : 0 },
          claim: { avg: avg(claimTimes), p50: percentile(claimTimes, 0.5), p75: percentile(claimTimes, 0.75), p95: percentile(claimTimes, 0.95), p99: percentile(claimTimes, 0.99), max: claimTimes.length ? Math.max(...claimTimes) : 0 },
          metadata: { avg: avg(metadataTimes), p50: percentile(metadataTimes, 0.5), p75: percentile(metadataTimes, 0.75), p95: percentile(metadataTimes, 0.95), p99: percentile(metadataTimes, 0.99), max: metadataTimes.length ? Math.max(...metadataTimes) : 0 },
          sourceFetch: { avg: avg(sourceFetchTimes), p50: percentile(sourceFetchTimes, 0.5), p75: percentile(sourceFetchTimes, 0.75), p95: percentile(sourceFetchTimes, 0.95), p99: percentile(sourceFetchTimes, 0.99), max: sourceFetchTimes.length ? Math.max(...sourceFetchTimes) : 0 },
          pageResolution: { avg: avg(pageResTimes), p50: percentile(pageResTimes, 0.5), p75: percentile(pageResTimes, 0.75), p95: percentile(pageResTimes, 0.95), p99: percentile(pageResTimes, 0.99), max: pageResTimes.length ? Math.max(...pageResTimes) : 0 },
          download: { avg: avg(downloadTimes), p50: percentile(downloadTimes, 0.5), p75: percentile(downloadTimes, 0.75), p95: percentile(downloadTimes, 0.95), p99: percentile(downloadTimes, 0.99), max: downloadTimes.length ? Math.max(...downloadTimes) : 0 },
          encode: { avg: avg(encodeTimes), p50: percentile(encodeTimes, 0.5), p75: percentile(encodeTimes, 0.75), p95: percentile(encodeTimes, 0.95), p99: percentile(encodeTimes, 0.99), max: encodeTimes.length ? Math.max(...encodeTimes) : 0 },
          telegramUpload: { avg: avg(telegramTimes), p50: percentile(telegramTimes, 0.5), p75: percentile(telegramTimes, 0.75), p95: percentile(telegramTimes, 0.95), p99: percentile(telegramTimes, 0.99), max: telegramTimes.length ? Math.max(...telegramTimes) : 0 },
          dbWait: { avg: avg(dbWaitTimes), p50: percentile(dbWaitTimes, 0.5), p75: percentile(dbWaitTimes, 0.75), p95: percentile(dbWaitTimes, 0.95), p99: percentile(dbWaitTimes, 0.99), max: dbWaitTimes.length ? Math.max(...dbWaitTimes) : 0 },
          dbPublish: { avg: avg(dbPublishTimes), p50: percentile(dbPublishTimes, 0.5), p75: percentile(dbPublishTimes, 0.75), p95: percentile(dbPublishTimes, 0.95), p99: percentile(dbPublishTimes, 0.99), max: dbPublishTimes.length ? Math.max(...dbPublishTimes) : 0 },
          rateLimitWait: { avg: avg(rateLimitTimes), p50: percentile(rateLimitTimes, 0.5), p75: percentile(rateLimitTimes, 0.75), p95: percentile(rateLimitTimes, 0.95), p99: percentile(rateLimitTimes, 0.99), max: rateLimitTimes.length ? Math.max(...rateLimitTimes) : 0 },
          semaphoreWait: { avg: avg(semWaitTimes), p50: percentile(semWaitTimes, 0.5), p75: percentile(semWaitTimes, 0.75), p95: percentile(semWaitTimes, 0.95), p99: percentile(semWaitTimes, 0.99), max: semWaitTimes.length ? Math.max(...semWaitTimes) : 0 },
          otherWait: { avg: avg(otherWaitTimes), p50: percentile(otherWaitTimes, 0.5), p75: percentile(otherWaitTimes, 0.75), p95: percentile(otherWaitTimes, 0.95), p99: percentile(otherWaitTimes, 0.99), max: otherWaitTimes.length ? Math.max(...otherWaitTimes) : 0 }
        },
        pageBuffer: {
          // Aggregate durations may exceed chapter wall time because multiple
          // page producers overlap. Event counts make that explicit.
          reservationWaitAggregateMs: { avg: avg(bufferReservationWaitTimes), p50: percentile(bufferReservationWaitTimes, 0.5), p95: percentile(bufferReservationWaitTimes, 0.95), max: bufferReservationWaitTimes.length ? Math.max(...bufferReservationWaitTimes) : 0 },
          bufferedPagePermitWaitAggregateMs: { avg: avg(bufferedPagePermitWaitTimes), p50: percentile(bufferedPagePermitWaitTimes, 0.5), p95: percentile(bufferedPagePermitWaitTimes, 0.95), max: bufferedPagePermitWaitTimes.length ? Math.max(...bufferedPagePermitWaitTimes) : 0 },
          reservationHoldAggregateMs: { avg: avg(bufferReservationHoldTimes), p50: percentile(bufferReservationHoldTimes, 0.5), p95: percentile(bufferReservationHoldTimes, 0.95), max: bufferReservationHoldTimes.length ? Math.max(...bufferReservationHoldTimes) : 0 },
          readyQueueDwellAggregateMs: { avg: avg(readyQueueDwellTimes), p50: percentile(readyQueueDwellTimes, 0.5), p95: percentile(readyQueueDwellTimes, 0.95), max: readyQueueDwellTimes.length ? Math.max(...readyQueueDwellTimes) : 0 },
          reservationWaitEvents: { avg: avg(bufferReservationWaitEvents), p50: percentile(bufferReservationWaitEvents, 0.5), p95: percentile(bufferReservationWaitEvents, 0.95), total: bufferReservationWaitEvents.reduce((sum, value) => sum + value, 0) },
          bufferedPagePermitWaitEvents: { avg: avg(bufferedPagePermitWaitEvents), p50: percentile(bufferedPagePermitWaitEvents, 0.5), p95: percentile(bufferedPagePermitWaitEvents, 0.95), total: bufferedPagePermitWaitEvents.reduce((sum, value) => sum + value, 0) }
        }
      },
      schedulerAcquireBreakdown: {
        totalMs: { avg: avg(schedulerAcquireTimes), p50: percentile(schedulerAcquireTimes, 0.5), p95: percentile(schedulerAcquireTimes, 0.95) },
        poolWaitMs: { avg: avg(poolWaitTimes), p50: percentile(poolWaitTimes, 0.5), p95: percentile(poolWaitTimes, 0.95) },
        sqlExecMs: { avg: avg(sqlExecTimes), p50: percentile(sqlExecTimes, 0.5), p95: percentile(sqlExecTimes, 0.95) },
        claimLockPoolWaitMs: { avg: avg(claimLockPoolWaitTimes), p50: percentile(claimLockPoolWaitTimes, 0.5), p95: percentile(claimLockPoolWaitTimes, 0.95) },
        claimLockSqlExecMs: { avg: avg(claimLockSqlExecTimes), p50: percentile(claimLockSqlExecTimes, 0.5), p95: percentile(claimLockSqlExecTimes, 0.95) },
        queriesCount: { avg: avg(queriesPerClaim), p50: percentile(queriesPerClaim, 0.5), p95: percentile(queriesPerClaim, 0.95) },
        worksTestedCount: { avg: avg(worksTestedList), p50: percentile(worksTestedList, 0.5), p95: percentile(worksTestedList, 0.95) },
        staffCheckMs: { avg: avg(staffCheckTimes), p50: percentile(staffCheckTimes, 0.5), p95: percentile(staffCheckTimes, 0.95) },
        p0ProbeMs: { avg: avg(p0ProbeTimes), p50: percentile(p0ProbeTimes, 0.5), p95: percentile(p0ProbeTimes, 0.95) },
        criticalWorkTimeMs: { avg: avg(criticalWorkTimes), p50: percentile(criticalWorkTimes, 0.5), p95: percentile(criticalWorkTimes, 0.95) },
        p1WorkTimeMs: { avg: avg(p1WorkTimes), p50: percentile(p1WorkTimes, 0.5), p95: percentile(p1WorkTimes, 0.95) },
        p2WorkTimeMs: { avg: avg(p2WorkTimes), p50: percentile(p2WorkTimes, 0.5), p95: percentile(p2WorkTimes, 0.95) },
        activeFallbackMs: { avg: avg(activeFallbackTimes), p50: percentile(activeFallbackTimes, 0.5), p95: percentile(activeFallbackTimes, 0.95) },
        admissionOnDemandMs: { avg: avg(admissionOnDemandTimes), p50: percentile(admissionOnDemandTimes, 0.5), p95: percentile(admissionOnDemandTimes, 0.95) },
        catalogFallbackMs: { avg: avg(catalogFallbackTimes), p50: percentile(catalogFallbackTimes, 0.5), p95: percentile(catalogFallbackTimes, 0.95) }
      },
      schedulerAcquireOutcomes: {
        attempts: this.schedulerClaimedAcquireSamples.length + this.schedulerEmptyAcquireSamples.length + this.schedulerErrorAcquireSamples.length,
        claimed: this.schedulerClaimedAcquireSamples.length,
        empty: this.schedulerEmptyAcquireSamples.length,
        errors: this.schedulerErrorAcquireSamples.length,
        emptyPercent: this.schedulerClaimedAcquireSamples.length + this.schedulerEmptyAcquireSamples.length + this.schedulerErrorAcquireSamples.length > 0 ? Math.round(this.schedulerEmptyAcquireSamples.length / (this.schedulerClaimedAcquireSamples.length + this.schedulerEmptyAcquireSamples.length + this.schedulerErrorAcquireSamples.length) * 1e3) / 10 : 0,
        claimedMs: { avg: avg(this.schedulerClaimedAcquireSamples), p50: percentile(this.schedulerClaimedAcquireSamples, 0.5), p95: percentile(this.schedulerClaimedAcquireSamples, 0.95) },
        emptyMs: { avg: avg(this.schedulerEmptyAcquireSamples), p50: percentile(this.schedulerEmptyAcquireSamples, 0.5), p95: percentile(this.schedulerEmptyAcquireSamples, 0.95) },
        errorMs: { avg: avg(this.schedulerErrorAcquireSamples), p50: percentile(this.schedulerErrorAcquireSamples, 0.5), p95: percentile(this.schedulerErrorAcquireSamples, 0.95) }
      },
      slowestChapters,
      sourceDistribution: sourceDist,
      limitersAudit: limitersSummary,
      yugabyteDbPool: {
        configuredMax: Number(this.poolRef?.options?.max || 2),
        waitAvgMs: avg(this.dbPoolWaitSamples),
        waitP50Ms: percentile(this.dbPoolWaitSamples, 0.5),
        waitP95Ms: percentile(this.dbPoolWaitSamples, 0.95),
        waitMaxMs: this.dbPoolMaxWaitMs,
        queuedWaitingAvg: avg(this.dbPoolQueuedSamples),
        queuedWaitingPeak: this.dbPoolQueuedSamples.length ? Math.max(...this.dbPoolQueuedSamples) : 0,
        totalQueriesSampled: this.dbPoolWaitSamples.length
      },
      telegramStorage: {
        activeUploadsAvg: avg(this.telegramActiveUploadsSamples),
        activeUploadsP95: percentile(this.telegramActiveUploadsSamples, 0.95),
        activeUploadsPeak: this.telegramActiveUploadsSamples.length ? Math.max(...this.telegramActiveUploadsSamples) : 0,
        pageUploadDurationAvg: avg(this.telegramPageUploadMsSamples),
        pageUploadDurationP95: percentile(this.telegramPageUploadMsSamples, 0.95),
        semaphoreWaitAvgMs: avg(this.telegramSemaphoreWaitSamples),
        semaphoreWaitP95Ms: percentile(this.telegramSemaphoreWaitSamples, 0.95),
        totalBytesUploaded: this.telegramTotalBytesUploaded
      },
      imageDownload: {
        activeRequestsAvg: avg(this.downloadActiveSamples),
        activeRequestsP95: percentile(this.downloadActiveSamples, 0.95),
        activeRequestsPeak: this.downloadActiveSamples.length ? Math.max(...this.downloadActiveSamples) : 0,
        pageDownloadDurationAvg: avg(this.downloadPageMsSamples),
        pageDownloadDurationP95: percentile(this.downloadPageMsSamples, 0.95),
        semaphoreWaitAvgMs: avg(this.downloadSemaphoreWaitSamples),
        semaphoreWaitP95Ms: percentile(this.downloadSemaphoreWaitSamples, 0.95),
        totalBytesDownloaded: this.downloadTotalBytes,
        errorsCount: this.downloadErrorsCount,
        retriesCount: this.downloadRetriesCount
      },
      eventLoopAndNode: {
        uptimeSeconds: process.uptime(),
        cpuTotalMicroseconds: process.cpuUsage().user + process.cpuUsage().system,
        eventLoopLagAvg: avg(this.eventLoopLagSamples),
        eventLoopLagP95: percentile(this.eventLoopLagSamples, 0.95),
        eventLoopLagMax: this.eventLoopLagSamples.length ? Math.max(...this.eventLoopLagSamples) : 0,
        eventLoopUtilizationAvg: avg(this.eluHistory),
        processCpuPercentAvg: avg(this.cpuPercentSamples),
        processCpuPercentPeak: this.cpuPercentSamples.length ? Math.max(...this.cpuPercentSamples) : 0,
        gcPausesCount: this.gcPauseSamples.length,
        gcPausesTotalMs: Math.round(this.gcPauseSamples.reduce((a, b) => a + b, 0)),
        gcPausesMaxMs: this.gcPauseSamples.length ? Math.max(...this.gcPauseSamples) : 0,
        rssMb: Math.round(mem.rss / 1024 / 1024),
        heapUsedMb: Math.round(mem.heapUsed / 1024 / 1024),
        heapTotalMb: Math.round(mem.heapTotal / 1024 / 1024),
        externalMb: Math.round(mem.external / 1024 / 1024),
        arrayBuffersMb: Math.round(mem.arrayBuffers / 1024 / 1024),
        gcPauseP50Ms: percentile(this.gcPauseSamples, 0.5),
        gcPauseP95Ms: percentile(this.gcPauseSamples, 0.95)
      }
    };
  }
  // --- Persistence to DB ---
  async flushTelemetryToDb() {
    if (!this.poolRef || this.flushing) return;
    if (!this.activeSessionId && Date.now() < this.nextSessionCheck) return;
    this.flushing = true;
    try {
      const settingRes = await this.poolRef.query(
        "SELECT value FROM settings WHERE key = 'active_diagnostic_session' LIMIT 1"
      );
      this.nextSessionCheck = Date.now() + 3e4;
      const persistedValue = settingRes.rows[0]?.value;
      let requestedSession = persistedValue;
      if (typeof requestedSession === "string" && (requestedSession.startsWith('"') || requestedSession.startsWith("{"))) {
        try {
          requestedSession = JSON.parse(requestedSession);
        } catch {
        }
      }
      if (requestedSession && requestedSession !== "IDLE") {
        const id = typeof requestedSession === "object" ? requestedSession.id : requestedSession;
        const started = typeof requestedSession === "object" ? Date.parse(requestedSession.started_at) : Number(String(id).match(/(\d{13})$/)?.[1] || Date.now());
        const expires = Math.min(started + 5 * 6e4, typeof requestedSession === "object" ? Date.parse(requestedSession.expires_at) : started + 5 * 6e4);
        if (!Number.isFinite(expires) || Date.now() >= expires) {
          await this.poolRef.query("UPDATE settings SET value = $1 WHERE key = 'active_diagnostic_session' AND value = $2", ["IDLE", persistedValue]);
          this.activeSessionId = null;
        } else if (id !== this.activeSessionId) {
          const session = {
            id,
            started_at: new Date(started).toISOString(),
            expires_at: new Date(expires).toISOString(),
            cpu_profile: requestedSession?.cpu_profile === true
          };
          await this.poolRef.query("UPDATE settings SET value = $1 WHERE key = 'active_diagnostic_session' AND value = $2", [JSON.stringify(session), persistedValue]);
          this.startSession(id, expires);
          if (session.cpu_profile) void this.captureBoundedCpuProfile(id);
        }
      } else {
        this.activeSessionId = null;
      }
      const report = this.getSnapshotReport();
      await this.poolRef.query(`
        INSERT INTO importer_diagnostic_telemetry (id, session_id, data, created_at)
        VALUES ($1, $2, $3::jsonb, NOW())
        ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data, created_at = NOW()
      `, [`session-${this.activeSessionId || "runtime"}`, this.activeSessionId || "runtime", JSON.stringify(report)]);
    } catch (err) {
      if (Date.now() - this.lastFlushWarningAt >= 6e4) {
        this.lastFlushWarningAt = Date.now();
        this.logger.warn("Telemetry persistence failed", { code: err?.code || err?.name });
      }
    } finally {
      this.flushing = false;
    }
  }
};
var telemetryCollector = TelemetryCollector.getInstance();

// src/db/yugabyte-direct.ts
import { performance as performance2 } from "node:perf_hooks";
var logger = new Logger("YugabyteDirect");
var pool = null;
function getSqlFingerprint(queryArg) {
  const sql = String(typeof queryArg === "string" ? queryArg : queryArg?.text || "").replace(/\s+/g, " ").trim();
  const command = sql.match(/^([A-Z]+)/i)?.[1]?.toUpperCase() || "UNKNOWN";
  const rpc = sql.match(/^SELECT\s+([a-z_][a-z0-9_]*)\s*\(/i)?.[1];
  if (rpc?.startsWith("importer_")) return `SELECT rpc:${rpc}`;
  const target = sql.match(/\b(?:FROM|INTO|UPDATE|DELETE\s+FROM)\s+(?:\"?public\"?\.)?\"?([a-z_][a-z0-9_]*)\"?/i)?.[1];
  return target ? `${command} ${target.toLowerCase()}` : command;
}
var EMBEDDED_YUGABYTE_CA = `-----BEGIN CERTIFICATE-----
MIIGxDCCBKygAwIBAgITF8MQH/VpOvqeGxybP3vBoEPj0TANBgkqhkiG9w0BAQsF
ADCBhDELMAkGA1UEBhMCVVMxCzAJBgNVBAgTAkNBMRIwEAYDVQQHEwlTdW5ueXZh
bGUxFTATBgNVBAoTDFl1Z2FieXRlIEluYzEXMBUGA1UECxMOWXVnYWJ5dGUgQ2xv
dWQxJDAiBgNVBAMTG1l1Z2FieXRlIENsb3VkIFJvb3QgQ0EgcHJvZDAeFw0yNTA5
MTgxNjIzMjBaFw0zMDA5MTcxNjIzMTlaMIGWMQswCQYDVQQGEwJVUzELMAkGA1UE
CBMCQ0ExEjAQBgNVBAcTCVN1bm55dmFsZTEXMBUGA1UEChMOWXVnYWJ5dGVEQiBJ
bmMxIDAeBgNVBAsTF1l1Z2FieXRlREIgQWVvbiBNYW5hZ2VkMSswKQYDVQQDEyJZ
dWdhYnl0ZSBDbG91ZCBTdWJvcmRpbmF0ZSBDQSBwcm9kMIIBojANBgkqhkiG9w0B
AQEFAAOCAY8AMIIBigKCAYEAl1CO4UpzZYYDqVuzLUyhNEah0a0VMNzgYQkCxXGN
QOqA0sp60bfJiAaUe1fO1A/VYSur3kpuxWk3/CxBrR6RTsZKpFy1XgeK6McecWUV
ACOtINMwrmHYg0hb032bPSvwDweTddkG9wzuD4Md9+5FK6ebjDhq/kPwSQHhG1lS
qygAcwRJgHcb3Ad6ZrtG5hYNG2Ikt86ipIV/6zgwHYoaDjI4u5Nl5Gqblkea2yn6
M+Z0AM0u+k6FsMkqu9IMXYC466ZTNXHB4pMLVgRkiUvLnUnA/nROGTHmi1ivLGDO
TvKPlEkVvbdZYbutG9isGUg61Fhqb1hRISPdC1uxD4RBoooIBHoOP27gWcdVa+qZ
Q/N6oYy6Yw3R58d0JJrjE5BwppAK9q4EMlRIj9JEbQIyBhtbkqPn+PwwvUHRFavY
s3BKmx2BSuYWaDCSmbDSOLxuf2bzaHIwrhlBgDhWwtVl4F5W1tzDV7FPl19vkl77
UPQo87d3C55rofMJ9BRsdqHbAgMBAAGjggGZMIIBlTAOBgNVHQ8BAf8EBAMCAQYw
HQYDVR0lBBYwFAYIKwYBBQUHAwEGCCsGAQUFBwMCMA8GA1UdEwEB/wQFMAMBAf8w
HQYDVR0OBBYEFLnilBT2qOKcpt+IED4inSpiiLCnMB8GA1UdIwQYMBaAFGCPfEqV
jRfctFj3jSsv+QkB/hI7MIGNBggrBgEFBQcBAQSBgDB+MHwGCCsGAQUFBzAChnBo
dHRwOi8vcHJpdmF0ZWNhLWNvbnRlbnQtNjExZTIyZjYtMDAwMC0yMGM5LWE2NTAt
ZDRmNTQ3ZjdlZTFjLnN0b3JhZ2UuZ29vZ2xlYXBpcy5jb20vYWExNDA4ODNkNGE3
YTJjMDBhMDkvY2EuY3J0MIGCBgNVHR8EezB5MHegdaBzhnFodHRwOi8vcHJpdmF0
ZWNhLWNvbnRlbnQtNjExZTIyZjYtMDAwMC0yMGM5LWE2NTAtZDRmNTQ3ZjdlZTFj
LnN0b3JhZ2UuZ29vZ2xlYXBpcy5jb20vYWExNDA4ODNkNGE3YTJjMDBhMDkvY3Js
LmNybDANBgkqhkiG9w0BAQsFAAOCAgEAt9HGIX1Btl0Fb70GpySsxv94632Kvs6G
1zTL21lpAnLmngezaGOuOzR/swwCIfHj3rf+wFKY6HQ371pLgEUvfe3Ve5aVKIs/
krvGzUIJybaMIZ2rtO88C+X+a+LInRJLJMhJ366X+jTNURLV2S2twCQYDnodyYWA
6z+Fq5fPs+O5khj6qCork6tyEgV9F1bLJNRatNRIYBOhif6GeR3bFOBoSb++d1KB
OrqB5fMIXnqcwV7Zjz8goaWQgeWf1VTyAeavUfKLmZl/SzmKfOGUeux5ezZk77KA
beXE/wMCCARVeV4LPf4nOIcJIciF94A2kZuFpyJmM0VVKbcGuSQe58HxVS3iwmN6
GHGegOqDy8yWknbrjADDyYzJvhUJciKN/Nu0XarXvCSR84BnNfKLB0zBNIUAlAUq
iT3NnyYZYlhD/lHZOtFRphyOQXU6VVkRrRjXer7LF9b7BWYaly1f3FzQf9gwoPyO
sprdOhIsaGIbAzx/hoOgUg28HNyfwih6pI80vGqRg2Zgri8Qb8cOSXVkb6Y0Voic
Y9KaDmilwQAALxGW/H/ScXNExKvWy5o7+H4v3Id/Dr026n1wPE3qOlERoaFmBj0U
+E5UevW1SOwyBjS3XR29cOz89FDIq418DGVpSex2q26ALmNX9E/A0XCPP2bk5lFX
hDzC5OY3o1A=
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIIF+jCCA+KgAwIBAgITQSClZYc7h4sJ9bg7LGKSKpUKFTANBgkqhkiG9w0BAQsF
ADCBhDELMAkGA1UEBhMCVVMxCzAJBgNVBAgTAkNBMRIwEAYDVQQHEwlTdW5ueXZh
bGUxFTATBgNVBAoTDFl1Z2FieXRlIEluYzEXMBUGA1UECxMOWXVnYWJ5dGUgQ2xv
dWQxJDAiBgNVBAMTG1l1Z2FieXRlIENsb3VkIFJvb3QgQ0EgcHJvZDAeFw0yMTA4
MjAwOTI4MTNaFw0zMTA4MTgwOTI4MTJaMIGEMQswCQYDVQQGEwJVUzELMAkGA1UE
CBMCQ0ExEjAQBgNVBAcTCVN1bm55dmFsZTEVMBMGA1UEChMMWXVnYWJ5dGUgSW5j
MRcwFQYDVQQLEw5ZdWdhYnl0ZSBDbG91ZDEkMCIGA1UEAxMbWXVnYWJ5dGUgQ2xv
dWQgUm9vdCBDQSBwcm9kMIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEA
0tTsrSyBfa09rA8ylcZRtxeMLzI3vE3++W9DLV5FK7knsrg45epjcf8zGRLlcKkN
00qaPpTMCwmHvJlyfGhxqrZhKBCtGosRyOvHkLtOhwkW8fHzrx2sm3UTjQpdjv/F
aQxj54YToyMUw66fdMl5PvA+tUbYwZHZEVM9NKtzGE4/j9bZUIQpj+bbJ/el8zY+
WsquZrZ1aA75tC4FzhRYMsEkrRH0iF+T6S3g4VAsn3qRfV+t/aswAAle6gPe+aP3
py5znRnJ5a0kunKEgpL7YJJ5AiqpVjyXNlL3LCvHvB5Lo4AHhVkfYafB8rs/301Q
Frdn4OeZdELv0kI7Ch1nI2/qIakEdodrOT2bTB3E1BMtSfN/z0wGC+sH1Fj3gtQ2
2Ez/AINeDSqJ0tagSU4XMzrLRXy92ToR5trzwy7sEISzxS5BcSuy55lQBhv1vztW
qaC2mfbYrvuVEBb9skF+YDSC+aM/QI5iVGO0m91e1b+okOnZeo7M1YEc5RnjrGOW
a1Q3L6+O/+le/7D8x5cEBBLdwf/DqbFmrIXsSaWMOt+MAopzBPcdyF0NEg//fA6Y
W9pVn8kqWTo1pzY2CIViRIyIFx74D1/fEXLZvjzgckRbxbayNlL/+DHtHkPThbuX
i7BaY3P1mivtgOC0BoZObiVIdX91AB7h4+WjHFf8NGUCAwEAAaNjMGEwDgYDVR0P
AQH/BAQDAgEGMA8GA1UdEwEB/wQFMAMBAf8wHQYDVR0OBBYEFGCPfEqVjRfctFj3
jSsv+QkB/hI7MB8GA1UdIwQYMBaAFGCPfEqVjRfctFj3jSsv+QkB/hI7MA0GCSqG
SIb3DQEBCwUAA4ICAQBn4vQjhhMYEUx+wz9ammb88NTbQvtx3KWgxzhPyR/ekj5X
bW1SxCnQwOTGqbk9rTRdTc5JB0WH4AqD5wijM+qtuYDwUUwkWBGn9XLjy9WN/PCz
X4ePteWvtE06o70EosAG8I7UM7MN1qnZdWoB+qfP9sxx3vyfWGHvHwMFRaq2ea1C
otN5fryj3X/Y3oyIMC0oeSAqcYX97zz9dToNl9Ue8nUDiUo4CHED15VM5RLyx/dO
+ujQ+4OiNQT5mxn8zlM1bOyj+t5mB3E1IGdNtaTcpWulrO4VR/0qrDRHeU26iurM
9GFYl19Z0afo3bYiyNiLV7omNmEcARTAXTPLTI06veZjIafVJwZZTwIoJb9wV7rv
D4cHS9IdEkn5PomMk5X96AOZKWnfvxPsORqgunG9o+azFSgOrLc5MI7OwjGO9M4J
jx6IbjAj2tCrnaE1XPsWR3B3nL6aFtfIYtLMu4vb6HXQ8aYSbocyBO788o7g4vBm
4yNfo1BHB1UCV7UFS5N+MnrUIJITHmJuSkwfFGUxAiNqR5lt0lOW9mFN6FTWH/wT
uHYiQRE3S9hMRqpUOUMmWRRpqHTxYl/FUrqOR8k4g1mN34ZqOpJZOwNnVMOsa+fh
o1DZJoSu3+Cu5ZEv1xOCWs0bOoinIt45bqT0jrEXQDDhwGWR3gC64VMqffla1g==
-----END CERTIFICATE-----`;
function yugabytePoolOptions(cfg, max, applicationName, connectionTimeoutMillis, queryTimeoutMillis, sessionOptions = "", disableParallelWorkers = true) {
  if (!cfg.YUGABYTE_HOST || !cfg.YUGABYTE_USER || !cfg.YUGABYTE_PASSWORD) {
    throw new Error("Yugabyte direct connection requires YUGABYTE_HOST, YUGABYTE_USER, and YUGABYTE_PASSWORD");
  }
  let sslConfig = {
    rejectUnauthorized: true,
    ca: EMBEDDED_YUGABYTE_CA
  };
  const candidateCerts = [
    cfg.YUGABYTE_SSL_CERT,
    path2.resolve(process.cwd(), "certs/yugabyte-root.crt"),
    path2.resolve(path2.dirname(fileURLToPath2(import.meta.url)), "../../certs/yugabyte-root.crt"),
    path2.resolve(path2.dirname(fileURLToPath2(import.meta.url)), "../certs/yugabyte-root.crt"),
    path2.resolve(process.cwd(), "build/certs/yugabyte-root.crt"),
    "/home/node/certs/yugabyte-root.crt",
    "/home/node/app/certs/yugabyte-root.crt",
    path2.resolve(process.cwd(), "config/root.crt"),
    path2.resolve(path2.dirname(fileURLToPath2(import.meta.url)), "../../config/root.crt"),
    "/home/awerkori/.config/project-nox/root.crt"
  ].filter(Boolean);
  for (const certPath of candidateCerts) {
    if (fs2.existsSync(certPath)) {
      sslConfig = {
        rejectUnauthorized: true,
        ca: fs2.readFileSync(certPath, "utf8")
      };
      break;
    }
  }
  return {
    host: cfg.YUGABYTE_HOST,
    port: cfg.YUGABYTE_PORT,
    user: cfg.YUGABYTE_USER,
    password: cfg.YUGABYTE_PASSWORD,
    database: cfg.YUGABYTE_DATABASE,
    max,
    connectionTimeoutMillis,
    idleTimeoutMillis: 3e4,
    query_timeout: queryTimeoutMillis,
    keepAlive: true,
    keepAliveInitialDelayMillis: 1e4,
    ssl: sslConfig,
    application_name: applicationName,
    // Yugabyte rejects this deployment's startup packet when four -c flags
    // are present. The isolated P2 pool needs the batched-loop setting more
    // than the parallel-worker hint; retain all safeguards in the main pool.
    options: `${disableParallelWorkers ? "-c max_parallel_workers_per_gather=0 " : ""}-c statement_timeout=${queryTimeoutMillis} -c idle_in_transaction_session_timeout=15000 ${sessionOptions}`.trim()
  };
}
function getYugabytePool() {
  if (pool) return pool;
  const cfg = getConfig();
  pool = new pg.Pool(yugabytePoolOptions(
    cfg,
    cfg.DIRECT_DB_POOL_MAX || parseInt(process.env.DIRECT_DB_POOL_MAX || "4", 10),
    "project-nox-importer-direct",
    25e3,
    25e3
  ));
  pool.on("error", (err) => {
    logger.error("Unexpected error on idle direct Yugabyte client", { error: err.message });
  });
  pool.on("connect", (client) => {
    const query = client.query.bind(client);
    let transactionStarted = 0;
    client.query = (...args) => {
      const start = performance2.now();
      const command = String(typeof args[0] === "string" ? args[0] : args[0]?.text || "").trim().split(/\s+/, 1)[0].toUpperCase();
      const fingerprint = getSqlFingerprint(args[0]);
      const finish = () => {
        telemetryCollector.recordDbQuery(performance2.now() - start, fingerprint);
        if (command === "BEGIN") transactionStarted = start;
        if ((command === "COMMIT" || command === "ROLLBACK") && transactionStarted) {
          telemetryCollector.recordDbTransaction(performance2.now() - transactionStarted);
          transactionStarted = 0;
        }
      };
      const callbackIndex = args.findIndex((a) => typeof a === "function");
      if (callbackIndex >= 0) {
        const callback = args[callbackIndex];
        args[callbackIndex] = (...result) => {
          finish();
          return callback(...result);
        };
        return query(...args);
      }
      try {
        return query(...args).finally(finish);
      } catch (error) {
        finish();
        throw error;
      }
    };
  });
  const trackHold = (client) => {
    const started = performance2.now();
    const release = client.release;
    client.release = (...args) => {
      telemetryCollector.recordDbHold(performance2.now() - started);
      return release.apply(client, args);
    };
    return client;
  };
  const origConnect = pool.connect.bind(pool);
  pool.connect = function(...args) {
    const t0 = performance2.now();
    const queued = pool.waitingCount || 0;
    const cb = typeof args[0] === "function" ? args[0] : typeof args[1] === "function" ? args[1] : null;
    if (cb) {
      return origConnect((err, client, done) => {
        const waitMs = performance2.now() - t0;
        telemetryCollector.recordDbPoolWait(waitMs, queued);
        if (client) trackHold(client);
        cb(err, client, client?.release || done);
      });
    }
    return origConnect(...args).then((client) => {
      const waitMs = performance2.now() - t0;
      telemetryCollector.recordDbPoolWait(waitMs, queued);
      return trackHold(client);
    }).catch((err) => {
      const waitMs = performance2.now() - t0;
      telemetryCollector.recordDbPoolWait(waitMs, queued);
      throw err;
    });
  };
  telemetryCollector.setPool(pool);
  return pool;
}

// scripts/retire-blocked-sources-production.ts
dotenv2.config({
  path: process.env.NOX_ENV_FILE || "/home/awerkori/.Projects/project-nox-importer/.env",
  override: false
});
var RETIREMENTS = [
  { source: "acervohentai", ids: ["acervohentai"], reason: "AUDIT_2026_10_JS_CHALLENGE" },
  { source: "amuy", ids: ["amuy"], reason: "AUDIT_2026_10_JS_CHALLENGE" },
  { source: "arthurscan", ids: ["arthurscan"], reason: "AUDIT_2026_10_JS_CHALLENGE" },
  { source: "geasscomics", ids: ["geasscomics"], reason: "AUDIT_2026_10_JS_CHALLENGE" },
  { source: "inkapk", ids: ["inkapk"], reason: "AUDIT_2026_10_JS_CHALLENGE" },
  { source: "mangaonline", ids: ["mangaonline"], reason: "AUDIT_2026_10_JS_CHALLENGE" },
  { source: "mangastop", ids: ["mangastop"], reason: "AUDIT_2026_10_JS_CHALLENGE" },
  {
    source: "pointzerotoons",
    ids: ["pointzerotoons", "pointzero", "point_zero_toons", "kitsuneyako"],
    reason: "AUDIT_2026_10_JS_CHALLENGE"
  },
  { source: "vegitoons", ids: ["vegitoons"], reason: "AUDIT_2026_10_API_BLOCK" },
  { source: "yaoifanclub", ids: ["yaoifanclub"], reason: "AUDIT_2026_10_JS_CHALLENGE" },
  { source: "yuriverso", ids: ["yuriverso"], reason: "AUDIT_2026_10_JS_CHALLENGE" }
];
var apply = process.argv.includes("--apply");
var confirmed = process.argv.includes("--confirm-retire-blocked-sources");
async function count(client, sql, params = []) {
  const result = await client.query(sql, params);
  return Number(result.rows[0]?.count || 0);
}
async function archiveAndDelete(client, source, runId, recordType, table, where, params, originalId) {
  const archiveWhere = where.replace(/\$(\d+)/g, (_match, index) => `$${Number(index) + 3}`);
  const archived = await client.query(
    `INSERT INTO public.importer_retired_source_records
       (source, record_type, original_id, retirement_run_id, record)
     SELECT $1::text, $2::text, ${originalId}, $3::uuid, to_jsonb(row)
       FROM public.${table} AS row
      WHERE ${archiveWhere}
     ON CONFLICT (source, record_type, original_id) DO UPDATE
       SET retirement_run_id = EXCLUDED.retirement_run_id,
           retired_at = now(),
           record = EXCLUDED.record`,
    [source, recordType, runId, ...params]
  );
  const deleted = await client.query(`DELETE FROM public.${table} AS row WHERE ${where}`, params);
  if ((archived.rowCount ?? 0) !== (deleted.rowCount ?? 0)) {
    throw new Error(`Archive/delete mismatch for ${source}/${recordType}`);
  }
  return deleted.rowCount ?? 0;
}
async function publishedSnapshot(client, ids, knownChapterIds) {
  const candidateIds = knownChapterIds ?? [...new Set((await client.query(
    `SELECT chapter_id
       FROM public.importer_chapter_mappings
      WHERE source = ANY($1::text[])
        AND chapter_id IS NOT NULL`,
    [ids]
  )).rows.map((row) => row.chapter_id))];
  if (candidateIds.length === 0) {
    return { totals: { chapters: 0, pages: 0, storedPages: 0, unavailablePages: 0 }, chapterIds: [] };
  }
  const chapters = await client.query(
    `SELECT id FROM public.chapters WHERE id = ANY($1::uuid[]) AND published_at IS NOT NULL`,
    [candidateIds]
  );
  const chapterIds = chapters.rows.map((row) => row.id);
  const totals = { chapters: chapterIds.length, pages: 0, storedPages: 0, unavailablePages: 0 };
  for (let offset = 0; offset < chapterIds.length; offset += 50) {
    const pages = await client.query(
      `SELECT count(page.media_id)::int AS pages,
              count(*) FILTER (WHERE media.storage_ready IS TRUE)::int AS stored_pages,
              count(*) FILTER (WHERE media.storage_ready IS DISTINCT FROM TRUE)::int AS unavailable_pages
         FROM public.pages AS page
         LEFT JOIN public.media AS media ON media.id = page.media_id
        WHERE page.chapter_id = ANY($1::uuid[])`,
      [chapterIds.slice(offset, offset + 50)]
    );
    totals.pages += Number(pages.rows[0]?.pages || 0);
    totals.storedPages += Number(pages.rows[0]?.stored_pages || 0);
    totals.unavailablePages += Number(pages.rows[0]?.unavailable_pages || 0);
  }
  return { totals, chapterIds };
}
async function inventory(client, retirement) {
  const ids = [...retirement.ids];
  const workPatterns = ids.map((source) => `source:${source}:%`);
  const params = [ids];
  const published = await publishedSnapshot(client, ids);
  return {
    workMappings: await count(client, `SELECT count(*)::int AS count FROM public.importer_work_mappings WHERE source = ANY($1::text[])`, params),
    chapterMappings: await count(client, `SELECT count(*)::int AS count FROM public.importer_chapter_mappings WHERE source = ANY($1::text[])`, params),
    legacyTasks: await count(client, `SELECT count(*)::int AS count FROM public.importer_queue WHERE source = ANY($1::text[])`, params),
    liteTasks: await count(client, `SELECT count(*)::int AS count FROM public.importer_lite_tasks WHERE COALESCE(payload->>'source', '') = ANY($1::text[]) OR work_key LIKE ANY($2::text[])`, [ids, workPatterns]),
    metadata: await count(client, `SELECT count(*)::int AS count FROM public.importer_lite_metadata_enrichment WHERE source = ANY($1::text[])`, params),
    liteRoutes: await count(client, `SELECT count(*)::int AS count FROM public.importer_lite_chapter_sources WHERE source = ANY($1::text[])`, params),
    sourceHealth: await count(client, `SELECT count(*)::int AS count FROM public.importer_lite_source_health WHERE source = ANY($1::text[])`, params),
    checkpoints: await count(client, `SELECT count(*)::int AS count FROM public.importer_checkpoints WHERE source = ANY($1::text[])`, params),
    readyWorks: await count(client, `SELECT count(*)::int AS count FROM public.importer_lite_ready_works WHERE work_key LIKE ANY($1::text[])`, [workPatterns]),
    manifests: await count(client, `SELECT count(*)::int AS count FROM public.importer_chapter_manifest WHERE selected_source = ANY($1::text[]) OR available_sources ?| $1::text[]`, params),
    runningLite: await count(client, `SELECT count(*)::int AS count FROM public.importer_lite_tasks WHERE state = 'RUNNING' AND (COALESCE(payload->>'source', '') = ANY($1::text[]) OR work_key LIKE ANY($2::text[]))`, [ids, workPatterns]),
    importingLegacy: await count(client, `SELECT count(*)::int AS count FROM public.importer_queue WHERE status = 'IMPORTING' AND source = ANY($1::text[])`, params),
    dependentUploads: await count(client, `SELECT count(*)::int AS count FROM public.importer_lite_uploads AS upload JOIN public.importer_lite_tasks AS task ON task.id = upload.task_id WHERE COALESCE(task.payload->>'source', '') = ANY($1::text[]) OR task.work_key LIKE ANY($2::text[])`, [ids, workPatterns]),
    published
  };
}
async function resetMetadataCursor(client, source, runId, ids) {
  const cursor = await client.query(
    `UPDATE public.importer_lite_metadata_source_cursor AS row
        SET last_source = NULL, updated_at = now()
      WHERE row.last_source = ANY($1::text[])
    RETURNING to_jsonb(row) AS record, row.cursor_name`,
    [ids]
  );
  for (const row of cursor.rows) {
    await client.query(
      `INSERT INTO public.importer_retired_source_records
         (source, record_type, original_id, retirement_run_id, record)
       VALUES ($1, 'METADATA_SOURCE_CURSOR', $2, $3, $4)
       ON CONFLICT (source, record_type, original_id) DO UPDATE
         SET retirement_run_id = EXCLUDED.retirement_run_id,
             retired_at = now(), record = EXCLUDED.record`,
      [source, row.cursor_name, runId, row.record]
    );
  }
  return cursor.rowCount ?? 0;
}
async function retireOne(client, retirement) {
  await client.query("BEGIN");
  try {
    await client.query("SELECT pg_advisory_xact_lock($1)", [7301019]);
    const prior = await client.query(
      `SELECT id, counts
         FROM public.importer_source_retirement_runs
        WHERE source = $1 AND reason = $2 AND completed_at IS NOT NULL
        ORDER BY completed_at DESC LIMIT 1`,
      [retirement.source, retirement.reason]
    );
    const before = await inventory(client, retirement);
    const operationalRows = before.workMappings + before.chapterMappings + before.legacyTasks + before.liteTasks + before.metadata + before.liteRoutes + before.sourceHealth + before.checkpoints + before.readyWorks;
    if (prior.rowCount && operationalRows === 0) {
      await client.query("COMMIT");
      return { source: retirement.source, mode: "already-retired", runId: prior.rows[0].id, counts: prior.rows[0].counts };
    }
    if (before.runningLite || before.importingLegacy) {
      throw new Error(`Refusing ${retirement.source}: active leases (lite=${before.runningLite}, legacy=${before.importingLegacy})`);
    }
    if (before.dependentUploads) throw new Error(`Refusing ${retirement.source}: ${before.dependentUploads} upload receipt(s) depend on source tasks`);
    if (before.manifests) throw new Error(`Refusing ${retirement.source}: ${before.manifests} manifest route(s) still reference the source`);
    if (before.published.totals.unavailablePages) {
      throw new Error(`Refusing ${retirement.source}: ${before.published.totals.unavailablePages} public page(s) are not stored`);
    }
    const run = await client.query(
      `INSERT INTO public.importer_source_retirement_runs (source, reason) VALUES ($1, $2) RETURNING id`,
      [retirement.source, retirement.reason]
    );
    const runId = String(run.rows[0].id);
    const ids = [...retirement.ids];
    const workPatterns = ids.map((source) => `source:${source}:%`);
    const counts = {};
    counts.liteReadyWorks = await archiveAndDelete(client, retirement.source, runId, "LITE_READY_WORK", "importer_lite_ready_works", "row.work_key LIKE ANY($1::text[])", [workPatterns], "row.work_key");
    counts.liteTasks = await archiveAndDelete(client, retirement.source, runId, "LITE_TASK", "importer_lite_tasks", "COALESCE(row.payload->>'source', '') = ANY($1::text[]) OR row.work_key LIKE ANY($2::text[])", [ids, workPatterns], "row.id::text");
    counts.liteChapterSources = await archiveAndDelete(client, retirement.source, runId, "LITE_CHAPTER_SOURCE", "importer_lite_chapter_sources", "row.source = ANY($1::text[])", [ids], "row.id::text");
    counts.metadataEnrichment = await archiveAndDelete(client, retirement.source, runId, "METADATA_ENRICHMENT", "importer_lite_metadata_enrichment", "row.source = ANY($1::text[])", [ids], "row.source || ':' || row.source_work_id");
    counts.sourceHealth = await archiveAndDelete(client, retirement.source, runId, "LITE_SOURCE_HEALTH", "importer_lite_source_health", "row.source = ANY($1::text[])", [ids], "row.source");
    counts.checkpoints = await archiveAndDelete(client, retirement.source, runId, "CHECKPOINT", "importer_checkpoints", "row.source = ANY($1::text[])", [ids], "row.id::text");
    counts.legacyTasks = await archiveAndDelete(client, retirement.source, runId, "LEGACY_QUEUE_TASK", "importer_queue", "row.source = ANY($1::text[])", [ids], "row.id::text");
    counts.chapterMappings = await archiveAndDelete(client, retirement.source, runId, "CHAPTER_MAPPING", "importer_chapter_mappings", "row.source = ANY($1::text[])", [ids], "row.id::text");
    counts.workMappings = await archiveAndDelete(client, retirement.source, runId, "WORK_MAPPING", "importer_work_mappings", "row.source = ANY($1::text[])", [ids], "row.id::text");
    counts.metadataCursorsReset = await resetMetadataCursor(client, retirement.source, runId, ids);
    await client.query(
      `UPDATE public.importer_sources
          SET enabled = false, status = 'DISABLED',
              chapter_ingestion_enabled = false, catalog_discovery_enabled = false,
              cooldown_until = NULL,
              blocked_reason = 'Retired after production source audit: ' || $2::text,
              blocked_details = jsonb_build_object('sourceRetirement', '2026-10-18', 'reason', $2::text, 'audit', 'production-admission-gate'),
              config = COALESCE(config, '{}'::jsonb) || jsonb_build_object('sourceRetirement', '2026-10-18', 'sourceRetirementReason', $2::text),
              updated_at = now()
        WHERE id = ANY($1::text[])`,
      [ids, retirement.reason]
    );
    const after = await inventory(client, retirement);
    const remaining = after.workMappings + after.chapterMappings + after.legacyTasks + after.liteTasks + after.metadata + after.liteRoutes + after.sourceHealth + after.checkpoints + after.readyWorks;
    if (remaining !== 0) throw new Error(`${retirement.source} retirement verification failed; ${remaining} operational row(s) remain`);
    const afterPublished = (await publishedSnapshot(client, ids, before.published.chapterIds)).totals;
    if (JSON.stringify(before.published.totals) !== JSON.stringify(afterPublished)) {
      throw new Error(`Published content preservation verification failed for ${retirement.source}`);
    }
    await client.query(
      `UPDATE public.importer_source_retirement_runs
          SET completed_at = now(), counts = $2::jsonb, updated_at = now()
        WHERE id = $1`,
      [runId, JSON.stringify({ ...counts, publishedBefore: before.published.totals, publishedAfter: afterPublished })]
    );
    await client.query("COMMIT");
    return { source: retirement.source, mode: "retired", runId, counts, published: afterPublished };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  }
}
async function main() {
  if (apply && !confirmed) throw new Error("Refusing destructive operation without --confirm-retire-blocked-sources");
  console.log(JSON.stringify({ mode: apply ? "apply-started" : "dry-run-started", sources: RETIREMENTS.map(({ source }) => source) }));
  const pool2 = getYugabytePool();
  const client = await pool2.connect();
  try {
    if (!apply) {
      const sources = [];
      for (const retirement of RETIREMENTS) {
        const result = await inventory(client, retirement);
        const { chapterIds: _chapterIds, ...published } = result.published;
        sources.push({ source: retirement.source, reason: retirement.reason, ...result, published });
      }
      console.log(JSON.stringify({ mode: "dry-run", sources }));
      return;
    }
    await client.query(readFileSync(resolve("migrations/20261018010000_retire_kuro_operational_state.sql"), "utf8"));
    await client.query(readFileSync(resolve("migrations/20261018090000_retire_persistently_blocked_sources.sql"), "utf8"));
    const results = [];
    for (const retirement of RETIREMENTS) results.push(await retireOne(client, retirement));
    console.log(JSON.stringify({ mode: "applied", results }));
  } finally {
    client.release();
    await pool2.end();
  }
}
try {
  await main();
} catch (error) {
  console.error(`Blocked-source retirement failed: ${error instanceof Error ? error.message : String(error)}`);
  process.exitCode = 1;
}
