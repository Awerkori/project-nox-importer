import { z } from 'zod';
import dotenv from 'dotenv';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';
// Multi-path dotenv resolution for Discloud container environments
const candidateEnvPaths = [
    path.resolve(process.cwd(), '.env'),
    '/home/node/.env',
    '/home/node/app/.env',
];
try {
    const currentDir = path.dirname(fileURLToPath(import.meta.url));
    candidateEnvPaths.push(path.resolve(currentDir, '../.env'));
    candidateEnvPaths.push(path.resolve(currentDir, '../../.env'));
}
catch {
    // ignore
}
for (const envPath of candidateEnvPaths) {
    if (fs.existsSync(envPath)) {
        dotenv.config({ path: envPath });
    }
}
dotenv.config();
const ConfigSchema = z.object({
    SUPABASE_URL: z.string().url().default('https://placeholder.supabase.co'),
    SUPABASE_SERVICE_ROLE_KEY: z.string().min(1).default('placeholder-service-role-key'),
    STORAGE_PROVIDER: z.enum(['worker', 'telegram', 'mock', 'direct_telegram']).default('direct_telegram'),
    NOX_STORAGE_BRIDGE_TOKEN: z.string().optional(),
    NOX_MANGA_URL: z.string().url().default('https://manga.project-nox-awerkori.workers.dev'),
    NOX_IMPORTER_GATEWAY_URL: z.string().url().default('https://project-nox-importer-gateway.project-nox-awerkori.workers.dev'),
    TELEGRAM_BOT_TOKEN: z.string().optional(),
    TELEGRAM_CHAT_ID: z.string().optional(),
    IMPORTER_USER_ID: z.string().uuid().optional(),
    WORKER_ID: z.string().min(1).default(() => `nox-worker-${process.pid}-${Math.random().toString(36).slice(2, 7)}`),
    POLL_INTERVAL_SECONDS: z.coerce.number().int().min(5).default(60),
    QUEUE_LEASE_DURATION_SECONDS: z.coerce.number().int().min(30).default(600),
    QUEUE_HEARTBEAT_INTERVAL_SECONDS: z.coerce.number().int().min(10).default(60),
    MAX_CONCURRENT_CHAPTERS: z.coerce.number().int().min(1).max(128).default(10),
    // Lite does not inherit the legacy chapter-worker setting. The measured
    // sixth-slot ramp remains inside the 24-request/page-buffer fences: it
    // fills gaps between bounded six-page waves rather than multiplying
    // download, buffer, Telegram or DB concurrency. An explicit protected
    // value still wins immediately (including a one-worker rollback) without
    // reviving legacy behavior.
    LITE_CHAPTER_WORKERS: z.coerce.number().int().min(1).max(32).default(6),
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
    SOURCE_OPERATION_TIMEOUT_MS: z.coerce.number().int().min(5_000).max(120_000).default(30_000),
    LOG_LEVEL: z.enum(['debug', 'info', 'warn', 'error']).default('info'),
    IMPORTER_DB_MODE: z.enum(['direct', 'gateway']).default('direct'),
    // This branch is the approved Lite cutover artifact.  An explicit `legacy`
    // setting remains available only for the documented rollback procedure; a
    // fresh deployment must never silently revive the v1 scheduler because an
    // environment variable was omitted by the native DIScloud Git integration.
    IMPORTER_RUNTIME_MODE: z.enum(['legacy', 'lite']).default('lite'),
    YUGABYTE_HOST: z.string().default('sa-east-1.b49305ea-8536-43e6-936e-b2fd77fc07b0.aws.yugabyte.cloud'),
    YUGABYTE_PORT: z.coerce.number().default(5433),
    YUGABYTE_USER: z.string().default('admin'),
    YUGABYTE_PASSWORD: z.string().min(1, 'YUGABYTE_PASSWORD environment variable is required'),
    YUGABYTE_DATABASE: z.string().default('project_nox_prod'),
    YUGABYTE_SSL_CERT: z.string().default('certs/yugabyte-root.crt'),
});
let _config = null;
export function getConfig() {
    if (!_config) {
        const result = ConfigSchema.safeParse(process.env);
        if (!result.success) {
            console.error('Invalid configuration:', result.error.format());
            throw new Error('Invalid environment configuration');
        }
        _config = result.data;
    }
    return _config;
}
export function setTestConfig(overrides) {
    _config = { ...getConfig(), ...overrides };
}
