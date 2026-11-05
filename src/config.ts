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
} catch {
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
  NOX_MANGA_URL: z.string().url().default('https://projectnox.org'),
  NOX_IMPORTER_GATEWAY_URL: z.string().url().default('https://project-nox-importer-gateway.project-nox-awerkori.workers.dev'),
  TELEGRAM_BOT_TOKEN: z.string().optional(),
  TELEGRAM_CHAT_ID: z.string().optional(),
  IMPORTER_USER_ID: z.string().uuid().optional(),
  WORKER_ID: z.string().min(1).default(() => `nox-worker-${process.pid}-${Math.random().toString(36).slice(2, 7)}`),
  POLL_INTERVAL_SECONDS: z.coerce.number().int().min(5).default(60),
  QUEUE_LEASE_DURATION_SECONDS: z.coerce.number().int().min(30).default(600),
  QUEUE_HEARTBEAT_INTERVAL_SECONDS: z.coerce.number().int().min(10).default(60),
  MAX_CONCURRENT_CHAPTERS: z.coerce.number().int().min(1).max(128).default(2),
  // Lite does not inherit the legacy chapter-worker setting. The controlled
  // worker capacity remains strictly bounded to 2 to preserve YugabyteDB headroom.
  LITE_CHAPTER_WORKERS: z.coerce.number().int().min(1).max(32).default(2),
  ADAPTIVE_INITIAL_CONCURRENCY: z.coerce.number().int().min(1).max(128).default(2),
  // Raising this ceiling requires a measured production ramp.
  TESTED_CONCURRENCY_CEILING: z.coerce.number().int().min(1).max(128).default(2),
  BATCH_PAGE_DOWNLOAD_CONCURRENCY: z.coerce.number().int().min(1).max(32).default(4),
  // Aeon has only 13 backend slots. These are hard per-process ceilings;
  // deployment overlap must still be prevented by the runtime leader. Direct
  // pool is capped to 1 and metadata pool to 1 to reserve headroom for the site.
  DIRECT_DB_POOL_MAX: z.coerce.number().int().min(1).max(2).default(1),
  METADATA_DB_POOL_MAX: z.coerce.number().int().min(1).max(1).default(1),
  // At the measured 52.75 MiB median chapter size, the former 4 MiB/s global
  // ceiling could never reach the 8/min acceptance floor (4.55/min before
  // network and publication overhead). Eight MiB/s is the first reversible
  // ramp; bot, shard, request and global-media limits still apply.
  UPLOAD_RATE_LIMIT_BYTES_PER_SEC: z.coerce.number().int().default(8 * 1024 * 1024),
  // Bounded media concurrency to protect DB connection manager and CPU.
  TELEGRAM_MEDIA_CONCURRENCY: z.coerce.number().int().min(1).max(32).default(6),
  DOWNLOAD_INFLIGHT_CONCURRENCY: z.coerce.number().int().min(1).max(48).default(12),
  BUFFERED_PAGE_CONCURRENCY: z.coerce.number().int().min(1).max(96).default(24),
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

export type Config = z.infer<typeof ConfigSchema>;

let _config: Config | null = null;

export function getConfig(): Config {
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

export function setTestConfig(overrides: Partial<Config>): void {
  _config = { ...getConfig(), ...overrides };
}
