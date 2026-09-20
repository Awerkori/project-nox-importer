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
  NOX_MANGA_URL: z.string().url().default('https://manga.project-nox-awerkori.workers.dev'),
  NOX_IMPORTER_GATEWAY_URL: z.string().url().default('https://project-nox-importer-gateway.project-nox-awerkori.workers.dev'),
  TELEGRAM_BOT_TOKEN: z.string().optional(),
  TELEGRAM_CHAT_ID: z.string().optional(),
  IMPORTER_USER_ID: z.string().uuid().optional(),
  WORKER_ID: z.string().min(1).default(() => `nox-worker-${process.pid}-${Math.random().toString(36).slice(2, 7)}`),
  POLL_INTERVAL_SECONDS: z.coerce.number().int().min(5).default(60),
  QUEUE_LEASE_DURATION_SECONDS: z.coerce.number().int().min(30).default(300),
  QUEUE_HEARTBEAT_INTERVAL_SECONDS: z.coerce.number().int().min(10).default(60),
  MAX_CONCURRENT_CHAPTERS: z.coerce.number().int().min(1).max(128).default(8),
  // Raising this ceiling requires a measured production ramp. Legacy MAX=32 cannot override it.
  TESTED_CONCURRENCY_CEILING: z.coerce.number().int().min(1).max(128).default(32),
  BATCH_PAGE_DOWNLOAD_CONCURRENCY: z.coerce.number().int().min(1).max(32).default(8),
  LOG_LEVEL: z.enum(['debug', 'info', 'warn', 'error']).default('info'),
  IMPORTER_DB_MODE: z.enum(['direct', 'gateway']).default('direct'),
  YUGABYTE_HOST: z.string().default('sa-east-1.b49305ea-8536-43e6-936e-b2fd77fc07b0.aws.yugabyte.cloud'),
  YUGABYTE_PORT: z.coerce.number().default(5433),
  YUGABYTE_USER: z.string().default('admin'),
  YUGABYTE_PASSWORD: z.string().default('rxpJQWQ3bVNHfv68K_4RkvgOgRv0zF'),
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
