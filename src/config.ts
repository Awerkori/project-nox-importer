import { z } from 'zod';
import dotenv from 'dotenv';

dotenv.config();

const ConfigSchema = z.object({
  SUPABASE_URL: z.string().url().default('https://placeholder.supabase.co'),
  SUPABASE_SERVICE_ROLE_KEY: z.string().min(1).default('placeholder-service-role-key'),
  STORAGE_PROVIDER: z.enum(['worker', 'telegram', 'mock']).default('worker'),
  NOX_STORAGE_BRIDGE_TOKEN: z.string().optional(),
  NOX_MANGA_URL: z.string().url().default('https://manga.project-nox-awerkori.workers.dev'),
  TELEGRAM_BOT_TOKEN: z.string().optional(),
  TELEGRAM_CHAT_ID: z.string().optional(),
  IMPORTER_USER_ID: z.string().uuid().optional(),
  WORKER_ID: z.string().min(1).default(() => `nox-worker-${process.pid}-${Math.random().toString(36).slice(2, 7)}`),
  POLL_INTERVAL_SECONDS: z.coerce.number().int().min(5).default(60),
  QUEUE_LEASE_DURATION_SECONDS: z.coerce.number().int().min(30).default(300),
  QUEUE_HEARTBEAT_INTERVAL_SECONDS: z.coerce.number().int().min(10).default(60),
  MAX_CONCURRENT_CHAPTERS: z.coerce.number().int().min(1).max(128).default(32),
  BATCH_PAGE_DOWNLOAD_CONCURRENCY: z.coerce.number().int().min(1).max(32).default(8),
  LOG_LEVEL: z.enum(['debug', 'info', 'warn', 'error']).default('info'),
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
