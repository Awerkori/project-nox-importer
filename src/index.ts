import { createClient } from '@supabase/supabase-js';
import { getConfig } from './config.js';
import { rootLogger } from './core/logger.js';
import { HostRateLimiter } from './core/rate-limiter.js';
import { SourceRegistry } from './sources/registry.js';
import { StorageProvider } from './storage/provider.js';
import { NoxWorkerStorageProvider } from './storage/worker.js';
import { TelegramStorageProvider } from './storage/telegram.js';
import { MockStorageProvider } from './storage/mock.js';
import { ImporterEngine } from './core/engine.js';
import { HealthMonitor } from './core/health.js';

async function main() {
  rootLogger.info('Starting Project Nox Importer daemon...');

  const config = getConfig();

  // 1. Initialize Supabase Client with service_role
  const supabase = createClient(config.SUPABASE_URL, config.SUPABASE_SERVICE_ROLE_KEY, {
    auth: { persistSession: false, autoRefreshToken: false },
  });

  // 2. Initialize Storage Provider
  let storage: StorageProvider;
  if (config.STORAGE_PROVIDER === 'worker') {
    if (!config.NOX_STORAGE_BRIDGE_TOKEN) {
      rootLogger.warn('Bridge token not provided, falling back to mock storage for safety');
      storage = new MockStorageProvider();
    } else {
      storage = new NoxWorkerStorageProvider(config.NOX_MANGA_URL, config.NOX_STORAGE_BRIDGE_TOKEN);
    }
  } else if (config.STORAGE_PROVIDER === 'telegram') {
    if (!config.TELEGRAM_BOT_TOKEN || !config.TELEGRAM_CHAT_ID) {
      rootLogger.warn('Telegram credentials not provided, falling back to mock storage for safety');
      storage = new MockStorageProvider();
    } else {
      storage = new TelegramStorageProvider(config.TELEGRAM_BOT_TOKEN, config.TELEGRAM_CHAT_ID);
    }
  } else {
    storage = new MockStorageProvider();
  }

  // 3. Health & Readiness check
  const health = new HealthMonitor(supabase, storage);
  const telemetry = await health.getCompactTelemetry();
  rootLogger.info(`Initial boot status: ${telemetry}`);

  // 4. Initialize Rate Limiter & Source Registry
  const rateLimiter = new HostRateLimiter(2.0);
  const registry = new SourceRegistry(rateLimiter);

  // 5. Initialize Importer Engine
  const engine = new ImporterEngine(supabase, storage, registry, rateLimiter, config);

  // 6. Graceful Shutdown Handlers
  let isShuttingDown = false;
  const shutdown = (signal: string) => {
    if (isShuttingDown) return;
    isShuttingDown = true;
    rootLogger.info(`Received ${signal}, initiating graceful shutdown...`);
    engine.stop();
    setTimeout(() => {
      rootLogger.warn('Forced shutdown after timeout (15s limit reached)');
      process.exit(1);
    }, 15_000).unref();
  };

  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));

  process.on('unhandledRejection', (reason: any) => {
    rootLogger.error('Unhandled Promise Rejection caught in daemon', {
      error: reason?.message || String(reason),
      stack: reason?.stack,
    });
  });

  process.on('uncaughtException', (err: Error) => {
    rootLogger.error('Uncaught Exception caught in daemon', {
      error: err?.message,
      stack: err?.stack,
    });
  });

  // 7. Start Engine
  await engine.start();
}

main().catch((err) => {
  rootLogger.error('Fatal initialization error in importer daemon', {
    error: err?.message,
    stack: err?.stack,
  });
  process.exit(1);
});
