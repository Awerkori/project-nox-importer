import fsSync from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { execFileSync } from 'node:child_process';
import dns from 'node:dns';
import WebSocket from 'ws';

// Ensure IPv4 is prioritized for external DNS queries on Discloud containers
dns.setDefaultResultOrder?.('ipv4first');
import { ImporterGatewayClient } from './core/gateway-client.js';
import { GatewaySupabaseClient } from './core/gateway-supabase.js';
import { DirectSupabaseClient } from './db/direct-supabase-client.js';
import { createYugabyteMetadataPool } from './db/yugabyte-direct.js';
import { getConfig } from './config.js';

// Polyfill native WebSocket for Node environments (e.g. Node 20 on DIScloud) where native WebSocket is missing
if (typeof (globalThis as any).WebSocket === 'undefined') {
  (globalThis as any).WebSocket = WebSocket;
}
import { rootLogger } from './core/logger.js';
import { HostRateLimiter } from './core/rate-limiter.js';
import { SourceRegistry } from './sources/registry.js';
import { StorageProvider } from './storage/provider.js';
import { DirectTelegramStorageProvider } from './storage/direct-telegram.js';
import { NoxWorkerStorageProvider } from './storage/worker.js';
import { TelegramStorageProvider } from './storage/telegram.js';
import { MockStorageProvider } from './storage/mock.js';
import { ImporterEngine } from './core/engine.js';
import { HealthMonitor } from './core/health.js';
import { diagnostics } from './core/diagnostics.js';
import { telemetryCollector } from './core/telemetry-collector.js';
import { ImporterLiteRuntime } from './lite/runtime.js';
import { reconcileAuditedSources } from './core/source-audit-bootstrap.js';

async function main() {
  const __dirname = path.dirname(fileURLToPath(import.meta.url));
  let buildCommit = 'unknown';
  try {
    buildCommit = fsSync.readFileSync(path.join(__dirname, 'BUILD_ID'), 'utf8').trim();
  } catch (e) { /* ignore */ }

  rootLogger.info(`Starting Project Nox Importer daemon... | Build: ${buildCommit}`);
  let buildMetadata: Record<string, unknown> = { buildId: buildCommit };
  try { buildMetadata = JSON.parse(buildCommit); } catch {}
  try {
    buildMetadata.gitSha = execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8', timeout: 2000, stdio: ['ignore', 'pipe', 'ignore'] }).trim();
  } catch {}
  telemetryCollector.setRuntimeFingerprint({ ...buildMetadata, nodeVersion: process.version, bootedAt: new Date().toISOString() });

  const config = getConfig();

  // Network diagnostics: determine egress IP and TCP port reachability
  try {
    const ipRes = await fetch('https://api.ipify.org?format=json', { signal: AbortSignal.timeout(4000) }).then(r => r.json() as Promise<{ ip: string }>);
    rootLogger.info(`Container Public Egress IP: ${ipRes.ip}`);

    const net = await import('net');
    const testPort = (host: string, port: number) => new Promise<string>((resolve) => {
      const s = new net.Socket();
      s.setTimeout(4000);
      s.on('connect', () => { s.destroy(); resolve('OPEN'); });
      s.on('timeout', () => { s.destroy(); resolve('TIMEOUT'); });
      s.on('error', (e) => { resolve('ERR: ' + e.message); });
      s.connect(port, host);
    });
    const portquizRes = await testPort('portquiz.net', 5433);
    const yugabyteRes = await testPort(config.YUGABYTE_HOST, config.YUGABYTE_PORT);
    rootLogger.info(`Network TCP Probe: portquiz.net:5433 = ${portquizRes} | yugabyte:${config.YUGABYTE_PORT} = ${yugabyteRes}`);
  } catch (e: any) {
    rootLogger.warn(`Failed network diagnostics: ${e.message}`);
  }

  // 1. Initialize Database Adapter based on IMPORTER_DB_MODE
  let supabase: any;
  if (config.IMPORTER_DB_MODE === 'direct') {
    rootLogger.info('IMPORTER DATABASE MODE: DIRECT');
    rootLogger.info('DB PATH: DIRECT YSQL TLS -> YugabyteDB Aeon');
    supabase = new DirectSupabaseClient();
  } else {
    rootLogger.info('IMPORTER DATABASE MODE: GATEWAY');
    rootLogger.info('DB PATH: HTTP -> Cloudflare Worker Gateway -> Hyperdrive -> YugabyteDB Aeon');
    const gateway = new ImporterGatewayClient(config.NOX_IMPORTER_GATEWAY_URL, config.NOX_STORAGE_BRIDGE_TOKEN || '');
    supabase = new GatewaySupabaseClient(gateway) as any;
  }

  // 2. Initialize Storage Provider
  let storage: StorageProvider;
  if (config.STORAGE_PROVIDER === 'direct_telegram' || config.STORAGE_PROVIDER === 'telegram') {
    storage = new DirectTelegramStorageProvider(undefined, config.UPLOAD_RATE_LIMIT_BYTES_PER_SEC);
  } else if (config.STORAGE_PROVIDER === 'worker') {
    if (!config.NOX_STORAGE_BRIDGE_TOKEN) {
      rootLogger.warn('Bridge token not provided, falling back to mock storage for safety');
      storage = new MockStorageProvider();
    } else {
      storage = new NoxWorkerStorageProvider(config.NOX_MANGA_URL, config.NOX_STORAGE_BRIDGE_TOKEN);
    }
  } else {
    storage = new MockStorageProvider();
  }

  // 3. Health & Readiness check
  const health = new HealthMonitor(storage, supabase);
  const telemetry = await health.getCompactTelemetry();
  rootLogger.info(`Initial boot status: ${telemetry}`);

  // 4. Initialize Rate Limiter & Source Registry
  const defaultHostRate = parseFloat(process.env.DEFAULT_HOST_RATE_PER_SECOND || '5.0');
  const rateLimiter = new HostRateLimiter(defaultHostRate);
  const registry = new SourceRegistry(rateLimiter);

  // Keep the narrow, audited source set coherent with the durable source
  // policy. This is idempotent and never resets a future source cooldown.
  if (config.IMPORTER_DB_MODE === 'direct') {
    await reconcileAuditedSources((supabase as DirectSupabaseClient).getPool());
  }

  // 5. Lite is the default in the approved cutover artifact. Both runtimes
  // share a durable leadership lease, so a rolling deploy cannot publish from
  // both schedulers concurrently.
  if (config.IMPORTER_RUNTIME_MODE === 'lite' && config.IMPORTER_DB_MODE !== 'direct') {
    throw new Error('Importer Lite requires IMPORTER_DB_MODE=direct');
  }
  rootLogger.info('IMPORTER RUNTIME MODE', {
    mode: config.IMPORTER_RUNTIME_MODE,
    liteChapterWorkers: config.LITE_CHAPTER_WORKERS,
    directDb: config.IMPORTER_DB_MODE === 'direct',
  });
  // This is one P2-only connection, not an expansion of chapter-worker
  // concurrency. It guarantees that the already single-flight historical
  // metadata producer can make progress during a busy chapter claim wave.
  const sourceMetadataPool = config.IMPORTER_RUNTIME_MODE === 'lite'
    ? createYugabyteMetadataPool() : undefined;
  const engine = config.IMPORTER_RUNTIME_MODE === 'lite'
    ? new ImporterLiteRuntime((supabase as DirectSupabaseClient).getPool(), supabase, registry, storage, config, rateLimiter, sourceMetadataPool)
    : new ImporterEngine(supabase, storage, registry, rateLimiter, config);

  // 6. Graceful Shutdown & Forensics Handlers
  diagnostics.initProcessHandlers(async (signal: string) => {
    rootLogger.info(`Received ${signal}, initiating graceful shutdown...`);
    engine.stop();
    setTimeout(() => {
      diagnostics.dumpForensics(`Forced shutdown after timeout (15s limit reached during ${signal})`);
      process.exit(1);
    }, 15_000).unref();
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
