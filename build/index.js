import fsSync from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import WebSocket from 'ws';
import { ImporterGatewayClient } from './core/gateway-client.js';
import { GatewaySupabaseClient } from './core/gateway-supabase.js';
import { DirectSupabaseClient } from './db/direct-supabase-client.js';
import { getConfig } from './config.js';
// Polyfill native WebSocket for Node environments (e.g. Node 20 on DIScloud) where native WebSocket is missing
if (typeof globalThis.WebSocket === 'undefined') {
    globalThis.WebSocket = WebSocket;
}
import { rootLogger } from './core/logger.js';
import { HostRateLimiter } from './core/rate-limiter.js';
import { SourceRegistry } from './sources/registry.js';
import { DirectTelegramStorageProvider } from './storage/direct-telegram.js';
import { NoxWorkerStorageProvider } from './storage/worker.js';
import { MockStorageProvider } from './storage/mock.js';
import { ImporterEngine } from './core/engine.js';
import { HealthMonitor } from './core/health.js';
import { diagnostics } from './core/diagnostics.js';
async function main() {
    const __dirname = path.dirname(fileURLToPath(import.meta.url));
    let buildCommit = 'unknown';
    try {
        buildCommit = fsSync.readFileSync(path.join(__dirname, 'BUILD_ID'), 'utf8').trim();
    }
    catch (e) { /* ignore */ }
    rootLogger.info(`Starting Project Nox Importer daemon... | Build: ${buildCommit}`);
    const config = getConfig();
    // Network diagnostics: determine egress IP and TCP port reachability
    try {
        const ipRes = await fetch('https://api.ipify.org?format=json', { signal: AbortSignal.timeout(4000) }).then(r => r.json());
        rootLogger.info(`Container Public Egress IP: ${ipRes.ip}`);
        const net = await import('net');
        const testPort = (host, port) => new Promise((resolve) => {
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
    }
    catch (e) {
        rootLogger.warn(`Failed network diagnostics: ${e.message}`);
    }
    // 1. Initialize Database Adapter based on IMPORTER_DB_MODE
    let supabase;
    if (config.IMPORTER_DB_MODE === 'direct') {
        rootLogger.info('IMPORTER DATABASE MODE: DIRECT');
        rootLogger.info('DB PATH: DIRECT YSQL TLS -> YugabyteDB Aeon');
        supabase = new DirectSupabaseClient();
    }
    else {
        rootLogger.info('IMPORTER DATABASE MODE: GATEWAY');
        rootLogger.info('DB PATH: HTTP -> Cloudflare Worker Gateway -> Hyperdrive -> YugabyteDB Aeon');
        const gateway = new ImporterGatewayClient(config.NOX_IMPORTER_GATEWAY_URL, config.NOX_STORAGE_BRIDGE_TOKEN || '');
        supabase = new GatewaySupabaseClient(gateway);
    }
    // 2. Initialize Storage Provider
    let storage;
    if (config.STORAGE_PROVIDER === 'direct_telegram' || config.STORAGE_PROVIDER === 'telegram') {
        storage = new DirectTelegramStorageProvider();
    }
    else if (config.STORAGE_PROVIDER === 'worker') {
        if (!config.NOX_STORAGE_BRIDGE_TOKEN) {
            rootLogger.warn('Bridge token not provided, falling back to mock storage for safety');
            storage = new MockStorageProvider();
        }
        else {
            storage = new NoxWorkerStorageProvider(config.NOX_MANGA_URL, config.NOX_STORAGE_BRIDGE_TOKEN);
        }
    }
    else {
        storage = new MockStorageProvider();
    }
    // 3. Health & Readiness check
    const health = new HealthMonitor(storage, supabase);
    const telemetry = await health.getCompactTelemetry();
    rootLogger.info(`Initial boot status: ${telemetry}`);
    // 4. Initialize Rate Limiter & Source Registry
    const rateLimiter = new HostRateLimiter(2.0);
    const registry = new SourceRegistry(rateLimiter, config.NOX_STORAGE_BRIDGE_TOKEN, config.NOX_MANGA_URL);
    // 5. Initialize Importer Engine
    const engine = new ImporterEngine(supabase, storage, registry, rateLimiter, config);
    // 6. Graceful Shutdown & Forensics Handlers
    diagnostics.initProcessHandlers(async (signal) => {
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
