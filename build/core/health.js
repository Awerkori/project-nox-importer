import { db } from '../db/index.js';
import * as schema from '../db/schema.js';
import { safeQuery } from '../db/safe.js';
import { Logger } from './logger.js';
export class HealthMonitor {
    storage;
    supabase;
    startTime = Date.now();
    logger = new Logger('HealthMonitor');
    constructor(storage, supabase) {
        this.storage = storage;
        this.supabase = supabase;
    }
    async checkHealth() {
        const mem = process.memoryUsage();
        const memoryUsageMb = {
            rss: Math.round(mem.rss / 1024 / 1024),
            heapUsed: Math.round(mem.heapUsed / 1024 / 1024),
            heapTotal: Math.round(mem.heapTotal / 1024 / 1024),
        };
        let storageHealthy = false;
        try {
            storageHealthy = await this.storage.healthCheck();
        }
        catch {
            storageHealthy = false;
        }
        let dbConnected = false;
        let dbError;
        const queueCounts = { queued: 0, importing: 0, failed: 0, retry: 0 };
        if (this.supabase) {
            try {
                const { data, error } = await this.supabase.from('importer_queue').select('status').limit(100);
                if (error) {
                    dbConnected = false;
                    dbError = error.message;
                }
                else {
                    dbConnected = true;
                    const rows = Array.isArray(data) ? data : [];
                    for (const row of rows) {
                        const s = (row.status || '').toLowerCase();
                        if (s === 'queued')
                            queueCounts.queued++;
                        else if (s === 'importing')
                            queueCounts.importing++;
                        else if (s === 'failed')
                            queueCounts.failed++;
                        else if (s === 'retry')
                            queueCounts.retry++;
                    }
                }
            }
            catch (err) {
                dbConnected = false;
                dbError = err?.message || 'Database error';
            }
        }
        else {
            try {
                const { data, error } = await safeQuery(db.select({ status: schema.importerQueue.status }).from(schema.importerQueue));
                if (error) {
                    dbError = error.message;
                }
                else {
                    dbConnected = true;
                    const rows = Array.isArray(data) ? data : [];
                    for (const row of rows) {
                        const s = (row.status || '').toLowerCase();
                        if (s === 'queued')
                            queueCounts.queued++;
                        else if (s === 'importing')
                            queueCounts.importing++;
                        else if (s === 'failed')
                            queueCounts.failed++;
                        else if (s === 'retry')
                            queueCounts.retry++;
                    }
                }
            }
            catch (err) {
                dbError = err?.message;
            }
        }
        let overallStatus = 'HEALTHY';
        if (!dbConnected) {
            overallStatus = 'UNHEALTHY';
        }
        else if (!storageHealthy || queueCounts.failed > 50) {
            overallStatus = 'DEGRADED';
        }
        const report = {
            status: overallStatus,
            uptimeSeconds: Math.floor((Date.now() - this.startTime) / 1000),
            memoryUsageMb,
            storage: {
                provider: this.storage.getProviderKey(),
                healthy: storageHealthy,
            },
            database: {
                connected: dbConnected,
                error: dbError,
            },
            queue: queueCounts,
            timestamp: new Date().toISOString(),
        };
        if (overallStatus !== 'HEALTHY') {
            this.logger.warn('Health monitor status degraded', report);
        }
        return report;
    }
    async getCompactTelemetry() {
        const report = await this.checkHealth();
        const mem = `${report.memoryUsageMb.heapUsed}MB heap / ${report.memoryUsageMb.rss}MB rss`;
        const q = `Q:[${report.queue.queued} queued, ${report.queue.importing} running, ${report.queue.retry} retry, ${report.queue.failed} fail]`;
        const upHours = Math.floor(report.uptimeSeconds / 3600);
        const upMins = Math.floor((report.uptimeSeconds % 3600) / 60);
        const uptime = `${upHours}h ${upMins}m`;
        return `[Health ${report.status}] Mem: ${mem} (512MB RAM) | ${q} | Storage: ${report.storage.provider} (${report.storage.healthy ? 'OK' : 'ERR'}) | Uptime: ${uptime}`;
    }
}
