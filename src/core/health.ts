import type { SupabaseClient } from '@supabase/supabase-js';
import { StorageProvider } from '../storage/provider.js';
import { Logger } from './logger.js';

export interface HealthReport {
  status: 'HEALTHY' | 'DEGRADED' | 'UNHEALTHY';
  uptimeSeconds: number;
  memoryUsageMb: {
    rss: number;
    heapUsed: number;
    heapTotal: number;
  };
  storage: {
    provider: string;
    healthy: boolean;
  };
  database: {
    connected: boolean;
    error?: string;
  };
  queue: {
    queued: number;
    importing: number;
    failed: number;
    retry: number;
  };
  timestamp: string;
}

export class HealthMonitor {
  private startTime = Date.now();
  private logger = new Logger('HealthMonitor');

  constructor(private supabase: SupabaseClient, private storage: StorageProvider) {}

  async checkHealth(): Promise<HealthReport> {
    const mem = process.memoryUsage();
    const memoryUsageMb = {
      rss: Math.round(mem.rss / 1024 / 1024),
      heapUsed: Math.round(mem.heapUsed / 1024 / 1024),
      heapTotal: Math.round(mem.heapTotal / 1024 / 1024),
    };

    // 1. Storage check
    let storageHealthy = false;
    try {
      storageHealthy = await this.storage.healthCheck();
    } catch {
      storageHealthy = false;
    }

    // 2. Database & Queue metrics check
    let dbConnected = false;
    let dbError: string | undefined;
    const queueCounts = { queued: 0, importing: 0, failed: 0, retry: 0 };

    try {
      const { data, error } = await this.supabase
        .from('importer_queue')
        .select('status');

      if (error) {
        dbError = error.message;
      } else {
        dbConnected = true;
        for (const row of data || []) {
          const s = (row.status || '').toLowerCase();
          if (s === 'queued') queueCounts.queued++;
          else if (s === 'importing') queueCounts.importing++;
          else if (s === 'failed') queueCounts.failed++;
          else if (s === 'retry') queueCounts.retry++;
        }
      }
    } catch (err: any) {
      dbError = err?.message;
    }

    let overallStatus: HealthReport['status'] = 'HEALTHY';
    if (!dbConnected) {
      overallStatus = 'UNHEALTHY';
    } else if (!storageHealthy || queueCounts.failed > 50) {
      overallStatus = 'DEGRADED';
    }

    const report: HealthReport = {
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
}
