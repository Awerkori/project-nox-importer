import type { SupabaseClient } from '@supabase/supabase-js';
import { StorageProvider } from '../storage/provider.js';
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
export declare class HealthMonitor {
    private supabase;
    private storage;
    private startTime;
    private logger;
    constructor(supabase: SupabaseClient, storage: StorageProvider);
    checkHealth(): Promise<HealthReport>;
    getCompactTelemetry(): Promise<string>;
}
