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
    private storage;
    private supabase?;
    private startTime;
    private logger;
    constructor(storage: StorageProvider, supabase?: any | undefined);
    checkHealth(): Promise<HealthReport>;
    getCompactTelemetry(): Promise<string>;
}
