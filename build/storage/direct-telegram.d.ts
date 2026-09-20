import { StorageProvider } from './provider.js';
export interface BotRuntimeMetrics {
    ref: string;
    token: string;
    username: string;
    activeUploads: number;
    totalUploads: number;
    successes: number;
    failures: number;
    rateLimits429: number;
    floodWaitSeconds: number;
    cooldownUntil: number;
    maxConcurrent: number;
    consecutiveSuccesses: number;
    recentUploadTimestamps: number[];
    latencies: number[];
}
export interface ShardRuntimeMetrics {
    shardId: string;
    channelId: string;
    name: string;
    activeUploads: number;
    totalUploads: number;
    successes: number;
    failures: number;
    rateLimits429: number;
    cooldownUntil: number;
    maxConcurrent: number;
    recentUploadTimestamps: number[];
    latencies: number[];
}
export interface UploadRecordMetadata {
    botRef: string;
    shardId: string;
    channelId: string;
    fileId: string;
    timestamp: number;
    byteSize: number;
    latencyMs: number;
}
export declare class BandwidthLimiter {
    private bytesPerSec;
    private maxBurst;
    private tokens;
    private lastRefill;
    private waitChain;
    constructor(bytesPerSec?: number, maxBurst?: number);
    acquire(bytes: number): Promise<void>;
    setRate(bytesPerSec: number): void;
}
export declare class DirectTelegramStorageProvider implements StorageProvider {
    private logger;
    private httpsAgent;
    private bandwidthLimiter;
    private bots;
    private shards;
    private metadataMap;
    private lastBotRef;
    private lastShardId;
    private lastChannelId;
    private botRoundRobinIndex;
    private shardRoundRobinIndex;
    private maxGlobalConcurrent;
    private currentGlobalActive;
    private waitingQueue;
    constructor(checkpointPath?: string, rateLimitBytesPerSec?: number);
    private loadConfiguration;
    setUploadRate(bytesPerSec: number): void;
    getProviderKey(): string;
    getLastBotReference(id?: string): string;
    getLastShardId(id?: string): string | null;
    getLastChannelId(id?: string): string | null;
    healthCheck(): Promise<boolean>;
    private cleanRecentWindows;
    private acquireGlobalSlot;
    private releaseGlobalSlot;
    private selectOptimalBot;
    private selectOptimalShard;
    private executeTelegramUpload;
    upload(bytes: Uint8Array, mime: string, id: string, chapterId?: string): Promise<string>;
    getMetricsSummary(): {
        totalBotUploads: number;
        totalShardUploads: number;
        bots: {
            bot: string;
            username: string;
            uploads: number;
            pct: number;
            successes: number;
            failures: number;
            rateLimits429: number;
            floodWaitSeconds: number;
            active: number;
            cooldownUntil: number;
            p50LatencyMs: number;
            p95LatencyMs: number;
        }[];
        shards: {
            shardNum: number;
            shardId: string;
            channelId: string;
            name: string;
            uploads: number;
            pct: number;
            successes: number;
            failures: number;
            rateLimits429: number;
            active: number;
            cooldownUntil: number;
            p50LatencyMs: number;
        }[];
    };
}
