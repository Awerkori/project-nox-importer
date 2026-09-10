import { SourceAdapter, SourceWorkSummary, SourceWorkDetails, SourceChapterSummary } from '../types.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';
export declare class NexusToonsAdapter implements SourceAdapter {
    private rateLimiter;
    private transport;
    readonly id = "nexus_toons";
    readonly name = "Nexus Toons";
    readonly baseUrl = "https://nx-toons.xyz";
    private directApiUrl;
    private fallbackApiUrl;
    private logger;
    private bridgeUrl;
    private bridgeToken;
    private directBlocked;
    constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch);
    private get headers();
    private requestViaBridge;
    private request;
    fetchUpdatedWorks(cursor?: string | null, options?: {
        mode?: 'bootstrap' | 'maintenance';
    }): Promise<{
        works: SourceWorkSummary[];
        nextCursor: string | null;
    }>;
    fetchWorkDetails(sourceWorkId: string): Promise<SourceWorkDetails>;
    fetchChapters(sourceWorkId: string): Promise<SourceChapterSummary[]>;
    fetchChapterPages(sourceChapterId: string): Promise<string[]>;
    searchWorks(query: string): Promise<SourceWorkSummary[]>;
}
