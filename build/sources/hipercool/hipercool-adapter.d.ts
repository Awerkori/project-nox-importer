import { SourceAdapter, SourceWorkSummary, SourceWorkDetails, SourceChapterSummary } from '../types.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';
export declare class HipercoolAdapter implements SourceAdapter {
    private rateLimiter;
    private transport;
    readonly id = "hipercool";
    readonly name = "HipercooL";
    readonly baseUrl = "https://lerhentais.com";
    private logger;
    private cookies;
    private lastSessionFetch;
    constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch);
    private get headers();
    private storeCookies;
    private ensureSession;
    private fetchJson;
    getImageHeaders(_url: string): Record<string, string>;
    fetchUpdatedWorks(cursor?: string | null, _options?: {
        mode?: 'bootstrap' | 'maintenance';
    }): Promise<{
        works: SourceWorkSummary[];
        nextCursor: string | null;
    }>;
    fetchWorkDetails(sourceWorkId: string): Promise<SourceWorkDetails>;
    fetchChapters(sourceWorkId: string): Promise<SourceChapterSummary[]>;
    fetchChapterPages(sourceChapterId: string, _chapterNumber?: number): Promise<string[]>;
    searchWorks(query: string): Promise<SourceWorkSummary[]>;
}
