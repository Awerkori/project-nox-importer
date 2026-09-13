import { SourceAdapter, SourceWorkSummary, SourceWorkDetails, SourceChapterSummary } from '../types.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';
import { Logger } from '../../core/logger.js';
export interface MangaWorkOptions {
    id: string;
    name: string;
    baseUrl: string;
    seriesPath?: string;
    rateLimitRps?: number;
}
export declare class MangaWorkAdapter implements SourceAdapter {
    protected rateLimiter: HostRateLimiter;
    protected transport: typeof fetch;
    readonly id: string;
    readonly name: string;
    readonly baseUrl: string;
    readonly seriesPath: string;
    protected logger: Logger;
    constructor(options: MangaWorkOptions, rateLimiter?: HostRateLimiter, transport?: typeof fetch);
    protected get headers(): Record<string, string>;
    protected fetchHtml(url: string, options?: RequestInit): Promise<string>;
    fetchUpdatedWorks(cursor?: string | null, options?: {
        mode?: 'bootstrap' | 'maintenance';
    }): Promise<{
        works: SourceWorkSummary[];
        nextCursor: string | null;
    }>;
    fetchWorkDetails(sourceWorkId: string): Promise<SourceWorkDetails>;
    fetchChapters(sourceWorkId: string): Promise<SourceChapterSummary[]>;
    fetchChapterPages(sourceChapterId: string, _chapterNumber?: number): Promise<string[]>;
    searchWorks(query: string): Promise<SourceWorkSummary[]>;
    getImageHeaders(_url?: string): Record<string, string>;
}
