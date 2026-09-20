import { SourceAdapter, SourceWorkSummary, SourceWorkDetails, SourceChapterSummary } from '../types.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';
export declare class GeassComicsAdapter implements SourceAdapter {
    private rateLimiter;
    private transport;
    readonly id = "geasscomics";
    readonly name = "Geass Comics";
    readonly baseUrl = "https://geasscomics.xyz";
    private readonly apiUrl;
    private logger;
    constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch);
    private get headers();
    private fetchJson;
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
    getImageHeaders(_url: string): Record<string, string>;
}
