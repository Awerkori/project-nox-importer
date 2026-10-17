import { HostRateLimiter } from '../../core/rate-limiter.js';
import { SourceAdapter, SourceChapterSummary, SourceWorkDetails, SourceWorkSnapshot, SourceWorkSummary } from '../types.js';
/** Public first-party WordPress API; no browser token, cookie or DOM selector. */
export declare class MangaStopAdapter implements SourceAdapter {
    private readonly rateLimiter;
    private readonly transport;
    readonly id = "mangastop";
    readonly name = "Manga Stop";
    readonly baseUrl = "https://mangastop.net";
    private readonly apiUrl;
    readonly probeUrl: string;
    constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch);
    private request;
    private summary;
    private details;
    private chapters;
    fetchUpdatedWorks(cursor?: string | null): Promise<{
        works: SourceWorkSummary[];
        nextCursor: string | null;
    }>;
    fetchWorkDetails(sourceWorkId: string): Promise<SourceWorkDetails>;
    fetchWorkSnapshot(sourceWorkId: string): Promise<SourceWorkSnapshot>;
    fetchChapters(sourceWorkId: string): Promise<SourceChapterSummary[]>;
    fetchChapterPages(sourceChapterId: string): Promise<string[]>;
    searchWorks(query: string): Promise<SourceWorkSummary[]>;
    getImageHeaders(): Record<string, string>;
}
