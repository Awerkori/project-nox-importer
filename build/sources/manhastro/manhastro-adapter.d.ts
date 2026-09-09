import { SourceAdapter, SourceWorkSummary, SourceWorkDetails, SourceChapterSummary } from '../types.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';
export declare class ManhastroAdapter implements SourceAdapter {
    private rateLimiter;
    private transport;
    readonly id = "manhastro";
    readonly name = "Manhastro";
    readonly baseUrl = "https://manhastro.net";
    private apiUrl;
    private logger;
    constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch);
    private get headers();
    private request;
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
}
