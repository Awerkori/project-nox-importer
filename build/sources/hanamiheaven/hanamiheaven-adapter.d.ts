import { SourceAdapter, SourceWorkSummary, SourceWorkDetails, SourceChapterSummary } from '../types.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';
export declare class HanamiHeavenAdapter implements SourceAdapter {
    private rateLimiter;
    private transport;
    readonly id = "hanamiheaven";
    readonly name = "Hanami Heaven";
    readonly baseUrl = "https://hanamiheaven.org";
    private logger;
    private cookies;
    constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch);
    private get headers();
    private storeCookies;
    private solveJsChallenge;
    private fetchHtml;
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
