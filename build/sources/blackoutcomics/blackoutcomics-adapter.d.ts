import { SourceAdapter, SourceWorkSummary, SourceWorkDetails, SourceChapterSummary } from '../types.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';
export declare class BlackoutComicsAdapter implements SourceAdapter {
    private rateLimiter;
    private transport;
    readonly id = "blackoutcomics";
    readonly name = "Blackout Comics";
    readonly baseUrl = "https://blackoutcomics.com";
    private logger;
    private loginPromise;
    private sessionCookies;
    private lastLoginAttempt;
    constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch);
    private get baseHeaders();
    private getCookieHeader;
    private storeCookiesFromResponse;
    /**
     * Single-flight synchronized authentication mutex
     */
    ensureAuthenticated(forceReauth?: boolean): Promise<boolean>;
    private fetchHtml;
    getImageHeaders(_url: string): Record<string, string>;
    fetchUpdatedWorks(_cursor?: string | null, _options?: {
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
