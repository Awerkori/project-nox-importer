import { SourceAdapter, SourceWorkSummary, SourceWorkDetails, SourceChapterSummary } from '../types.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';
export declare class KuroAdapter implements SourceAdapter {
    private rateLimiter;
    private transport;
    readonly id = "kuro";
    readonly name = "Kuro Mangas";
    readonly baseUrl = "https://kuromangas.com";
    private apiUrl;
    private cdnUrl;
    private logger;
    private encKey;
    private bridgeUrl;
    private bridgeToken;
    private sessionCookie;
    private clientToken;
    private cfClearance;
    private loginPromise;
    private lastLoginAttempt;
    constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch);
    hasValidSession(): boolean;
    clearSession(): void;
    login(force?: boolean): Promise<boolean>;
    private getAuthHeaders;
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
    getImageHeaders(_url: string): Record<string, string>;
}
