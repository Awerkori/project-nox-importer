import { SourceAdapter, SourceWorkSummary, SourceWorkDetails, SourceChapterSummary, SourceWorkSnapshot } from '../types.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';
import { Logger } from '../../core/logger.js';
export interface MadaraOptions {
    id: string;
    name: string;
    baseUrl: string;
    mangaSubString?: string;
    rateLimitRps?: number;
}
export declare class MadaraAdapter implements SourceAdapter {
    protected rateLimiter: HostRateLimiter;
    protected transport: typeof fetch;
    readonly id: string;
    readonly name: string;
    readonly baseUrl: string;
    readonly mangaSubString: string;
    protected logger: Logger;
    constructor(options: MadaraOptions, rateLimiter?: HostRateLimiter, transport?: typeof fetch);
    protected get headers(): Record<string, string>;
    protected fetchHtml(url: string, options?: RequestInit): Promise<string>;
    /**
     * Madara chapter URLs are scoped by the work slug:
     *   /manga/<source-work-id>/capitulo-<n>/
     *
     * Some themes render recommendation/sidebar links beside an otherwise
     * incomplete chapter list.  Treating those links as chapters of the page
     * being parsed corrupts the canonical work.  This intentionally rejects
     * an unfamiliar URL shape rather than guessing its ownership.
     */
    isChapterOwnedByWork(sourceWorkId: string, sourceChapterId: string): boolean;
    fetchUpdatedWorks(cursor?: string | null, options?: {
        mode?: 'bootstrap' | 'maintenance';
    }): Promise<{
        works: SourceWorkSummary[];
        nextCursor: string | null;
    }>;
    private parseWorkDetails;
    private extractChaptersFromHtml;
    fetchWorkDetails(sourceWorkId: string): Promise<SourceWorkDetails>;
    /**
     * Madara puts the work metadata and the initial chapter list in the same
     * HTML document. A sync previously fetched that document once per value,
     * serializing two identical source reads through the catalog lane.
     */
    fetchWorkSnapshot(sourceWorkId: string): Promise<SourceWorkSnapshot>;
    fetchChapters(sourceWorkId: string): Promise<SourceChapterSummary[]>;
    fetchChapterPages(sourceChapterId: string, _chapterNumber?: number): Promise<string[]>;
    searchWorks(query: string): Promise<SourceWorkSummary[]>;
    getImageHeaders(url: string): Record<string, string>;
}
