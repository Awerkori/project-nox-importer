import { HostRateLimiter } from '../../core/rate-limiter.js';
import { SourceAdapter, SourceChapterSummary, SourceWorkDetails, SourceWorkSnapshot, SourceWorkSummary } from '../types.js';
/**
 * BR Scans exposes a small, version-independent JSON API backed by its own
 * catalogue. It is deliberately modelled on that API rather than the public
 * web UI, so a theme change cannot break discovery or the reader manifest.
 */
export declare class BRScansAdapter implements SourceAdapter {
    private readonly rateLimiter;
    private readonly transport;
    readonly id = "brscans";
    readonly name = "BR Scans";
    readonly baseUrl = "https://brscans.vercel.app";
    private readonly apiUrl;
    constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch);
    private request;
    private workSummary;
    private workDetails;
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
