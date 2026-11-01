import { SourceAdapter, SourceWorkSummary, SourceWorkDetails, SourceChapterSummary, SourceWorkSnapshot } from '../types.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';
import { Logger } from '../../core/logger.js';
export interface WpGalleryOptions {
    id: string;
    name: string;
    baseUrl: string;
    rateLimitRps?: number;
}
/**
 * WordPress exposes post categories/tags through the embedded `wp:term`
 * relation.  These are provider-declared taxonomy values, not terms guessed
 * from a title or synopsis.  Some themes surface the same explicit taxonomy
 * through Yoast's articleSection; merge both defensively.
 */
export declare function extractWpGenres(post: any): string[];
/**
 * Only consume fields whose provider schema explicitly labels them as an
 * alternative/original/native title.  Keeping this narrow avoids turning
 * navigation, prose, or machine translations into false aliases.
 */
export declare function extractWpAlternativeTitles(post: any): string[];
export declare class WpGalleryAdapter implements SourceAdapter {
    protected rateLimiter: HostRateLimiter;
    protected transport: typeof fetch;
    readonly id: string;
    readonly name: string;
    readonly baseUrl: string;
    protected logger: Logger;
    constructor(options: WpGalleryOptions, rateLimiter?: HostRateLimiter, transport?: typeof fetch);
    protected get headers(): Record<string, string>;
    protected fetchJson<T = any>(url: string): Promise<T>;
    protected fetchHtml(url: string): Promise<string>;
    fetchUpdatedWorks(cursor?: string | null, options?: {
        mode?: 'bootstrap' | 'maintenance';
    }): Promise<{
        works: SourceWorkSummary[];
        nextCursor: string | null;
    }>;
    fetchWorkSnapshot(sourceWorkId: string, _signal?: AbortSignal): Promise<SourceWorkSnapshot>;
    fetchWorkDetails(sourceWorkId: string): Promise<SourceWorkDetails>;
    fetchChapters(sourceWorkId: string): Promise<SourceChapterSummary[]>;
    fetchChapterPages(sourceChapterId: string): Promise<string[]>;
    searchWorks(query: string): Promise<SourceWorkSummary[]>;
    getImageHeaders(url?: string): Record<string, string>;
}
