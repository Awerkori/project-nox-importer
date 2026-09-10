export interface SourceWorkSummary {
    sourceWorkId: string;
    title: string;
    slug: string;
    coverUrl?: string | null;
    updatedAt?: string;
}
export interface SourceWorkDetails {
    sourceWorkId: string;
    title: string;
    slug: string;
    coverUrl?: string | null;
    synopsis?: string;
    author?: string;
    artist?: string;
    kind?: 'MANGA' | 'MANHWA' | 'MANHUA' | 'WEBTOON';
    status?: 'ONGOING' | 'COMPLETED' | 'HIATUS' | 'CANCELLED';
    year?: number;
    ageRating?: number;
    genres?: string[];
    alternativeTitles?: string[];
    raw?: Record<string, any>;
}
export interface SourceChapterSummary {
    sourceChapterId: string;
    number: number;
    title?: string;
    createdAt?: string;
    pageCount?: number | null;
}
export interface SourceAdapter {
    readonly id: string;
    readonly name: string;
    readonly baseUrl: string;
    /**
     * Discover recently updated works from this source
     */
    fetchUpdatedWorks(cursor?: string | null, options?: {
        mode?: 'bootstrap' | 'maintenance';
    }): Promise<{
        works: SourceWorkSummary[];
        nextCursor: string | null;
    }>;
    /**
     * Fetch full work metadata
     */
    fetchWorkDetails(sourceWorkId: string): Promise<SourceWorkDetails>;
    /**
     * Fetch chapters list for a work
     */
    fetchChapters(sourceWorkId: string): Promise<SourceChapterSummary[]>;
    /**
     * Fetch image page URLs for a specific chapter
     */
    fetchChapterPages(sourceChapterId: string, chapterNumber?: number): Promise<string[]>;
    /**
     * Search for works matching a query string (title, slug, or alias)
     */
    searchWorks(query: string): Promise<SourceWorkSummary[]>;
    /**
     * Optional custom headers for downloading chapter images (e.g. cookies, referer)
     */
    getImageHeaders?(url: string): Promise<Record<string, string>> | Record<string, string>;
}
