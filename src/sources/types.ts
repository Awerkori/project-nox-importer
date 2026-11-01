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
  kind?: 'MANGA' | 'MANHWA' | 'MANHUA' | 'WEBTOON' | 'PORNHWA' | 'UNKNOWN';
  status?: 'ONGOING' | 'COMPLETED' | 'HIATUS' | 'CANCELLED' | 'UNKNOWN';
  year?: number;
  ageRating?: number;
  /** BCP-47 when the adapter can prove it; Lite keeps editions separate. */
  language?: string;
  edition?: string;
  genres?: string[];
  alternativeTitles?: string[];
  raw?: Record<string, any>;
}

export interface SourceChapterSummary {
  sourceChapterId: string;
  /** Provider text preserved for Lite identity; `number` remains legacy display/sort compatibility. */
  displayNumber?: string;
  number: number;
  title?: string;
  createdAt?: string;
  pageCount?: number | null;
}

/**
 * A source may expose the work metadata and its chapter list in one response.
 * Adapters opt in only when both values came from the same authoritative
 * payload; callers otherwise keep using the independent methods below.
 */
export interface SourceWorkSnapshot {
  details: SourceWorkDetails;
  chapters: SourceChapterSummary[];
}

export interface SourceAdapter {
  readonly id: string;
  readonly name: string;
  readonly baseUrl: string;

  /**
   * Optional: override URL used by the admission probe.
   * If set, the probe hits this URL instead of baseUrl.
   * Use for sources that work through an internal bridge.
   */
  readonly probeUrl?: string;

  /**
   * Optional admission request for adapters whose public catalog/API has a
   * different origin or requires the same public headers used by the normal
   * adapter requests.  This is deliberately limited to an ordinary public
   * HTTP request.  An adapter with an explicitly authorized authenticated API
   * may supply `request` so stage one exercises that same API path; it never
   * grants the Gate a browser challenge bypass or a raw credential.
   */
  readonly admissionProbe?: {
    url: string;
    headers?: Record<string, string>;
    expectedType?: 'html' | 'json';
    /**
     * Optional adapter-owned request for an authenticated catalog/API.  The
     * adapter keeps credentials/session handling private and returns only the
     * upstream response to classify.  All five functional stages still run.
     */
    request?: () => Promise<Response>;
    /** Structural marker the adapter itself needs from a usable HTML catalog. */
    usableHtmlMarker?: RegExp;
    /**
     * Some public WordPress catalogs receive Cloudflare's passive JS-detection
     * snippet together with their complete usable HTML.  Permit stage one to
     * continue only when that full document is present; all functional stages
     * remain mandatory.
     */
    allowInlineJsDetection?: boolean;
  };

  /**
   * Discover recently updated works from this source
   */
  fetchUpdatedWorks(
    cursor?: string | null,
    options?: { mode?: 'bootstrap' | 'maintenance' }
  ): Promise<{
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
   * Optional combined work read. This avoids issuing two identical upstream
   * requests when a provider's work-detail response already contains chapters.
   */
  fetchWorkSnapshot?(sourceWorkId: string, signal?: AbortSignal): Promise<SourceWorkSnapshot>;

  /**
   * Fetch image page URLs for a specific chapter
   */
  fetchChapterPages(sourceChapterId: string, chapterNumber?: number): Promise<string[]>;

  /**
   * Optional, fail-closed ownership proof for adapters whose chapter URL
   * embeds the work slug.  The importer invokes this again at execution
   * time, so an old/corrupt queue payload cannot cross-publish content into
   * another canonical work.
   */
  isChapterOwnedByWork?(sourceWorkId: string, sourceChapterId: string): boolean;

  /**
   * Search for works matching a query string (title, slug, or alias)
   */
  searchWorks(query: string): Promise<SourceWorkSummary[]>;

  /**
   * Optional custom headers for downloading chapter images (e.g. cookies, referer)
   */
  getImageHeaders?(url: string): Promise<Record<string, string>> | Record<string, string>;
}
