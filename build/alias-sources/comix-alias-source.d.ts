import { type SourceHttpClientOptions } from './http-client.js';
import type { AliasOnlySourceAdapter, AliasSourceDetailOutcome, AliasSourceHealthSnapshot, AliasSourceSearchOutcome } from './types.js';
export declare function extractComixInitialData(html: string): Record<string, any> | null;
export declare function extractComixHid(urlOrHid: string): string;
/**
 * Comix (`https://comix.to`) ALIAS_ONLY adapter:
 * - Public SSR routes (`/` and `/title/<hid>`) expose unencrypted `<script id="initial-data">`
 *   with `title`, `altTitles`, `authors`, `artists`, `year`, and `links.{al, mal, mu, md, mb}`.
 * - Search API (`/api/v1/manga`) currently enforces a custom VM-obfuscated token + WAF gate
 *   (`HTTP 403 {"code":"missing_token"}`). Per project rules, we NEVER bypass access controls,
 *   WAF, or Cloudflare: the search API client trips its circuit breaker cleanly on `missing_token`
 *   while keeping public SSR discovery and direct `/title/<hid>` resolution operational.
 */
export declare class ComixAliasSource implements AliasOnlySourceAdapter {
    readonly id: "comix";
    readonly name = "Comix";
    readonly baseUrl = "https://comix.to";
    readonly category: "ALIAS_ONLY";
    private readonly ssrHttp;
    private readonly apiHttp;
    private ssrCatalogSeeded;
    private ssrSeedPromise;
    private readonly ssrCandidatesByHid;
    private readonly ssrTitleIndex;
    constructor(httpOptions?: Partial<SourceHttpClientOptions>);
    getHealthSnapshot(): AliasSourceHealthSnapshot;
    private indexSsrCandidate;
    private ensureSsrSeeded;
    searchWorks(query: string, limit?: number): Promise<AliasSourceSearchOutcome>;
    fetchWorkAliases(sourceWorkId: string): Promise<AliasSourceDetailOutcome>;
}
