import { SourceAdapter } from './types.js';
import { HostRateLimiter } from '../core/rate-limiter.js';
export declare class SourceRegistry {
    private adapters;
    constructor(rateLimiter?: HostRateLimiter, bridgeToken?: string | null, mangaUrl?: string | null);
    register(adapter: SourceAdapter): void;
    clear(): void;
    get(id: string): SourceAdapter | undefined;
    getAll(): SourceAdapter[];
}
