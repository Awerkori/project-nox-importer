import { MangaWorkAdapter } from '../common/mangawork-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';
export declare class PizzariaScanAdapter extends MangaWorkAdapter {
    constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch);
}
