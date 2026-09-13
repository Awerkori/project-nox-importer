import { MangaThemesiaAdapter } from '../common/mangathemesia-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';
export declare class PointZeroToonsAdapter extends MangaThemesiaAdapter {
    constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch);
}
