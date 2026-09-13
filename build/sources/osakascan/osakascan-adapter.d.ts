import { ZeistMangaAdapter } from '../common/zeistmanga-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';
export declare class OsakaScanAdapter extends ZeistMangaAdapter {
    constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch);
}
