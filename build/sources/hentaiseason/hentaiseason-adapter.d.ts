import { WpGalleryAdapter } from '../common/wpgallery-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';
export declare class HentaiSeasonAdapter extends WpGalleryAdapter {
    constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch);
}
