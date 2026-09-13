import { GreenShitAdapter } from '../common/greenshit-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';
export declare class VegitoonsAdapter extends GreenShitAdapter {
    constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch);
}
