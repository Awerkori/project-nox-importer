import { MadaraAdapter } from '../common/madara-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';
export declare class FleurBlancheAdapter extends MadaraAdapter {
    constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch);
}
