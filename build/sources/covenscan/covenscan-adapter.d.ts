import { MadaraAdapter } from '../common/madara-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';
export declare class CovenScanAdapter extends MadaraAdapter {
    constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch);
}
