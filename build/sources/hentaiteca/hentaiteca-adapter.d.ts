import { MadaraAdapter } from '../common/madara-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';
/**
 * Hentai Teca is a standard Madara installation. Keep the integration on the
 * shared adapter so the catalog, AJAX manifest and Reader fallbacks stay in
 * sync with the maintained Madara parser.
 */
export declare class HentaiTecaAdapter extends MadaraAdapter {
    constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch);
}
