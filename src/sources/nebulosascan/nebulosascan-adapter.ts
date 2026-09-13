import { MadaraAdapter } from '../common/madara-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';

export class NebulosaScanAdapter extends MadaraAdapter {
  constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch) {
    super(
      {
        id: 'nebulosascan',
        name: 'Nebulosa Scan',
        baseUrl: 'https://nebulosascan.com',
        mangaSubString: 'manga',
      },
      rateLimiter,
      transport
    );
  }
}
