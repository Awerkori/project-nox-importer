import { MadaraAdapter } from '../common/madara-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';

export class NinjaScanAdapter extends MadaraAdapter {
  constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch) {
    super(
      {
        id: 'ninjascan',
        name: 'Ninja Scan',
        baseUrl: 'https://ninjacomics.xyz',
        mangaSubString: 'manga',
        rateLimitRps: 2.0,
      },
      rateLimiter,
      transport
    );
  }
}
