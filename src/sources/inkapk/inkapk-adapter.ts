import { MadaraAdapter } from '../common/madara-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';

export class InkapkAdapter extends MadaraAdapter {
  constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch) {
    super(
      {
        id: 'inkapk',
        name: 'Inkapk',
        baseUrl: 'https://inkapk.net',
        mangaSubString: 'obras',
        rateLimitRps: 2.0,
      },
      rateLimiter,
      transport
    );
  }
}
