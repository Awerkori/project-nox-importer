import { MadaraAdapter } from '../common/madara-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';

export class MonteTaiAdapter extends MadaraAdapter {
  constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch) {
    super(
      {
        id: 'montetai',
        name: 'Monte Tai',
        baseUrl: 'https://montetaiscanlator.xyz',
        mangaSubString: 'manga',
      },
      rateLimiter,
      transport
    );
  }
}
