import { MadaraAdapter } from '../common/madara-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';

export class FleurBlancheAdapter extends MadaraAdapter {
  constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch) {
    super(
      {
        id: 'fleurblanche',
        name: 'Fleur Blanche',
        baseUrl: 'https://fbsquadx.com',
        mangaSubString: 'manga',
      },
      rateLimiter,
      transport
    );
  }
}
