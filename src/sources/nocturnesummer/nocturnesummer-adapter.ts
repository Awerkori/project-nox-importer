import { MadaraAdapter } from '../common/madara-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';

export class NocturneSummerAdapter extends MadaraAdapter {
  constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch) {
    super(
      {
        id: 'nocturnesummer',
        name: 'Nocturne Summer',
        baseUrl: 'https://nocfsb.com',
        mangaSubString: 'manga',
      },
      rateLimiter,
      transport
    );
  }
}
