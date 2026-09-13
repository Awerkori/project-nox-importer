import { MadaraAdapter } from '../common/madara-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';

export class YaoiFanClubAdapter extends MadaraAdapter {
  constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch) {
    super(
      {
        id: 'yaoifanclub',
        name: 'Yaoi Fan Club',
        baseUrl: 'https://yaoifanclub.com',
        mangaSubString: 'obra',
        rateLimitRps: 2.0,
      },
      rateLimiter,
      transport
    );
  }
}
