import { MadaraAdapter } from '../common/madara-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';

export class TiaManhwaAdapter extends MadaraAdapter {
  constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch) {
    super(
      {
        id: 'tiamanhwa',
        name: 'Tia Manhwa',
        baseUrl: 'https://tiamanhwa.com',
        mangaSubString: 'manhwa',
        rateLimitRps: 2.0,
      },
      rateLimiter,
      transport
    );
  }
}
