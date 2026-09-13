import { MadaraAdapter } from '../common/madara-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';

export class CovenScanAdapter extends MadaraAdapter {
  constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch) {
    super(
      {
        id: 'covenscan',
        name: 'Coven Scan',
        baseUrl: 'https://covendasbruxonas.com',
        mangaSubString: 'manga',
        rateLimitRps: 2.0,
      },
      rateLimiter,
      transport
    );
  }
}
