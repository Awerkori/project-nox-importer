import { MadaraAdapter } from '../common/madara-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';

export class HotCabaretScanAdapter extends MadaraAdapter {
  constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch) {
    super(
      {
        id: 'hotcabaretscan',
        name: 'Hot Cabaret Scan',
        baseUrl: 'https://hotcabaretscan.com',
        mangaSubString: 'manga',
        rateLimitRps: 2.0,
      },
      rateLimiter,
      transport
    );
  }
}
