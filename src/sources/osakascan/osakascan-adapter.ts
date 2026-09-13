import { ZeistMangaAdapter } from '../common/zeistmanga-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';

export class OsakaScanAdapter extends ZeistMangaAdapter {
  constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch) {
    super(
      {
        id: 'osakascan',
        name: 'Osaka Scan',
        baseUrl: 'https://www.osakascan.com',
        seriesCategory: 'Series',
        rateLimitRps: 2.0,
      },
      rateLimiter,
      transport
    );
  }
}
