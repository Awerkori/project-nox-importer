import { ZeistMangaAdapter } from '../common/zeistmanga-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';

export class Ler999Adapter extends ZeistMangaAdapter {
  constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch) {
    super(
      {
        id: 'ler999',
        name: 'Ler 999',
        baseUrl: 'https://ler999.blogspot.com',
        seriesCategory: 'Series',
        rateLimitRps: 2.0,
      },
      rateLimiter,
      transport
    );
  }
}
