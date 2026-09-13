import { ZeistMangaAdapter } from '../common/zeistmanga-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';

export class ApenasUmaFaAdapter extends ZeistMangaAdapter {
  constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch) {
    super(
      {
        id: 'apenasumafa',
        name: 'Apenas Uma Fã',
        baseUrl: 'https://apenasuma-fa.blogspot.com',
        seriesCategory: 'Series',
        rateLimitRps: 2.0,
      },
      rateLimiter,
      transport
    );
  }
}
