import { ZeistMangaAdapter } from '../common/zeistmanga-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';

export class GalaxScanlatorAdapter extends ZeistMangaAdapter {
  constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch) {
    super(
      {
        id: 'galaxscanlator',
        name: 'GALAX Scans',
        baseUrl: 'https://galaxscanlator.blogspot.com',
        seriesCategory: 'Series',
        rateLimitRps: 2.0,
      },
      rateLimiter,
      transport
    );
  }
}
