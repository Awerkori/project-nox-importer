import { MangaWorkAdapter } from '../common/mangawork-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';

export class ApeComicsAdapter extends MangaWorkAdapter {
  constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch) {
    super(
      {
        id: 'apecomics',
        name: 'Capitoons',
        baseUrl: 'https://capitoons.com',
        seriesPath: 'series',
        rateLimitRps: 2.0,
      },
      rateLimiter,
      transport
    );
  }
}
