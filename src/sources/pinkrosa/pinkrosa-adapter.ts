import { ZeistMangaAdapter } from '../common/zeistmanga-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';

export class PinkRosaAdapter extends ZeistMangaAdapter {
  constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch) {
    super(
      {
        id: 'pinkrosa',
        name: 'Pink Rosa',
        baseUrl: 'https://scanpinkrosa.blogspot.com',
        seriesCategory: 'Series',
        rateLimitRps: 2.0,
      },
      rateLimiter,
      transport
    );
  }
}
