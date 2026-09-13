import { WpGalleryAdapter } from '../common/wpgallery-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';

export class HentaiTokyoAdapter extends WpGalleryAdapter {
  constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch) {
    super(
      {
        id: 'hentaitokyo',
        name: 'Hentai Tokyo',
        baseUrl: 'https://hentaitokyo.net',
        rateLimitRps: 2.0,
      },
      rateLimiter,
      transport
    );
  }
}
