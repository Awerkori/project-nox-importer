import { WpGalleryAdapter } from '../common/wpgallery-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';

export class HentaiSeasonAdapter extends WpGalleryAdapter {
  constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch) {
    super(
      {
        id: 'hentaiseason',
        name: 'Hentai Season',
        baseUrl: 'https://hentaiseason.com',
        rateLimitRps: 2.0,
      },
      rateLimiter,
      transport
    );
  }
}
