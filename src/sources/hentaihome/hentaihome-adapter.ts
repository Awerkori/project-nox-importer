import { WpGalleryAdapter } from '../common/wpgallery-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';

export class HentaiHomeAdapter extends WpGalleryAdapter {
  constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch) {
    super(
      {
        id: 'hentaihome',
        name: 'Hentai Home',
        baseUrl: 'https://www.hentaihome.net',
        rateLimitRps: 2.0,
      },
      rateLimiter,
      transport
    );
  }
}
