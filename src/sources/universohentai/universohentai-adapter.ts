import { WpGalleryAdapter } from '../common/wpgallery-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';

export class UniversoHentaiAdapter extends WpGalleryAdapter {
  constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch) {
    super(
      {
        id: 'universohentai',
        name: 'Universo Hentai',
        baseUrl: 'https://universohentai.com',
        rateLimitRps: 2.0,
      },
      rateLimiter,
      transport
    );
  }
}
