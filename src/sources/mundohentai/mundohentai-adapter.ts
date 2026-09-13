import { WpGalleryAdapter } from '../common/wpgallery-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';

export class MundoHentaiAdapter extends WpGalleryAdapter {
  constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch) {
    super(
      {
        id: 'mundohentai',
        name: 'Mundo Hentai',
        baseUrl: 'https://mundohentaioficial.com',
        rateLimitRps: 2.0,
      },
      rateLimiter,
      transport
    );
  }
}
