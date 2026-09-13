import { WpGalleryAdapter } from '../common/wpgallery-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';

export class BrasilHentaiAdapter extends WpGalleryAdapter {
  constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch) {
    super(
      {
        id: 'brasilhentai',
        name: 'Brasil Hentai',
        baseUrl: 'https://brasilhentai.com',
        rateLimitRps: 2.0,
      },
      rateLimiter,
      transport
    );
  }
}
