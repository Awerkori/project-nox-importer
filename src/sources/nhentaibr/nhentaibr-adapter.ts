import { WpGalleryAdapter } from '../common/wpgallery-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';

export class NHentaiBrAdapter extends WpGalleryAdapter {
  constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch) {
    super(
      {
        id: 'nhentaibr',
        name: 'NHentai.net.br',
        baseUrl: 'https://nhentai.net.br',
        rateLimitRps: 2.0,
      },
      rateLimiter,
      transport
    );
  }
}
