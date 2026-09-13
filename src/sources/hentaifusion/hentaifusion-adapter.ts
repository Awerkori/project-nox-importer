import { WpGalleryAdapter } from '../common/wpgallery-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';

export class HentaiFusionAdapter extends WpGalleryAdapter {
  constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch) {
    super(
      {
        id: 'hentaifusion',
        name: 'Hentai Fusion',
        baseUrl: 'https://hentaifusion.me',
        rateLimitRps: 2.0,
      },
      rateLimiter,
      transport
    );
  }
}
