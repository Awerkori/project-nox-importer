import { WpGalleryAdapter } from '../common/wpgallery-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';

export class ZettaHqAdapter extends WpGalleryAdapter {
  constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch) {
    super(
      {
        id: 'zettahq',
        name: 'ZettaHQ',
        baseUrl: 'https://zettahq.com',
        rateLimitRps: 2.0,
      },
      rateLimiter,
      transport
    );
  }
}
