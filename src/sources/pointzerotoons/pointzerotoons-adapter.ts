import { MangaThemesiaAdapter } from '../common/mangathemesia-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';

export class PointZeroToonsAdapter extends MangaThemesiaAdapter {
  constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch) {
    super(
      {
        id: 'pointzerotoons',
        name: 'Point Zero Toons',
        baseUrl: 'https://kitsuneyako.com',
        mangaSubString: 'manga',
        rateLimitRps: 2.0,
      },
      rateLimiter,
      transport
    );
  }
}
