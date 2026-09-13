import { MadaraAdapter } from '../common/madara-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';

export class MangaOnlineTvAdapter extends MadaraAdapter {
  constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch) {
    super(
      {
        id: 'mangaonlinetv',
        name: 'Manga Online tv',
        baseUrl: 'https://mangaonline.tv',
        mangaSubString: 'manga',
        rateLimitRps: 2.0,
      },
      rateLimiter,
      transport
    );
  }
}
