import { MadaraAdapter } from '../common/madara-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';

export class MangaLivreToAdapter extends MadaraAdapter {
  constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch) {
    super(
      {
        id: 'mangalivreto',
        name: 'Manga Livre.to',
        baseUrl: 'https://mangalivre.to',
        mangaSubString: 'manga',
      },
      rateLimiter,
      transport
    );
  }
}
