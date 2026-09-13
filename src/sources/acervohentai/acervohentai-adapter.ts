import { MadaraAdapter } from '../common/madara-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';

export class AcervoHentaiAdapter extends MadaraAdapter {
  constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch) {
    super(
      {
        id: 'acervohentai',
        name: 'Acervo Hentai',
        baseUrl: 'https://acervohentai.com',
        mangaSubString: 'manhwa',
        rateLimitRps: 2.0,
      },
      rateLimiter,
      transport
    );
  }
}
