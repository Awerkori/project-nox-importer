import { MadaraAdapter } from '../common/madara-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';

export class TankouHentaiAdapter extends MadaraAdapter {
  constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch) {
    super(
      {
        id: 'tankouhentai',
        name: 'Tankou Hentai',
        baseUrl: 'https://tankouhentai.com',
        mangaSubString: 'manga',
      },
      rateLimiter,
      transport
    );
  }
}
