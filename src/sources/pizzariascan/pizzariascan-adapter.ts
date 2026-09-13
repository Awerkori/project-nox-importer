import { MangaWorkAdapter } from '../common/mangawork-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';

export class PizzariaScanAdapter extends MangaWorkAdapter {
  constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch) {
    super(
      {
        id: 'pizzariascan',
        name: 'Pizzaria Scan',
        baseUrl: 'https://pizzariacomics.com',
        seriesPath: 'todas-as-obras',
        rateLimitRps: 2.0,
      },
      rateLimiter,
      transport
    );
  }
}
