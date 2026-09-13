import { MadaraAdapter } from '../common/madara-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';

export class CafeComYaoiAdapter extends MadaraAdapter {
  constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch) {
    super(
      {
        id: 'cafecomyaoi',
        name: 'Café com Yaoi',
        baseUrl: 'https://cafecomyaoi.com.br',
        mangaSubString: 'manga',
      },
      rateLimiter,
      transport
    );
  }
}
