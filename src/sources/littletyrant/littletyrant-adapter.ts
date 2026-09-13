import { MadaraAdapter } from '../common/madara-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';

export class LittleTyrantAdapter extends MadaraAdapter {
  constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch) {
    super(
      {
        id: 'littletyrant',
        name: 'Little Tyrant',
        baseUrl: 'https://tiraninha.world',
        mangaSubString: 'manga',
      },
      rateLimiter,
      transport
    );
  }
}
