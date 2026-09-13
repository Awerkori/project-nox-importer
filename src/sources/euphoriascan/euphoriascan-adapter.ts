import { MadaraAdapter } from '../common/madara-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';

export class EuphoriaScanAdapter extends MadaraAdapter {
  constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch) {
    super(
      {
        id: 'euphoriascan',
        name: 'Euphoria Scan',
        baseUrl: 'https://euphoriascan.com',
        mangaSubString: 'manga',
      },
      rateLimiter,
      transport
    );
  }
}
