import { GreenShitAdapter } from '../common/greenshit-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';

export class VegitoonsAdapter extends GreenShitAdapter {
  constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch) {
    super(
      {
        id: 'vegitoons',
        name: 'Vegitoons',
        baseUrl: 'https://vegitoons.black',
        apiUrl: 'https://api.vegitoons.black',
        cdnUrl: 'https://cdn.vegitoons.black',
        scanId: '1',
        defaultGenreId: '1',
        rateLimitRps: 2.0,
      },
      rateLimiter,
      transport
    );
  }
}
