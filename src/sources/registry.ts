import { SourceAdapter } from './types.js';
import { NexusAdapter } from './nexus/nexus-adapter.js';
import { HostRateLimiter } from '../core/rate-limiter.js';

export class SourceRegistry {
  private adapters = new Map<string, SourceAdapter>();

  constructor(rateLimiter: HostRateLimiter) {
    // Register pilot adapter
    this.register(new NexusAdapter(rateLimiter));
  }

  register(adapter: SourceAdapter): void {
    this.adapters.set(adapter.id, adapter);
  }

  get(id: string): SourceAdapter | undefined {
    return this.adapters.get(id);
  }

  getAll(): SourceAdapter[] {
    return Array.from(this.adapters.values());
  }
}
