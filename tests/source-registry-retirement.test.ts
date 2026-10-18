import { describe, expect, it } from 'vitest';
import { HostRateLimiter } from '../src/core/rate-limiter.js';
import { SourceRegistry } from '../src/sources/registry.js';

describe('retired source registry', () => {
  it('does not expose Kuro or its legacy alias as a runnable adapter', () => {
    const registry = new SourceRegistry(new HostRateLimiter(1));

    expect(registry.get('kuro')).toBeUndefined();
    expect(registry.get('kuromangas')).toBeUndefined();
    expect(registry.getAll().some((adapter) => adapter.id === 'kuro')).toBe(false);
  });
});
