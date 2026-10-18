import { describe, expect, it } from 'vitest';
import { HostRateLimiter } from '../src/core/rate-limiter.js';
import { SourceRegistry } from '../src/sources/registry.js';

describe('retired source registry', () => {
  it('does not expose any retired provider or legacy alias as a runnable adapter', () => {
    const registry = new SourceRegistry(new HostRateLimiter(1));

    const retiredIds = [
      'kuro', 'kuromangas', 'megahentai', 'osakascan',
      'acervohentai', 'amuy', 'arthurscan', 'geasscomics', 'inkapk',
      'mangaonline', 'mangastop', 'pointzerotoons', 'pointzero',
      'point_zero_toons', 'kitsuneyako', 'vegitoons', 'yaoifanclub', 'yuriverso',
    ];

    for (const id of retiredIds) expect(registry.get(id)).toBeUndefined();
    expect(registry.getAll()).toHaveLength(43);
  });
});
