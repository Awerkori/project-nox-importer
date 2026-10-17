import { describe, expect, it } from 'vitest';
import { ImperioDaBritanniaAdapter } from '../../src/sources/imperiodabritannia/imperiodabritannia-adapter.js';
import { HostRateLimiter } from '../../src/core/rate-limiter.js';

describe('ImperioDaBritanniaAdapter', () => {
  it('uses the current public MangoTheme API probe and conservative limits', () => {
    const adapter = new ImperioDaBritanniaAdapter(new HostRateLimiter(1));
    expect(adapter.id).toBe('imperiodabritannia');
    expect(adapter.admissionProbe.url).toContain('/api/obras?pagina=1&limite=1');
    expect(adapter.admissionProbe.expectedType).toBe('json');
    expect(adapter.isChapterOwnedByWork('42', '42:1.00')).toBe(true);
    expect(adapter.isChapterOwnedByWork('42', '43:1.00')).toBe(false);
  });
});
