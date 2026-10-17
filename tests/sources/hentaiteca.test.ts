import { describe, expect, it } from 'vitest';
import { HentaiTecaAdapter } from '../../src/sources/hentaiteca/hentaiteca-adapter.js';
import { HostRateLimiter } from '../../src/core/rate-limiter.js';

describe('HentaiTecaAdapter', () => {
  it('uses the maintained Madara contract with conservative source limits', () => {
    const adapter = new HentaiTecaAdapter(new HostRateLimiter(1));

    expect(adapter.id).toBe('hentaiteca');
    expect(adapter.baseUrl).toBe('https://hentaiteca.online');
    expect(adapter.probeUrl).toBe('https://hentaiteca.online/manga/?order=update');
    expect(adapter.admissionProbe.usableHtmlMarker).toEqual(/\bwp-manga\b/i);
  });
});
