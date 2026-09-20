import { describe, it, expect } from 'vitest';
import { GeassComicsAdapter } from '../../src/sources/geasscomics/geasscomics-adapter.js';

describe('GeassComicsAdapter', () => {
  it('instantiates correctly with Geass Comics configuration', () => {
    const adapter = new GeassComicsAdapter();
    expect(adapter.id).toBe('geasscomics');
    expect(adapter.name).toBe('Geass Comics');
    expect(adapter.baseUrl).toBe('https://geasscomics.xyz');
  });

  it('provides custom image headers with Referer', () => {
    const adapter = new GeassComicsAdapter();
    const headers = adapter.getImageHeaders('https://cdn.geasscomics.xyz/image.webp');
    expect(headers).toBeDefined();
    expect(headers.Referer).toBe('https://geasscomics.xyz/');
  });
});
