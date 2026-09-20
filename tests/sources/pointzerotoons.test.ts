import { describe, it, expect } from 'vitest';
import { PointZeroToonsAdapter } from '../../src/sources/pointzerotoons/pointzerotoons-adapter.js';

describe('PointZeroToonsAdapter', () => {
  it('instantiates correctly with Kitsune Yako configuration', () => {
    const adapter = new PointZeroToonsAdapter();
    expect(adapter.id).toBe('pointzerotoons');
    expect(adapter.name).toBe('Kitsune Yako');
    expect(adapter.baseUrl).toBe('https://kitsuneyako.com');
  });

  it('provides custom image headers with Referer', () => {
    const adapter = new PointZeroToonsAdapter();
    const headers = adapter.getImageHeaders('https://cdn.kitsuneyako.com/image.webp');
    expect(headers).toBeDefined();
    expect(headers.Referer).toBe('https://kitsuneyako.com/');
  });
});
