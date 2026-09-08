import { describe, it, expect } from 'vitest';
import { inspectImage, calculateSha256 } from '../src/storage/media.js';

describe('Media Validation & Inspection', () => {
  // Minimal valid 1x1 PNG
  const validPng = new Uint8Array([
    0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, // PNG signature
    0x00, 0x00, 0x00, 0x0d, // IHDR length (13)
    0x49, 0x48, 0x44, 0x52, // 'IHDR'
    0x00, 0x00, 0x00, 0x01, // width: 1
    0x00, 0x00, 0x00, 0x01, // height: 1
    0x08, 0x06, 0x00, 0x00, 0x00, // 8-bit RGBA
    0x1f, 0x15, 0xc4, 0x89, // CRC
    0x00, 0x00, 0x00, 0x0a, // IDAT length
    0x49, 0x44, 0x41, 0x54, // 'IDAT'
    0x78, 0x9c, 0x63, 0x00, 0x01, 0x00, 0x00, 0x05, 0x00, 0x01, // compressed data
    0x0d, 0x0a, 0x2d, 0xb4, // CRC
    0x00, 0x00, 0x00, 0x00, // IEND length (0)
    0x49, 0x45, 0x4e, 0x44, // 'IEND'
    0xae, 0x42, 0x60, 0x82, // CRC
  ]);

  // Minimal valid 1x1 JPEG
  const validJpeg = new Uint8Array([
    0xff, 0xd8, // SOI
    0xff, 0xc0, // SOF0
    0x00, 0x0b, // length 11
    0x08, // precision 8
    0x00, 0x01, // height: 1
    0x00, 0x01, // width: 1
    0x01, 0x01, 0x11, 0x00, // 1 component
    0xff, 0xda, // SOS
    0x00, 0x08, 0x01, 0x01, 0x00, 0x00, 0x3f, 0x00,
    0xff, 0xd9, // EOI
  ]);

  it('validates genuine PNG and extracts dimensions', () => {
    const info = inspectImage(validPng);
    expect(info.mime).toBe('image/png');
    expect(info.width).toBe(1);
    expect(info.height).toBe(1);
  });

  it('validates genuine JPEG and extracts dimensions', () => {
    const info = inspectImage(validJpeg);
    expect(info.mime).toBe('image/jpeg');
    expect(info.width).toBe(1);
    expect(info.height).toBe(1);
  });

  it('rejects truncated/corrupt images', () => {
    const corruptPng = validPng.slice(0, 20);
    expect(() => inspectImage(corruptPng)).toThrow();
  });

  it('rejects unsupported file formats (e.g. text/html or GIF)', () => {
    const fakeHtml = new TextEncoder().encode('<html><body>Not an image</body></html>');
    expect(() => inspectImage(fakeHtml)).toThrow(/Formato não permitido/i);
  });

  it('calculates deterministic SHA-256 hash', () => {
    const hash1 = calculateSha256(validPng);
    const hash2 = calculateSha256(validPng);
    expect(hash1).toBe(hash2);
    expect(hash1).toHaveLength(64);
  });
});
