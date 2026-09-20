import { describe, it, expect } from 'vitest';
import { CloudflareClassifier } from '../src/core/cloudflare-classifier.js';

describe('Granular Media Validation & Challenge Detection', () => {
  it('correctly classifies valid WebP image with RIFF...WEBP magic bytes', () => {
    const webpBuffer = Buffer.alloc(100);
    webpBuffer.write('RIFF', 0);
    webpBuffer.write('WEBP', 8);

    const result = CloudflareClassifier.inspect(200, { 'content-type': 'image/webp' }, '', {
      url: 'https://cdn.mangalivre.tv/storage/123/001.webp',
      expectedType: 'image',
      buffer: webpBuffer,
    });

    expect(result.isValidImage).toBe(true);
    expect(result.isChallenge).toBe(false);
    expect(result.isFakeContent).toBe(false);
  });

  it('correctly detects Cloudflare Challenge HTML disguised as WebP', () => {
    const challengeHtml = '<!DOCTYPE html><html lang="en-US"><head><title>Just a moment...</title></head><body>cf-challenge</body></html>';
    const fakeBuffer = Buffer.from(challengeHtml, 'utf-8');

    const result = CloudflareClassifier.inspect(200, { 'content-type': 'image/webp' }, challengeHtml, {
      url: 'https://cdn.mangalivre.tv/storage/123/001.webp',
      expectedType: 'image',
      buffer: fakeBuffer,
    });

    expect(result.isValidImage).toBe(false);
    expect(result.isChallenge).toBe(true);
    expect(result.isFakeContent).toBe(true);
  });

  it('rejects corrupt binary with invalid magic bytes even if content-type is image/jpeg', () => {
    const corruptBuffer = Buffer.from('NOT AN IMAGE DATA AT ALL JUST JUNK JUNK JUNK', 'utf-8');

    const result = CloudflareClassifier.inspect(200, { 'content-type': 'image/jpeg' }, '', {
      url: 'https://static.mangaflix.net/images/001.jpg',
      expectedType: 'image',
      buffer: corruptBuffer,
    });

    expect(result.isValidImage).toBe(false);
  });
});
