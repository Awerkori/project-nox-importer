import { describe, it, expect, vi } from 'vitest';
import { KuroAdapter } from '../../src/sources/kuro/kuro-adapter.js';
import {
  RabbitCipher,
  evpBytesToKey,
  derivePassword,
  decryptVSecure,
  DEFAULT_ENC_KEY,
} from '../../src/sources/kuro/kuro-decryptor.js';
import { HostRateLimiter } from '../../src/core/rate-limiter.js';

describe('KuroAdapter & Rabbit Decryptor', () => {
  it('verifies RabbitCipher symmetric encryption and decryption', () => {
    const key = Buffer.from('1234567890abcdef');
    const iv = Buffer.from('abcdef12');

    const cipher1 = new RabbitCipher();
    cipher1.setup(key, iv);

    const original = Buffer.from('Sensitive payload: {"manga_id": 42, "secret": "none"}', 'utf8');
    const encrypted = cipher1.crypt(original);

    expect(encrypted).not.toEqual(original);

    const cipher2 = new RabbitCipher();
    cipher2.setup(key, iv);
    const decrypted = cipher2.crypt(encrypted);

    expect(decrypted.toString('utf8')).toBe('Sensitive payload: {"manga_id": 42, "secret": "none"}');
  });

  it('verifies EVP key derivation and password derivation', () => {
    const pwd = derivePassword('2026-09-08');
    expect(pwd.startsWith(DEFAULT_ENC_KEY)).toBe(true);
    expect(pwd.length).toBe(DEFAULT_ENC_KEY.length + 8);

    const salt = Buffer.from('12345678');
    const { key, iv } = evpBytesToKey(Buffer.from(pwd, 'utf8'), salt);
    expect(key.length).toBe(16);
    expect(iv.length).toBe(8);
  });

  it('correctly decrypts simulated _v_secure payload', () => {
    const testDate = new Date().toISOString().split('T')[0];
    const pwd = derivePassword(testDate);
    const salt = Buffer.from('87654321');
    const { key, iv } = evpBytesToKey(Buffer.from(pwd, 'utf8'), salt);

    const cipher = new RabbitCipher();
    cipher.setup(key, iv);

    const payload = JSON.stringify({
      data_key_123: {
        title: 'Decrypted Kuro Manga',
        id: 777,
      },
    });

    const ciphertext = cipher.crypt(Buffer.from(payload, 'utf8'));
    // OpenSSL / CryptoJS format: "Salted__" (8 bytes) + salt (8 bytes) + ciphertext
    const saltedHeader = Buffer.from('Salted__', 'utf8');
    const fullEncrypted = Buffer.concat([saltedHeader, salt, ciphertext]);
    const vSecureBase64 = fullEncrypted.toString('base64');

    const decrypted = decryptVSecure(vSecureBase64, 'data_key_123');
    expect(decrypted).toEqual({
      title: 'Decrypted Kuro Manga',
      id: 777,
    });
  });

  it('safely throws descriptive error when authentication is missing without leaking secrets', async () => {
    const mockTransport = vi.fn().mockResolvedValue({
      ok: false,
      status: 401,
      headers: new Headers(),
      text: async () => 'Unauthorized',
    });

    const rateLimiter = new HostRateLimiter(10.0);
    const adapter = new KuroAdapter(rateLimiter, mockTransport as any);

    await expect(adapter.fetchUpdatedWorks(null, { mode: 'maintenance' })).rejects.toThrow(
      /Kuro requires authentication/
    );
  });

  it('processes authenticated responses cleanly', async () => {
    const mockTransport = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      headers: new Headers(),
      json: async () => ({
        data: [
          {
            manga_id: 101,
            manga_title: 'Chainsaw Man',
            manga_cover: '/uploads/covers/csm.jpg',
          },
        ],
        pagination: { hasNext: false },
      }),
    });

    const rateLimiter = new HostRateLimiter(10.0);
    const adapter = new KuroAdapter(rateLimiter, mockTransport as any);

    const { works } = await adapter.fetchUpdatedWorks(null, { mode: 'maintenance' });
    expect(works.length).toBe(1);
    expect(works[0].sourceWorkId).toBe('101');
    expect(works[0].title).toBe('Chainsaw Man');
    expect(works[0].slug).toBe('chainsaw-man');
    expect(works[0].coverUrl).toBe('https://cdn.kuromangas.com/covers/csm.jpg');
  });

  it('fetches work details, chapters and pages', async () => {
    const mockTransport = vi.fn().mockImplementation(async (url: string) => {
      if (url.includes('/mangas/101')) {
        return {
          ok: true,
          status: 200,
          headers: new Headers(),
          json: async () => ({
            manga: {
              id: 101,
              title: 'Chainsaw Man',
              cover_image: '/covers/csm.jpg',
              description: 'Denji is a teenage boy living with a Chainsaw Devil.',
              author: 'Tatsuki Fujimoto',
              genres: ['Action', 'Supernatural'],
              status: 'ongoing',
            },
            chapters: [
              {
                id: 501,
                chapter_number: '1',
                title: 'Dog & Chainsaw',
                upload_date: '2026-09-01T12:00:00Z',
              },
            ],
          }),
        };
      }
      if (url.includes('/chapters/501')) {
        return {
          ok: true,
          status: 200,
          headers: new Headers(),
          json: async () => ({
            id: 501,
            pages: ['/uploads/page1.webp', '/uploads/page2.webp'],
          }),
        };
      }
      throw new Error(`Unexpected url: ${url}`);
    });

    const rateLimiter = new HostRateLimiter(10.0);
    const adapter = new KuroAdapter(rateLimiter, mockTransport as any);

    const details = await adapter.fetchWorkDetails('101');
    expect(details.title).toBe('Chainsaw Man');
    expect(details.author).toBe('Tatsuki Fujimoto');
    expect(details.status).toBe('ONGOING');

    const chapters = await adapter.fetchChapters('101');
    expect(chapters.length).toBe(1);
    expect(chapters[0].sourceChapterId).toBe('501');
    expect(chapters[0].number).toBe(1);

    const pages = await adapter.fetchChapterPages('501');
    expect(pages).toEqual([
      'https://cdn.kuromangas.com/page1.webp',
      'https://cdn.kuromangas.com/page2.webp',
    ]);
  });
});
