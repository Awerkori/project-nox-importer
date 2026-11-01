import { describe, it, expect, vi } from 'vitest';
import crypto from 'node:crypto';
import { MangoToonsAdapter } from '../../src/sources/mangotoons/mangotoons-adapter.js';
import {
  decryptMangoPayload,
  DEFAULT_MANGOTOONS_ENC_KEY,
  DEFAULT_MANGOTOONS_SALT,
} from '../../src/sources/mangotoons/mangotoons-decryptor.js';
import { HostRateLimiter } from '../../src/core/rate-limiter.js';

describe('MangoToonsAdapter & Decryptor', () => {
  it('correctly encrypts and decrypts AES-256-CBC payloads with SHA-256 key derivation', () => {
    const originalObject = {
      obra: {
        id: 14290,
        nome: 'Test Mango Work',
        total_capitulos: 12,
      },
    };
    const jsonStr = JSON.stringify(originalObject);

    // Encrypt matching Kotlin MangoThemeDecrypt specifications
    const keyBytes = crypto
      .createHash('sha256')
      .update(DEFAULT_MANGOTOONS_ENC_KEY + DEFAULT_MANGOTOONS_SALT, 'utf8')
      .digest();
    const iv = crypto.randomBytes(16);
    const cipher = crypto.createCipheriv('aes-256-cbc', keyBytes, iv);
    const encrypted = Buffer.concat([cipher.update(Buffer.from(jsonStr, 'utf8')), cipher.final()]);

    const payload = `${iv.toString('hex')}:${encrypted.toString('hex')}`;

    // Decrypt using decryptMangoPayload
    const result = decryptMangoPayload(payload);
    expect(result).toEqual(originalObject);
    expect(result.obra.nome).toBe('Test Mango Work');
  });

  it('transparently parses plaintext JSON payloads', () => {
    const payload = JSON.stringify({ status: 'ok', count: 42 });
    const result = decryptMangoPayload(payload);
    expect(result).toEqual({ status: 'ok', count: 42 });
  });

  it('fetches updated works with bootstrap pagination', async () => {
    const mockTransport = vi.fn().mockResolvedValue({
      ok: true,
      headers: new Headers({ 'content-type': 'application/json' }),
      text: async () =>
        JSON.stringify([
          {
            id: 1,
            obra_id: 101,
            obra_nome: 'Solo Leveling Mango',
            obra_slug: 'solo-leveling-mango',
            imagem: 'https://cdn.mangotoons.com/covers/101.jpg',
            published_at: '2026-09-08T10:00:00Z',
          },
        ]),
    });

    const limiter = new HostRateLimiter(10);
    const adapter = new MangoToonsAdapter(limiter, mockTransport as any);

    const result = await adapter.fetchUpdatedWorks('2026-09-08T09:00:00Z|0', { mode: 'bootstrap' });

    expect(result.works.length).toBe(1);
    expect(result.works[0].sourceWorkId).toBe('101');
    expect(result.works[0].title).toBe('Solo Leveling Mango');
    expect(result.nextCursor).toBe('2026-09-08T10:00:00Z|1');

    // Verify request URL
    const [callUrl] = mockTransport.mock.calls[0];
    expect(callUrl).toContain('/obras/lancamentos?limite=48');
  });

  it('fetches work details and maps kind, status, and genres', async () => {
    const mockTransport = vi.fn().mockResolvedValue({
      ok: true,
      headers: new Headers({ 'content-type': 'application/json' }),
      text: async () =>
        JSON.stringify({
          obra: {
            id: 202,
            nome: 'Return of the Mount Hua Sect',
            slug: 'mount-hua',
            descricao: 'Chung Myung reincarnates 100 years later.',
            formato_nome: 'Webtoon',
            status_nome: 'Em Andamento',
            imagem: '/covers/202.png',
            tags: [{ nome: 'Ação' }, { nome: 'Murim' }],
          },
        }),
    });

    const limiter = new HostRateLimiter(10);
    const adapter = new MangoToonsAdapter(limiter, mockTransport as any);

    const details = await adapter.fetchWorkDetails('202');
    expect(details.sourceWorkId).toBe('202');
    expect(details.title).toBe('Return of the Mount Hua Sect');
    expect(details.kind).toBe('WEBTOON');
    expect(details.status).toBe('ONGOING');
    expect(details.coverUrl).toBe('https://cdn.mangotoons.com/covers/202.png');
    expect(details.genres).toEqual(['Ação', 'Murim']);
  });

  it('fetches chapters and chapter pages correctly', async () => {
    const mockTransport = vi
      .fn()
      // First call: /obras/202/capitulos
      .mockResolvedValueOnce({
        ok: true,
        headers: new Headers({ 'content-type': 'application/json' }),
        text: async () =>
          JSON.stringify([
            { id: 1001, numero: 2, nome: 'Capítulo 2', total_paginas: 20 },
            { id: 1000, numero: 1, nome: 'Capítulo 1', total_paginas: 18 },
          ]),
      })
      // Second call: /leitor/obras/202/capitulos/1/manifest
      .mockResolvedValueOnce({
        ok: true,
        headers: new Headers({ 'content-type': 'application/json' }),
        text: async () =>
          JSON.stringify({
            pages: [
              { index: 1, url: 'https://cdn.mangotoons.com/p2.jpg' },
              { index: 0, url: '/p1.jpg' },
            ],
          }),
      });

    const limiter = new HostRateLimiter(10);
    const adapter = new MangoToonsAdapter(limiter, mockTransport as any);

    const chapters = await adapter.fetchChapters('202');
    expect(chapters.length).toBe(2);
    // Should be sorted ascending
    expect(chapters[0].number).toBe(1);
    expect(chapters[1].number).toBe(2);

    const pages = await adapter.fetchChapterPages(chapters[0].sourceChapterId, 1);
    expect(pages.length).toBe(2);
    // Should be sorted ascending by index: page 1 first, then page 2
    expect(pages[0]).toBe('https://cdn.mangotoons.com/p1.jpg');
    expect(pages[1]).toBe('https://cdn.mangotoons.com/p2.jpg');
  });
});
