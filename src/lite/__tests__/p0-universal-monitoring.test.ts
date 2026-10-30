import { describe, it, expect } from 'vitest';
import { HanamiHeavenAdapter } from '../../sources/hanamiheaven/hanamiheaven-adapter.js';
import { HipercoolAdapter } from '../../sources/hipercool/hipercool-adapter.js';
import { TaimuMangasAdapter } from '../../sources/taimumangas/taimumangas-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';
import { ImporterLiteRuntime } from '../runtime.js';

describe('P0 Universal Monitoring & Status Integrity', () => {
  it('correctly parses ONGOING status from HanamiHeaven Portuguese Lançando metadata', async () => {
    const adapter = new HanamiHeavenAdapter(new HostRateLimiter());
    const mockHtml = `
      <div class="profile-manga">
        <div class="post-title"><h1>Regressed Warrior's Female Dominance Diary</h1></div>
        <div class="post-status">
          <div class="post-content_item">
            <div class="summary-heading"><h5>Lançamento</h5></div>
            <div class="summary-content"><a href="https://hanamiheaven.org/manga-release/2024/">2024</a></div>
          </div>
          <div class="post-content_item">
            <div class="summary-heading"><h5>Status</h5></div>
            <div class="summary-content">Lançando</div>
          </div>
        </div>
        <div class="summary_content"><div class="description-summary"><p>Sinopse completa.</p></div></div>
      </div>
    `;

    (adapter as any).fetchHtml = async () => mockHtml;

    const details = await adapter.fetchWorkDetails('regressed-warriors-female-dominance-diary');
    expect(details.status).toBe('ONGOING');
  });

  it('correctly parses ONGOING status from Hipercool series json', async () => {
    const adapter = new HipercoolAdapter(new HostRateLimiter());
    (adapter as any).fetchJson = async () => [
      { result: { data: { json: null } } },
      {
        result: {
          data: {
            json: {
              id: 29108,
              title: "Regressed Warrior's Female Dominance Diary",
              slug: "regressed-warriors-female-dominance-diary",
              status: "ongoing",
              type: "manhwa",
            }
          }
        }
      }
    ];

    const details = await adapter.fetchWorkDetails('regressed-warriors-female-dominance-diary');
    expect(details.status).toBe('ONGOING');
  });

  it('correctly parses ONGOING status from TaimuMangas api json', async () => {
    const adapter = new TaimuMangasAdapter(new HostRateLimiter());
    (adapter as any).request = async () => ({
      identifier: 'test-manga',
      title: "Test Manga",
      status: "ongoing",
    });

    const details = await adapter.fetchWorkDetails('test-manga');
    expect(details.status).toBe('ONGOING');
  });

  it('verifies universal P0 intelligent cadence rules in runtime', () => {
    const fn = (ImporterLiteRuntime.prototype as any).monitorIntervalMs;
    const dummyTask: any = { checkpoint: {} };

    // 1. Release burst: 5 minutes
    expect(fn.call({}, dummyTask, 'ONGOING', 1)).toBe(5 * 60_000);

    // 2. Active weekly release window (8 days ago): exactly 25 minutes
    const eightDaysAgo = new Date(Date.now() - 8 * 24 * 3600 * 1000).toISOString();
    expect(fn.call({}, dummyTask, 'ONGOING', 0, eightDaysAgo)).toBe(25 * 60_000);

    // 3. Very recent release (2 days ago): paced at 45 to 90 min (never 25m, never 6h)
    const twoDaysAgo = new Date(Date.now() - 2 * 24 * 3600 * 1000).toISOString();
    const recentDelay = fn.call({}, dummyTask, 'ONGOING', 0, twoDaysAgo);
    expect(recentDelay).toBeGreaterThanOrEqual(45 * 60_000);
    expect(recentDelay).toBeLessThanOrEqual(90 * 60_000);

    // 4. Overdue/older release (18 days ago): capped at 120 minutes (never 360m / 6h)
    const eighteenDaysAgo = new Date(Date.now() - 18 * 24 * 3600 * 1000).toISOString();
    const olderDelay = fn.call({}, { checkpoint: { emptyChecks: 5 } } as any, 'ONGOING', 0, eighteenDaysAgo);
    expect(olderDelay).toBeLessThanOrEqual(120 * 60_000);
  });
});
