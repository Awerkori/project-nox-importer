import { describe, it, expect, vi } from 'vitest';
import { CloudflareClassifier } from '../src/core/cloudflare-classifier.js';
import { SourceCircuitBreaker } from '../src/core/circuit-breaker.js';
import { SharedNetworkDetector } from '../src/core/shared-network-detector.js';
import { SourceAdmissionGate } from '../src/core/source-admission-gate.js';
import { SourceAdapter } from '../src/sources/types.js';

describe('Cloudflare Classifier & Resilience Suite', () => {
  describe('CloudflareClassifier', () => {
    it('correctly identifies Cloudflare headers and 403 status', () => {
      const insp = CloudflareClassifier.inspect(
        403,
        {
          server: 'cloudflare',
          'cf-ray': 'a3a95c14fb07f41e-GIG',
        },
        '<!DOCTYPE html><html><head><title>Attention Required! | Cloudflare</title></head></html>'
      );

      expect(insp.isBlocked).toBe(true);
      expect(insp.classification).toBe('CLOUDFLARE_DATACENTER_BLOCK');
      expect(insp.cfRay).toBe('a3a95c14fb07f41e-GIG');
    });

    it('identifies Turnstile challenge', () => {
      const insp = CloudflareClassifier.inspect(
        403,
        { server: 'cloudflare' },
        '<div class="cf-turnstile" data-sitekey="0x4AAAAAA"></div>'
      );

      expect(insp.isChallenge).toBe(true);
      expect(insp.classification).toBe('TURNSTILE');
    });

    it('identifies JS Challenge / HCDN challenge', () => {
      const insp = CloudflareClassifier.inspect(
        403,
        { server: 'hcdn' },
        '<form action="/hcdn-cgi/jschallenge-validate" method="POST"></form>'
      );

      expect(insp.isChallenge).toBe(true);
      expect(insp.classification).toBe('JS_CHALLENGE');
    });

    it('detects fake HTML challenge returned with HTTP 200 when image was expected', () => {
      const fakeHtml = '<!DOCTYPE html><html><body>Just a moment...</body></html>';
      const fakeBuf = new TextEncoder().encode(fakeHtml);

      const insp = CloudflareClassifier.inspect(
        200,
        { 'content-type': 'text/html; charset=utf-8' },
        fakeHtml,
        {
          expectedType: 'image',
          buffer: fakeBuf,
        }
      );

      expect(insp.isFakeContent).toBe(true);
      expect(insp.isBlocked).toBe(true);
      expect(insp.isValidImage).toBe(false);
    });

    it('validates genuine binary JPEG and PNG images with magic bytes', () => {
      // JPEG: FF D8 FF
      const jpegBytes = new Uint8Array([0xff, 0xd8, 0xff, 0xe0, 0x00, 0x10, 0x4a, 0x46, 0x49, 0x46]);
      const jpegInsp = CloudflareClassifier.inspect(
        200,
        { 'content-type': 'image/jpeg' },
        '',
        {
          expectedType: 'image',
          buffer: jpegBytes,
        }
      );
      expect(jpegInsp.isValidImage).toBe(true);
      expect(jpegInsp.isFakeContent).toBe(false);
      expect(jpegInsp.isBlocked).toBe(false);

      // PNG: 89 50 4E 47
      const pngBytes = new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00]);
      const pngInsp = CloudflareClassifier.inspect(
        200,
        { 'content-type': 'image/png' },
        '',
        {
          expectedType: 'image',
          buffer: pngBytes,
        }
      );
      expect(pngInsp.isValidImage).toBe(true);
      expect(pngInsp.isFakeContent).toBe(false);
      expect(pngInsp.isBlocked).toBe(false);
    });
  });

  describe('SourceCircuitBreaker', () => {
    it('manages CLOSED -> DEGRADED -> OPEN -> HALF_OPEN lifecycle', () => {
      const breaker = new SourceCircuitBreaker(3, 1000, 5000);
      const source = 'test-breaker-source';

      expect(breaker.canExecute(source)).toBe(true);
      expect(breaker.getState(source)).toBe('CLOSED');

      // Failure 1 -> DEGRADED
      breaker.recordFailure(source, 'UNKNOWN_CLOUDFLARE');
      expect(breaker.getState(source)).toBe('DEGRADED');
      expect(breaker.canExecute(source)).toBe(true);

      // Failure 2 -> DEGRADED
      breaker.recordFailure(source, 'UNKNOWN_CLOUDFLARE');
      expect(breaker.getState(source)).toBe('DEGRADED');
      expect(breaker.canExecute(source)).toBe(true);

      // Failure 3 -> Trips to OPEN
      const tripRes = breaker.recordFailure(source, 'CLOUDFLARE_DATACENTER_BLOCK');
      expect(tripRes.tripped).toBe(true);
      expect(breaker.getState(source)).toBe('OPEN');
      expect(breaker.canExecute(source)).toBe(false);

      // Reset
      breaker.reset(source);
      expect(breaker.getState(source)).toBe('CLOSED');
      expect(breaker.canExecute(source)).toBe(true);
    });
  });

  describe('SharedNetworkDetector', () => {
    it('triggers incident when 5 distinct sources fail within window', () => {
      const detector = new SharedNetworkDetector(60_000, 5);

      expect(detector.isSharedBlockActive()).toBe(false);

      detector.recordBlockEvent({ sourceId: 'src-1', classification: 'CLOUDFLARE_DATACENTER_BLOCK' });
      detector.recordBlockEvent({ sourceId: 'src-2', classification: 'CLOUDFLARE_DATACENTER_BLOCK' });
      detector.recordBlockEvent({ sourceId: 'src-3', classification: 'CLOUDFLARE_DATACENTER_BLOCK' });
      detector.recordBlockEvent({ sourceId: 'src-4', classification: 'CLOUDFLARE_DATACENTER_BLOCK' });
      expect(detector.isSharedBlockActive()).toBe(false);

      // 5th distinct source trips shared network block
      const isTripped = detector.recordBlockEvent({ sourceId: 'src-5', classification: 'CLOUDFLARE_DATACENTER_BLOCK' });
      expect(isTripped).toBe(true);
      expect(detector.isSharedBlockActive()).toBe(true);

      const summary = detector.getIncidentSummary();
      expect(summary.active).toBe(true);
      expect(summary.affectedSources.length).toBe(5);
    });
  });

  describe('SourceAdmissionGate', () => {
    it('executes 6-stage probe successfully when all endpoints return clean responses', async () => {
      const gate = new SourceAdmissionGate();

      const mockAdapter: SourceAdapter = {
        id: 'clean-source',
        name: 'Clean Source',
        baseUrl: 'https://clean.example.com',
        searchWorks: vi.fn(async () => [
          { sourceWorkId: 'work-1', title: 'Work 1', slug: 'work-1' },
        ]),
        fetchUpdatedWorks: vi.fn(),
        fetchWorkDetails: vi.fn(async () => ({
          title: 'Work 1',
          sourceWorkId: 'work-1',
          synopsis: 'Test',
        })),
        fetchChapters: vi.fn(async () => [
          { sourceChapterId: 'ch-1', number: 1, title: 'Ch 1', pageCount: 1 },
        ]),
        fetchChapterPages: vi.fn(async () => [
          'https://cdn.clean.example.com/p1.jpg',
        ]),
      };

      const jpegBytes = new Uint8Array([0xff, 0xd8, 0xff, 0xe0, 0x00, 0x10, 0x4a, 0x46, 0x49, 0x46]);

      const mockTransport = vi.fn(async (url: string) => {
        if (url.includes('p1.jpg')) {
          return {
            ok: true,
            status: 200,
            headers: new Headers({ 'content-type': 'image/jpeg' }),
            arrayBuffer: async () => jpegBytes.buffer,
          } as any;
        }
        return {
          ok: true,
          status: 200,
          headers: new Headers({ 'content-type': 'text/html' }),
          text: async () => '<html><body>Welcome</body></html>',
        } as any;
      });

      const report = await gate.executeProdProbe(mockAdapter, mockTransport);
      expect(report.overallStatus).toBe('PASS');
      expect(report.recommendedState).toBe('PROD_WARMUP');
      expect(report.stages.length).toBe(6);
      expect(report.stages.every((s) => s.status === 'PASS')).toBe(true);
    });

    it('immediately rejects and marks UPSTREAM_BLOCKED if Base URL returns Cloudflare 403', async () => {
      const gate = new SourceAdmissionGate();

      const mockAdapter: SourceAdapter = {
        id: 'blocked-source',
        name: 'Blocked Source',
        baseUrl: 'https://cf-blocked.example.com',
        fetchUpdatedWorks: vi.fn(),
      };

      const mockTransport = vi.fn(async () => {
        return {
          ok: false,
          status: 403,
          headers: new Headers({
            server: 'cloudflare',
            'cf-ray': 'test-ray-123',
          }),
          text: async () => '<!DOCTYPE html>Cloudflare 403 Forbidden',
        } as any;
      });

      const report = await gate.executeProdProbe(mockAdapter, mockTransport);
      expect(report.overallStatus).toBe('FAIL');
      expect(report.recommendedState).toBe('UPSTREAM_BLOCKED');
      expect(report.isAsnBlock).toBe(true);
      expect(report.cfRay).toBe('test-ray-123');
    });
  });
});
