import { describe, expect, it } from 'vitest';
import { SourceAdmissionGate } from '../source-admission-gate.js';

describe('SourceAdmissionGate', () => {
  it('falls back to a bounded bootstrap work when search returns chapter-post false works', async () => {
    const gate = new SourceAdmissionGate(1_000);
    const adapter = {
      id: 'feed-source', name: 'Feed source', baseUrl: 'https://source.test',
      async searchWorks() { return [{ sourceWorkId: 'chapter-post', title: 'Chapter post' }]; },
      async fetchUpdatedWorks() { return { works: [{ sourceWorkId: 'canonical-work', title: 'Series' }], nextCursor: null }; },
      async fetchWorkDetails(sourceWorkId: string) { return { sourceWorkId, title: sourceWorkId }; },
      async fetchChapters(sourceWorkId: string) {
        return sourceWorkId === 'canonical-work' ? [{ sourceChapterId: 'chapter-1', number: 1, title: 'Chapter 1' }] : [];
      },
      async fetchChapterPages() { return ['https://image.test/page.webp']; },
    } as any;
    const transport = async (url: string) => {
      if (url === 'https://image.test/page.webp') {
        const image = new Uint8Array(5_000);
        image.set([0xff, 0xd8, 0xff, 0xe0, 0, 0, 0, 0]);
        return new Response(image, { status: 200, headers: { 'content-type': 'image/jpeg' } });
      }
      return new Response('<html><body>ok</body></html>', { status: 200, headers: { 'content-type': 'text/html' } });
    };

    const report = await gate.executeProdProbe(adapter, transport as typeof fetch);
    expect(report.overallStatus).toBe('PASS');
    expect(report.stages.find((stage) => stage.stage === 'CHAPTERS')).toMatchObject({ status: 'PASS' });
  });
});
