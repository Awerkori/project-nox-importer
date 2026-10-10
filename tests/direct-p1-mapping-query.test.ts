import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';

const source = readFileSync(new URL('../src/db/yugabyte-direct.ts', import.meta.url), 'utf8');

describe('direct P1 catalog mapping selector', () => {
  it('keeps the indexed source-work lookup separate from the legacy work-id fallback', () => {
    const selector = source.slice(
      source.indexOf('const query = `', source.indexOf('export async function acquireJobsDirect')),
      source.indexOf('const res = await p.query', source.indexOf('export async function acquireJobsDirect')),
    );

    expect(selector).toContain('wm_by_source_work_id.source = q.source');
    expect(selector).toContain("wm_by_source_work_id.source_work_id = q.payload->>'sourceWorkId'");
    expect(selector).toContain('wm_by_work_id.source = q.source');
    expect(selector).toContain("wm_by_work_id.work_id::text = q.payload->>'workId'");
    expect(selector).not.toMatch(
      /wm\.source = q\.source\s+AND\s+\(\s*wm\.source_work_id = q\.payload->>'sourceWorkId'\s+OR\s+wm\.work_id::text = q\.payload->>'workId'/,
    );
  });
});
