import { describe, expect, it, vi } from 'vitest';
import { DeduplicationEngine, type CandidateWork } from '../src/core/deduplication.js';

function readOne(data: any) {
  const chain: any = {
    select: () => chain,
    eq: () => chain,
    maybeSingle: async () => ({ data, error: null }),
  };
  return chain;
}

function candidate(loadCoverId: () => Promise<string | null>): CandidateWork {
  return {
    source: 'mangaflix',
    sourceWorkId: 'source-work-1',
    title: 'Known Work',
    slug: 'known-work',
    loadCoverId,
  };
}

function engineFor(input: { mapping: any; work?: any; media?: any }) {
  const updates: any[] = [];
  const supabase = {
    from(table: string) {
      if (table === 'importer_work_mappings') {
        const chain = readOne(input.mapping);
        chain.update = (values: any) => ({
          eq: async () => {
            updates.push({ mapping: values });
            return { error: null };
          },
        });
        return chain;
      }
      if (table === 'media') return readOne(input.media);
      if (table === 'works') {
        const chain = readOne(input.work);
        chain.update = (values: any) => ({
          eq: async () => {
            updates.push(values);
            return { error: null };
          },
        });
        return chain;
      }
      throw new Error(`unexpected table ${table}`);
    },
  };
  const engine = new DeduplicationEngine(supabase as any);
  engine.syncWorkTags = vi.fn().mockResolvedValue(undefined);
  return { engine, updates };
}

describe('deferred sync cover loading', () => {
  const mapping = {
    id: 'mapping-1',
    source: 'mangaflix',
    source_work_id: 'source-work-1',
    source_slug: 'known-work',
    work_id: 'work-1',
    sync_status: 'SYNCED',
    metadata: {},
  };
  const work = {
    id: 'work-1',
    title: 'Known Work',
    aliases: [],
    synopsis: 'Already complete metadata',
    description: 'Already complete metadata',
    author: 'Known author',
    artist: 'Known artist',
    kind: 'MANHWA',
    status: 'ONGOING',
    year: 2025,
    age_rating: 12,
    cover_id: 'healthy-cover',
    metadata_provenance: {},
    content_rating: 'GENERAL',
  };

  it('does not load a source cover when the canonical cover is healthy', async () => {
    const loadCoverId = vi.fn(async () => 'unused-cover');
    const { engine, updates } = engineFor({
      mapping,
      work,
      media: { id: 'healthy-cover', storage_ready: true, bytes: 2500, width: 300, height: 420 },
    });

    const result = await engine.resolveWork(candidate(loadCoverId));

    expect(result.status).toBe('EXISTING_MAPPING');
    expect(loadCoverId).not.toHaveBeenCalled();
    expect(updates.some((update) => update.cover_id)).toBe(false);
  });

  it('loads a source cover to repair an unhealthy canonical cover', async () => {
    const loadCoverId = vi.fn(async () => 'replacement-cover');
    const { engine, updates } = engineFor({
      mapping,
      work,
      media: { id: 'healthy-cover', storage_ready: false, bytes: 0, width: 0, height: 0 },
    });

    await engine.resolveWork(candidate(loadCoverId));

    expect(loadCoverId).toHaveBeenCalledTimes(1);
    expect(updates).toContainEqual(expect.objectContaining({ cover_id: 'replacement-cover' }));
  });

  it('does not load a cover for an identity that remains ambiguous', async () => {
    const loadCoverId = vi.fn(async () => 'unused-cover');
    const { engine } = engineFor({
      mapping: {
        ...mapping,
        work_id: null,
        sync_status: 'AMBIGUOUS',
        metadata: { ambiguity_reason: 'Conflict with multiple existing works or low confidence score' },
      },
    });

    const result = await engine.resolveWork(candidate(loadCoverId));

    expect(result.status).toBe('AMBIGUOUS');
    expect(loadCoverId).not.toHaveBeenCalled();
  });
});
