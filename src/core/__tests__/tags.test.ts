import { describe, it, expect, vi } from 'vitest';
import { DeduplicationEngine } from '../deduplication.js';

describe('Tag Normalization', () => {
  it('normalizes Yaoi aliases', () => {
     const engine = new DeduplicationEngine({} as any);
     expect(engine['normalizeTagName']('BL')).toBe('Yaoi');
     expect(engine['normalizeTagName']('Boys Love')).toBe('Yaoi');
     expect(engine['normalizeTagName']('shounen-ai')).toBe('Yaoi');
  });
  
  it('normalizes Adult aliases', () => {
     const engine = new DeduplicationEngine({} as any);
     expect(engine['normalizeTagName']('18+')).toBe('Adulto');
     expect(engine['normalizeTagName']('mature')).toBe('Adulto');
  });

  it('filters garbage tags', () => {
     const engine = new DeduplicationEngine({} as any);
     expect(engine['isGarbageTag']('Leia no nosso site')).toBe(true);
     expect(engine['isGarbageTag']('Completo')).toBe(true);
     expect(engine['isGarbageTag']('Manhua')).toBe(true);
     expect(engine['isGarbageTag']('Action')).toBe(false);
  });

  it('normalizes common upstream genres to the canonical Portuguese catalog', () => {
    const engine = new DeduplicationEngine({} as any);
    expect(engine['normalizeTagName']('Action')).toBe('Ação');
    expect(engine['normalizeTagName']('Horror')).toBe('Terror');
    expect(engine['normalizeTagName']('Psychological')).toBe('Psicológico');
    expect(engine['normalizeTagName']('Reincarnation')).toBe('Reencarnação');
  });
  
  it('provides default tags for specialized sources', () => {
     const engine = new DeduplicationEngine({} as any);
     expect(engine['getProviderDefaultTags']('yaoifanclub')).toContain('Yaoi');
     expect(engine['getProviderDefaultTags']('megahentai')).toContain('Hentai');
     expect(engine['getProviderDefaultTags']('randomscan')).toHaveLength(0);
  });

  it('adds normalized source genres without removing manual tags or reloading the catalog per sync', async () => {
    const catalog = [
      { id: 'action', name: 'Ação', slug: 'acao' },
      { id: 'horror', name: 'Terror', slug: 'terror' },
      { id: 'psychological', name: 'Psicológico', slug: 'psicologico' },
      { id: 'reincarnation', name: 'Reencarnação', slug: 'reencarnacao' },
    ];
    const tagSelect = vi.fn().mockResolvedValue({ data: catalog, error: null });
    const existingWorkTags = new Set(['manual-curated-tag']);
    const upsertWorkTags = vi.fn().mockImplementation(async (rows: Array<{ tag_id: string }>) => {
      for (const row of rows) existingWorkTags.add(row.tag_id);
      return { error: null };
    });
    const client = {
      from: vi.fn((table: string) => {
        if (table === 'tags') return { select: tagSelect };
        if (table === 'work_tags') return { upsert: upsertWorkTags };
        throw new Error(`unexpected table ${table}`);
      }),
    };
    const engine = new DeduplicationEngine(client as any);
    const candidate = {
      source: 'mangaflix', sourceWorkId: 'source-1', title: 'Tagged Work', slug: 'tagged-work',
      genres: ['Action', 'Horror', 'Psychological', 'Reincarnation', 'Manhua'],
    };

    await engine.syncWorkTags('work-1', candidate, false, 'MANHUA', 'mangaflix');
    await engine.syncWorkTags('work-1', candidate, false, 'MANHUA', 'mangaflix');

    expect(tagSelect).toHaveBeenCalledTimes(1);
    expect(existingWorkTags).toEqual(new Set([
      'manual-curated-tag', 'action', 'horror', 'psychological', 'reincarnation',
    ]));
    expect(upsertWorkTags).toHaveBeenCalledWith(expect.arrayContaining([
      expect.objectContaining({ work_id: 'work-1', tag_id: 'action', system_generated: true }),
    ]), { onConflict: 'work_id,tag_id' });
  });

  it('creates a genuinely new source genre as a GENRE', async () => {
    const createTag = vi.fn().mockReturnValue({
      select: () => ({ maybeSingle: async () => ({ data: { id: 'new-genre' }, error: null }) }),
    });
    const upsertWorkTags = vi.fn().mockResolvedValue({ error: null });
    const client = {
      from: vi.fn((table: string) => {
        if (table === 'tags') return {
          select: async () => ({ data: [], error: null }),
          upsert: createTag,
        };
        if (table === 'work_tags') return { upsert: upsertWorkTags };
        throw new Error(`unexpected table ${table}`);
      }),
    };
    const engine = new DeduplicationEngine(client as any);

    await engine.syncWorkTags('work-2', {
      source: 'source', sourceWorkId: 'source-2', title: 'New Genre', slug: 'new-genre', genres: ['Original Genre'],
    }, false, 'MANGA', 'source');

    expect(createTag).toHaveBeenCalledWith({
      name: 'Original genre', slug: 'original-genre', kind: 'GENRE',
    }, { onConflict: 'slug' });
    expect(upsertWorkTags).toHaveBeenCalledWith([
      { work_id: 'work-2', tag_id: 'new-genre', system_generated: true },
    ], { onConflict: 'work_id,tag_id' });
  });
});
