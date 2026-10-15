import { describe, it, expect, vi } from 'vitest';
import { DeduplicationEngine } from '../deduplication.js';
import { CANONICAL_ADULT_TAG_NAME, canonicalAdultTagName } from '../tag-normalization.js';

describe('Tag Normalization', () => {
  it('normalizes Yaoi aliases', () => {
     const engine = new DeduplicationEngine({} as any);
     expect(engine['normalizeTagName']('BL')).toBe('Yaoi');
     expect(engine['normalizeTagName']('Boys Love')).toBe('Yaoi');
     expect(engine['normalizeTagName']('shounen-ai')).toBe('Yaoi');
  });
  
  it('normalizes Adult aliases', () => {
     const engine = new DeduplicationEngine({} as any);
     for (const alias of ['Adulto', '+18', '18+', 'Adult', 'Adulto +18', 'Adulto (+18)', 'mature']) {
       expect(engine['normalizeTagName'](alias)).toBe(CANONICAL_ADULT_TAG_NAME);
       expect(canonicalAdultTagName(alias)).toBe(CANONICAL_ADULT_TAG_NAME);
     }
     expect(canonicalAdultTagName('Adult adventure')).toBeNull();
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

  it('merges known mapped metadata without resolving the source mapping again', async () => {
    const engine = new DeduplicationEngine({} as any);
    const merge = vi.spyOn(engine, 'applyMetadataPrecedence').mockResolvedValue();
    const candidate = { source: 'source-a', sourceWorkId: 'known-id', title: 'Título', slug: 'titulo' };

    await expect(engine.mergeKnownMappedWork(candidate, 'work-id', 'source-a')).resolves.toMatchObject({
      workId: 'work-id', status: 'EXISTING_MAPPING', slug: 'titulo',
    });
    expect(merge).toHaveBeenCalledTimes(1);
    expect(merge).toHaveBeenCalledWith('work-id', candidate, 'source-a');
  });

  it('rejects challenge titles before merging known mapped metadata', async () => {
    const engine = new DeduplicationEngine({} as any);
    const merge = vi.spyOn(engine, 'applyMetadataPrecedence').mockResolvedValue();

    await expect(engine.mergeKnownMappedWork({
      source: 'source-a', sourceWorkId: 'known-id', title: 'Login', slug: 'login',
    }, 'work-id', 'source-a')).resolves.toMatchObject({ status: 'FAILED', workId: null });
    expect(merge).not.toHaveBeenCalled();
  });

  it('keeps explicit detailed adult taxonomy distinct from the generic adult rating', () => {
    const engine = new DeduplicationEngine({} as any);
    expect(engine['normalizeTagName']('milf')).toBe('MILF');
    expect(engine['normalizeTagName']('NTR')).toBe('NTR');
    expect(engine['normalizeTagName']('netorare')).toBe('Netorare');
    expect(engine['normalizeTagName']('vanilla')).toBe('Vanilla');
    expect(engine['normalizeTagName']('Adulto')).toBe(CANONICAL_ADULT_TAG_NAME);
  });
  
  it('never invents tags from an adult or specialized source name', async () => {
    const workTagUpsert = vi.fn().mockResolvedValue({ data: [], error: null });
    const engine = new DeduplicationEngine({
      from: (table: string) => {
        if (table === 'tags') return { select: async () => ({ data: [], error: null }) };
        if (table === 'work_tags') return { upsert: workTagUpsert };
        throw new Error(`unexpected table ${table}`);
      },
    } as any);

    await engine.syncWorkTags('adult-work', {
      source: 'megahentai', sourceWorkId: 'adult-1', title: 'Explicitly untagged work', slug: 'untagged', genres: [],
    }, true, 'PORNHWA', 'megahentai');

    expect(workTagUpsert).not.toHaveBeenCalled();
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
    const upsertWorkTags = vi.fn().mockImplementation((rows: Array<{ tag_id: string }>) => ({
      select: async () => {
        for (const row of rows) existingWorkTags.add(row.tag_id);
        return { data: rows, error: null };
      },
    }));
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
    ]), { onConflict: 'work_id,tag_id', ignoreDuplicates: true });
  });

  it('creates a genuinely new source genre as a GENRE', async () => {
    const createTag = vi.fn().mockReturnValue({
      select: async () => ({ data: [{ id: 'new-genre', name: 'Original genre', slug: 'original-genre' }], error: null }),
    });
    const upsertWorkTags = vi.fn().mockReturnValue({
      select: async () => ({ data: [{ tag_id: 'new-genre' }], error: null }),
    });
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

    expect(createTag).toHaveBeenCalledWith([{
      name: 'Original genre', slug: 'original-genre', kind: 'GENRE',
    }], { onConflict: 'slug' });
    expect(upsertWorkTags).toHaveBeenCalledWith([
      { work_id: 'work-2', tag_id: 'new-genre', system_generated: true },
    ], { onConflict: 'work_id,tag_id', ignoreDuplicates: true });
  });

  it('removes only legacy system-generated type tags during reconciliation', async () => {
    const deleteIn = vi.fn().mockResolvedValue({ error: null });
    const deleteSystemGenerated = vi.fn(() => ({ in: deleteIn }));
    const deleteWork = vi.fn(() => ({ eq: deleteSystemGenerated }));
    const deleteRoot = vi.fn(() => ({ eq: deleteWork }));
    const client = {
      from: vi.fn((table: string) => {
        if (table === 'tags') return {
          select: async () => ({ data: [
            { id: 'manhua-type', name: 'Manhua', slug: 'manhua' },
            { id: 'manhwa-type', name: 'Manhwa', slug: 'manhwa' },
          ], error: null }),
        };
        if (table === 'work_tags') return { delete: deleteRoot };
        throw new Error(`unexpected table ${table}`);
      }),
    };
    const engine = new DeduplicationEngine(client as any);

    await engine.removeLegacySystemGeneratedNonGenreTags('work-3');

    expect(deleteRoot).toHaveBeenCalledOnce();
    expect(deleteWork).toHaveBeenCalledWith('work_id', 'work-3');
    expect(deleteSystemGenerated).toHaveBeenCalledWith('system_generated', true);
    expect(deleteIn).toHaveBeenCalledWith('tag_id', expect.arrayContaining(['manhua-type', 'manhwa-type']));
  });
});
