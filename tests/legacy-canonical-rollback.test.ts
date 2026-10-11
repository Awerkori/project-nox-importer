import { randomUUID } from 'node:crypto';
import { PGlite } from '@electric-sql/pglite';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import {
  legacyRollbackChapterIdentity,
  publishBatchDirect,
  setYugabytePoolForTest,
} from '../src/db/yugabyte-direct.js';

let db: PGlite;
const workId = '00000000-0000-4000-8000-000000000101';

beforeEach(async () => {
  db = new PGlite();
  await db.exec(`
    CREATE TABLE works (id uuid PRIMARY KEY, slug text, title text, latest_chapter_published_at timestamptz, updated_at timestamptz);
    CREATE TABLE chapters (
      id uuid PRIMARY KEY, work_id uuid NOT NULL, number numeric, title text, published_at timestamptz,
      origin text, views_total integer, created_at timestamptz,
      canonical_chapter_key text, display_number text, chapter_order_key text,
      chapter_kind text, edition text, language text
    );
    CREATE UNIQUE INDEX chapters_work_canonical_chapter_key_uq
      ON chapters(work_id,canonical_chapter_key) WHERE canonical_chapter_key IS NOT NULL;
    CREATE TABLE importer_work_mappings (
      id uuid PRIMARY KEY, source text NOT NULL, source_work_id text NOT NULL, work_id uuid NOT NULL,
      source_slug text, source_title text, sync_status text, metadata jsonb, confidence_score numeric,
      is_primary boolean, match_method text, last_synced_at timestamptz, created_at timestamptz, updated_at timestamptz,
      UNIQUE(source,source_work_id)
    );
    CREATE TABLE importer_chapter_mappings (
      id uuid PRIMARY KEY, source text NOT NULL, source_chapter_id text NOT NULL, chapter_id uuid,
      work_id uuid, work_mapping_id uuid, chapter_number numeric, chapter_sort_key numeric, page_count integer,
      is_page_provider boolean, status text, is_gap boolean, last_error text, created_at timestamptz, updated_at timestamptz,
      UNIQUE(source,source_chapter_id)
    );
    CREATE TABLE importer_queue (
      id uuid PRIMARY KEY, status text, locked_by text, locked_at timestamptz, lease_expires_at timestamptz,
      last_error text, progress_current integer, progress_total integer, progress_stage text, updated_at timestamptz
    );
    CREATE TABLE media (
      id uuid PRIMARY KEY, provider text, provider_key text, mime text, width integer, height integer, bytes bigint,
      sha256 text, created_by uuid, storage_ready boolean, purpose text, bot_reference text, storage_shard_id uuid,
      chapter_id uuid, created_at timestamptz
    );
    CREATE TABLE pages (
      chapter_id uuid NOT NULL, position integer NOT NULL, media_id uuid NOT NULL, width integer, height integer,
      UNIQUE(chapter_id,position)
    );
    INSERT INTO works(id,slug,title) VALUES('${workId}','canonical-test','Canonical Test');
  `);
  setYugabytePoolForTest({
    query: (text: string, values?: unknown[]) => db.query(text, values),
    connect: async () => ({
      query: (text: string, values?: unknown[]) => db.query(text, values),
      release: () => undefined,
    }),
    end: async () => undefined,
  } as any);
  process.env.IMPORTER_LEGACY_READER_CANONICAL_IDENTITY = '1';
});

afterEach(async () => {
  delete process.env.IMPORTER_LEGACY_READER_CANONICAL_IDENTITY;
  setYugabytePoolForTest(null);
  await db.close();
});

function legacyPayload(sourceChapterId: string) {
  return {
    work: { id: workId, slug: 'canonical-test', title: 'Canonical Test' },
    workMapping: { source: 'source-a', sourceWorkId: 'canonical-test', sourceSlug: 'canonical-test', sourceTitle: 'Canonical Test' },
    // Both inputs deliberately have the same old numeric projection.
    chapter: { number: 12.1, title: 'Chapter 12.1', source: 'source-a', sourceChapterId },
    pages: [{ position: 1, mediaId: randomUUID(), providerKey: `telegram:${sourceChapterId}`,
      mime: 'image/jpeg', width: 100, height: 200, bytes: 1000, sha256: `hash-${sourceChapterId}` }],
  };
}

describe('legacy rollback Reader identity adapter', () => {
  it('uses source chapter identity instead of numeric equality after Reader phase 2', () => {
    // The legacy payload has already collapsed 12.10 to a JS number. The
    // rollback adapter cannot invent that formatting, but it must still keep
    // independently mapped source chapters from colliding.
    const first = legacyRollbackChapterIdentity('source-a', 'chapter-12.1', 12.1);
    const second = legacyRollbackChapterIdentity('source-a', 'chapter-12.10', 12.1);
    expect(first.canonicalKey).not.toBe(second.canonicalKey);
    expect(first.orderKey).not.toBe(second.orderKey);
    expect(first.displayNumber).toBe('12.1');
  });

  it('normalizes cosmetic source whitespace without merging different source chapters', () => {
    expect(legacyRollbackChapterIdentity(' Source A ', ' chapter-12 ', 12).canonicalKey)
      .toBe(legacyRollbackChapterIdentity('Source A', 'chapter-12', 12).canonicalKey);
    expect(legacyRollbackChapterIdentity('Source A', 'chapter-12', 12).canonicalKey)
      .not.toBe(legacyRollbackChapterIdentity('Source A', 'chapter-12-special', 12).canonicalKey);
  });

  it('publishes two same-number legacy mappings safely against the phase-two Reader schema', async () => {
    const first = await publishBatchDirect(legacyPayload('chapter-12.1') as any);
    const second = await publishBatchDirect(legacyPayload('chapter-12.10') as any);
    expect(first.chapterId).not.toBe(second.chapterId);
    const chapters = await db.query(`SELECT canonical_chapter_key,display_number FROM chapters ORDER BY canonical_chapter_key`);
    expect(chapters.rows).toHaveLength(2);
    expect(chapters.rows.map((row) => row.display_number)).toEqual(['12.1', '12.1']);
    expect(chapters.rows.map((row) => row.canonical_chapter_key)).toEqual([
      'legacy-v1|source-a|chapter-12.1',
      'legacy-v1|source-a|chapter-12.10',
    ]);
  });

  it('keeps the legacy numeric upsert path unchanged until canonical rollback mode is explicitly enabled', async () => {
    delete process.env.IMPORTER_LEGACY_READER_CANONICAL_IDENTITY;
    await db.exec('CREATE UNIQUE INDEX chapters_work_id_number_key ON chapters(work_id,number)');
    const first = await publishBatchDirect(legacyPayload('chapter-12.1') as any);
    const second = await publishBatchDirect(legacyPayload('chapter-12.10') as any);
    expect(first.chapterId).toBe(second.chapterId);
    expect((await db.query('SELECT id FROM chapters')).rows).toHaveLength(1);
  });
});
