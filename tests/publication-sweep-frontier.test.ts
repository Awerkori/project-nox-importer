import { describe, expect, it } from 'vitest';
import { PGlite } from '@electric-sql/pglite';
import { PUBLISHABLE_STAGED_WORKS_QUERY } from '../src/core/publication.js';

describe('staged publication sweep frontier selection', () => {
  it('does not let a blocked prefix hide a later publishable frontier', async () => {
    const db = new PGlite();
    await db.exec(`
      CREATE TABLE importer_chapter_mappings (
        work_id uuid NOT NULL,
        chapter_sort_key numeric NOT NULL,
        status text NOT NULL,
        is_gap boolean DEFAULT false
      );
      CREATE TABLE chapters (
        work_id uuid NOT NULL,
        number numeric NOT NULL,
        published_at timestamptz
      );
      CREATE TABLE importer_queue (
        payload jsonb NOT NULL,
        task_type text NOT NULL,
        status text NOT NULL,
        chapter_sort_key numeric
      );
    `);

    // Forty old frontiers are blocked by an unresolved step (0 -> 1.5).
    for (let i = 1; i <= 40; i += 1) {
      const workId = `00000000-0000-0000-0000-${String(i).padStart(12, '0')}`;
      await db.query(`INSERT INTO chapters(work_id, number, published_at) VALUES ($1, 0, NOW())`, [workId]);
      await db.query(
        `INSERT INTO importer_chapter_mappings(work_id, chapter_sort_key, status)
         VALUES ($1, 1.5, 'STAGED')`,
        [workId],
      );
    }

    const publishableWork = '00000000-0000-0000-0000-000000000100';
    await db.query(`INSERT INTO chapters(work_id, number, published_at) VALUES ($1, 1, NOW())`, [publishableWork]);
    await db.query(
      `INSERT INTO importer_chapter_mappings(work_id, chapter_sort_key, status)
       VALUES ($1, 2, 'STAGED')`,
      [publishableWork],
    );

    const result = await db.query(PUBLISHABLE_STAGED_WORKS_QUERY);
    expect(result.rows.map((row: any) => row.work_id)).toContain(publishableWork);
    await db.close();
  });
});
