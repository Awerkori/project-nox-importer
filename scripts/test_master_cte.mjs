import pg from 'pg';
import crypto from 'crypto';
import dotenv from 'dotenv';
dotenv.config({ path: '/home/awerkori/.Projects/project-nox-importer/.env' });

const pool = new pg.Pool({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false },
  max: 1
});

async function run() {
  const client = await pool.connect();
  try {
    console.log("Connected to Yugabyte. Testing FULL 2-statement chapter publication in a transaction...");
    
    // Pick an existing work id
    const wRes = await client.query("SELECT id FROM works LIMIT 1;");
    const workId = wRes.rows[0].id;
    const testChId = crypto.randomUUID();
    const testWmId = crypto.randomUUID();
    const testCmId = crypto.randomUUID();
    const nowIso = new Date().toISOString();

    // Prepare 30 pages
    const numPages = 30;
    const mediaIds = [];
    const providerKeys = [];
    const mimes = [];
    const widths = [];
    const heights = [];
    const bytesArr = [];
    const sha256s = [];
    const botRefs = [];
    const shardIds = [];
    const positions = [];

    for (let i = 1; i <= numPages; i++) {
      const mId = crypto.randomUUID();
      mediaIds.push(mId);
      providerKeys.push(`tg_key_${i}`);
      mimes.push('image/jpeg');
      widths.push(1080);
      heights.push(1920);
      bytesArr.push(150000);
      sha256s.push(crypto.createHash('sha256').update(`test_${i}`).digest('hex'));
      botRefs.push('MANGA_STORAGE_01');
      shardIds.push('52fa1027-e95e-4c4f-94a2-944208e9a6e6');
      positions.push(i);
    }

    const tStart = Date.now();
    await client.query("BEGIN;");
    const tBegin = Date.now() - tStart;

    // Statement 1: Master Metadata CTE (Chapters, Work Mapping, Chapter Mapping, Works Update)
    const t0Master = Date.now();
    const masterRes = await client.query(`
      WITH upsert_ch AS (
        INSERT INTO chapters (
          id, work_id, number, title, published_at, origin, views_total, created_at
        ) VALUES (
          $1::uuid, $2::uuid, $3::numeric, $4::text, $5::timestamptz, 'importer', 0, $5::timestamptz
        )
        ON CONFLICT (work_id, number) DO UPDATE SET
          title = EXCLUDED.title,
          published_at = COALESCE(chapters.published_at, EXCLUDED.published_at)
        RETURNING id, published_at
      ),
      upsert_wm AS (
        INSERT INTO importer_work_mappings (
          id, source, source_work_id, work_id, source_slug, source_title,
          sync_status, metadata, confidence_score, is_primary, match_method,
          last_synced_at, created_at, updated_at
        ) VALUES (
          $6::uuid, $7::text, $8::text, $2::uuid, $9::text, $10::text, 'ACTIVE',
          $11::jsonb, $12::numeric, true, 'EXACT', $5::timestamptz, $5::timestamptz, $5::timestamptz
        )
        ON CONFLICT (source, source_work_id) DO UPDATE SET
          work_id = EXCLUDED.work_id,
          source_slug = EXCLUDED.source_slug,
          source_title = EXCLUDED.source_title,
          sync_status = 'ACTIVE',
          metadata = EXCLUDED.metadata,
          last_synced_at = EXCLUDED.last_synced_at,
          updated_at = EXCLUDED.updated_at
        RETURNING id
      ),
      upsert_cm AS (
        INSERT INTO importer_chapter_mappings (
          id, source, source_chapter_id, chapter_id, work_id, work_mapping_id,
          chapter_number, chapter_sort_key, page_count, is_page_provider, status, is_gap,
          created_at, updated_at
        )
        SELECT
          $13::uuid, $7::text, $14::text, c.id, $2::uuid, wm.id,
          $3::numeric, $15::numeric, $16::int, true, 'COMPLETED', false,
          $5::timestamptz, $5::timestamptz
        FROM upsert_ch c, upsert_wm wm
        ON CONFLICT (source, source_chapter_id) DO UPDATE SET
          chapter_id = EXCLUDED.chapter_id,
          work_id = EXCLUDED.work_id,
          work_mapping_id = EXCLUDED.work_mapping_id,
          chapter_number = EXCLUDED.chapter_number,
          chapter_sort_key = EXCLUDED.chapter_sort_key,
          page_count = EXCLUDED.page_count,
          is_page_provider = EXCLUDED.is_page_provider,
          status = 'COMPLETED',
          is_gap = false,
          last_error = NULL,
          updated_at = EXCLUDED.updated_at
        RETURNING id
      ),
      upd_work AS (
        UPDATE works
        SET latest_chapter_published_at = CASE 
              WHEN latest_chapter_published_at IS NULL THEN (SELECT published_at FROM upsert_ch)
              ELSE GREATEST(latest_chapter_published_at, (SELECT published_at FROM upsert_ch))
            END,
            updated_at = NOW()
        WHERE id = $2::uuid
        RETURNING id
      )
      SELECT c.id as chapter_id, c.published_at, wm.id as work_mapping_id
      FROM upsert_ch c, upsert_wm wm;
    `, [
      testChId, workId, 99999, 'Test Chapter 99999', nowIso,
      testWmId, 'test_source', 'test_swid_1', 'test_slug', 'Test Title',
      '{}', 1.0,
      testCmId, 'test_scid_1', 99999.0, numPages
    ]);
    const tMaster = Date.now() - t0Master;
    const resolvedChapterId = masterRes.rows[0].chapter_id;

    // Statement 2: Media Batch Upsert + Pages Overwrite CTE
    const t0MediaPages = Date.now();
    await client.query(`
      WITH ins_media AS (
        INSERT INTO media (
          id, provider, provider_key, mime, width, height, bytes, sha256,
          created_by, storage_ready, purpose, bot_reference, storage_shard_id, chapter_id, created_at
        )
        SELECT 
          m.id, 'telegram', m.provider_key, m.mime, m.width, m.height, m.bytes, m.sha256,
          '732fbe87-5040-41fb-9983-0aedb2af44c8'::uuid, true, 'editorial', m.bot_ref, m.shard_id, $1::uuid, $2::timestamptz
        FROM UNNEST(
          $3::uuid[], $4::text[], $5::text[], $6::int[], $7::int[], $8::bigint[], $9::text[], $10::text[], $11::uuid[]
        ) AS m(id, provider_key, mime, width, height, bytes, sha256, bot_ref, shard_id)
        ON CONFLICT (id) DO UPDATE SET
          provider_key = EXCLUDED.provider_key,
          storage_ready = true,
          chapter_id = EXCLUDED.chapter_id
        RETURNING id
      ),
      del_pages AS (
        DELETE FROM pages WHERE chapter_id = $1::uuid
      )
      INSERT INTO pages (chapter_id, position, media_id, width, height)
      SELECT $1::uuid, p.pos, p.m_id, p.w, p.h
      FROM UNNEST($12::int[], $3::uuid[], $6::int[], $7::int[]) AS p(pos, m_id, w, h);
    `, [
      resolvedChapterId, nowIso,
      mediaIds, providerKeys, mimes, widths, heights, bytesArr, sha256s, botRefs, shardIds,
      positions
    ]);
    const tMediaPages = Date.now() - t0MediaPages;

    const t0Commit = Date.now();
    await client.query("ROLLBACK;"); // Roll back for test
    const tCommit = Date.now() - t0Commit;

    const totalTxTime = Date.now() - tStart;
    console.log(`TOTAL TRANSACTION DURATION: ${totalTxTime}ms!`);
    console.log(`  - BEGIN: ${tBegin}ms`);
    console.log(`  - Master Metadata CTE (4 tables): ${tMaster}ms`);
    console.log(`  - Media Batch + Pages Overwrite CTE: ${tMediaPages}ms`);
    console.log(`  - COMMIT/ROLLBACK: ${tCommit}ms`);
  } catch(e) {
    console.error("Test error:", e.message);
    await client.query("ROLLBACK;").catch(()=>{});
  } finally {
    client.release();
    await pool.end();
  }
}
run();
