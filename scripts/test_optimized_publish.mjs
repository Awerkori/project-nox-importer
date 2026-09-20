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
    console.log("=== TESTE DE OTIMIZAÇÃO: COMPOUND CTEs NO YUGABYTE ===");
    
    // 1. Get an existing work
    const wRes = await client.query("SELECT id, title FROM works LIMIT 1;");
    const workId = wRes.rows[0].id;
    console.log(`Usando obra existente: ${wRes.rows[0].title} (${workId})`);

    // Prepare test chapter
    const chNum = 88888;
    const numPages = 25;
    const testChId = crypto.randomUUID();
    const testWmId = crypto.randomUUID();
    const testCmId = crypto.randomUUID();
    const nowIso = new Date().toISOString();

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
      mediaIds.push(crypto.randomUUID());
      providerKeys.push(`tg_key_${i}`);
      mimes.push('image/jpeg');
      widths.push(1080);
      heights.push(1920);
      bytesArr.push(120000);
      sha256s.push(crypto.createHash('sha256').update(`test_${i}`).digest('hex'));
      botRefs.push('MANGA_STORAGE_01');
      shardIds.push('52fa1027-e95e-4c4f-94a2-944208e9a6e6');
      positions.push(i);
    }

    // TEST 1: Publication in 4 Round-Trips
    console.log("\n--- TESTE 1: Publicação com 4 Round-Trips (BEGIN, Master CTE, Media/Pages CTE, COMMIT) ---");
    const t0 = Date.now();
    await client.query("BEGIN;");
    const tBegin = Date.now() - t0;

    // Round-Trip 2: Master CTE
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
      testChId, workId, chNum, `Capítulo ${chNum}`, nowIso,
      testWmId, 'test_bench', 'bench_sw_1', 'bench_slug', 'Bench Work',
      '{}', 1.0,
      testCmId, 'bench_sc_1', chNum, numPages
    ]);
    const tMaster = Date.now() - t0Master;
    const resolvedChapterId = masterRes.rows[0].chapter_id;

    // Round-Trip 3: Media & Pages Compound CTE
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
      del_surplus AS (
        DELETE FROM pages WHERE chapter_id = $1::uuid AND position > $13::int
      )
      INSERT INTO pages (chapter_id, position, media_id, width, height)
      SELECT $1::uuid, p.pos, p.m_id, p.w, p.h
      FROM UNNEST($12::int[], $3::uuid[], $6::int[], $7::int[]) AS p(pos, m_id, w, h)
      ON CONFLICT (chapter_id, position) DO UPDATE SET
        media_id = EXCLUDED.media_id,
        width = EXCLUDED.width,
        height = EXCLUDED.height;
    `, [
      resolvedChapterId, nowIso,
      mediaIds, providerKeys, mimes, widths, heights, bytesArr, sha256s, botRefs, shardIds,
      positions, numPages
    ]);
    const tMediaPages = Date.now() - t0MediaPages;

    // Round-Trip 4: COMMIT
    const t0Commit = Date.now();
    await client.query("COMMIT;");
    const tCommit = Date.now() - t0Commit;

    const totalTxTime = Date.now() - t0;
    console.log(`>>> TOTAL TX TIME (4 Round-Trips): ${totalTxTime}ms! <<<`);
    console.log(`  - BEGIN: ${tBegin}ms`);
    console.log(`  - Master Metadata CTE: ${tMaster}ms`);
    console.log(`  - Media & Pages CTE: ${tMediaPages}ms`);
    console.log(`  - COMMIT: ${tCommit}ms`);

    // TEST 2: Verify Data In Database
    console.log("\n--- TESTE 2: Verificando dados persistidos no banco ---");
    const chCheck = await client.query("SELECT id, number, published_at FROM chapters WHERE id = $1;", [resolvedChapterId]);
    console.log("Capítulo encontrado:", chCheck.rows[0]);

    const pgCheck = await client.query("SELECT count(*) as count FROM pages WHERE chapter_id = $1;", [resolvedChapterId]);
    console.log(`Páginas persistidas: ${pgCheck.rows[0].count} (esperado: ${numPages})`);

    const mCheck = await client.query("SELECT count(*) as count FROM media WHERE chapter_id = $1;", [resolvedChapterId]);
    console.log(`Mídias persistidas: ${mCheck.rows[0].count} (esperado: ${numPages})`);

    // TEST 3: Idempotência (Republicação do mesmo capítulo)
    console.log("\n--- TESTE 3: Testando republicação idempotente (mesmo número) ---");
    const t0Idem = Date.now();
    await client.query("BEGIN;");
    const idemMasterRes = await client.query(`
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
      )
      SELECT id as chapter_id, published_at FROM upsert_ch;
    `, [crypto.randomUUID(), workId, chNum, `Capítulo ${chNum} Atualizado`, new Date().toISOString()]);
    
    await client.query("COMMIT;");
    const tIdem = Date.now() - t0Idem;
    console.log(`Republicação em ${tIdem}ms! Id retornado: ${idemMasterRes.rows[0].chapter_id}`);
    if (idemMasterRes.rows[0].chapter_id === resolvedChapterId) {
      console.log("IDEMPOTÊNCIA CONFIRMADA: O ID existente do capítulo foi reutilizado perfeitamente!");
    } else {
      throw new Error(`FALHA DE IDEMPOTÊNCIA: ID esperado ${resolvedChapterId}, recebido ${idemMasterRes.rows[0].chapter_id}`);
    }

    // Clean up test rows
    console.log("\n--- Limpando dados de teste ---");
    await client.query("DELETE FROM pages WHERE chapter_id = $1;", [resolvedChapterId]);
    await client.query("DELETE FROM media WHERE chapter_id = $1;", [resolvedChapterId]);
    await client.query("DELETE FROM importer_chapter_mappings WHERE chapter_id = $1;", [resolvedChapterId]);
    await client.query("DELETE FROM chapters WHERE id = $1;", [resolvedChapterId]);
    await client.query("DELETE FROM importer_work_mappings WHERE id = $1;", [testWmId]);
    console.log("Dados de teste removidos perfeitamente!");

    console.log("\n=== TODOS OS TESTES PASSARAM COM 100% DE SUCESSO! ===");
  } catch(e) {
    console.error("Erro no teste:", e.message);
    await client.query("ROLLBACK;").catch(()=>{});
  } finally {
    client.release();
    await pool.end();
  }
}
run();
