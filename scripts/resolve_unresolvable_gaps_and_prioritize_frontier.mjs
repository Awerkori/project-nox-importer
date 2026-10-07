import pg from 'pg';
import dotenv from 'dotenv';
dotenv.config({ path: '/home/awerkori/.Projects/project-nox-importer/.env' });

const client = new pg.Client({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false }
});

async function run() {
  await client.connect();

  console.log("=== 1. RESOLVING UNRESOLVABLE GAPS FOR GÊNIO DO TELETRANSPORTE ===");
  const genioId = '23354114-053e-4d2b-98f3-31cbc0744161';
  // Check work mapping id
  const wmRes = await client.query("SELECT id, source FROM importer_work_mappings WHERE work_id = $1 LIMIT 1", [genioId]);
  const wm = wmRes.rows[0];
  if (wm) {
    // Missing chapters 2, 3, 4, 18
    const missingGaps = [2, 3, 4, 18];
    for (const g of missingGaps) {
      await client.query(`
        INSERT INTO importer_chapter_mappings (
          source, source_chapter_id, work_id, work_mapping_id, chapter_number, chapter_sort_key,
          page_count, is_page_provider, status, is_gap, updated_at
        ) VALUES (
          $1, $2, $3, $4, $5, $5, 0, false, 'COMPLETED', true, NOW()
        )
        ON CONFLICT (source, source_chapter_id) DO UPDATE
        SET is_gap = true, status = 'COMPLETED', updated_at = NOW();
      `, [wm.source, `gap_${genioId}_${g}`, genioId, wm.id, g]);
      console.log(`Registered explicit gap for chapter ${g}`);
    }
  }

  console.log("\n=== 2. BOOSTING FRONTIER FOR ME TORNEI A FAMÍLIA DO VILÃO ===");
  const vilaoId = 'bf4859ca-69eb-4615-8317-7c2c5ec91a96';
  const boostRes = await client.query(`
    UPDATE importer_queue
    SET priority = 100, attempts = 0, next_run_at = NOW()
    WHERE (payload->>'workId') = $1
      AND chapter_sort_key = 57
      AND status IN ('QUEUED', 'RETRY')
    RETURNING id, source, chapter_sort_key, priority;
  `, [vilaoId]);
  console.log("Boosted cap 57 jobs:", boostRes.rows);

  console.log("\n=== 3. AUDITING OTHER STAGED WORKS FOR GAPS ===");
  const topStaged = await client.query(`
    SELECT m.work_id, w.title, count(*) as staged_cnt, min(m.chapter_number) as min_staged
    FROM importer_chapter_mappings m
    JOIN works w ON w.id = m.work_id
    WHERE m.status = 'STAGED'
    GROUP BY m.work_id, w.title
    HAVING count(*) >= 3
    ORDER BY staged_cnt DESC;
  `);
  console.table(topStaged.rows);

  await client.end();
}
run().catch(console.error);
