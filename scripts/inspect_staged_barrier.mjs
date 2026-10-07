import pg from 'pg';
import fs from 'fs';

const { Pool } = pg;
const envPath = '/home/awerkori/.config/project-nox/yugabyte.env';
let envVars = {};
if (fs.existsSync(envPath)) {
  const content = fs.readFileSync(envPath, 'utf8');
  for (const line of content.split('\n')) {
    const trimmed = line.trim();
    if (trimmed && !trimmed.startsWith('#') && trimmed.includes('=')) {
      const idx = trimmed.indexOf('=');
      const k = trimmed.substring(0, idx).trim();
      let v = trimmed.substring(idx + 1).trim();
      if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) {
        v = v.substring(1, v.length - 1);
      }
      envVars[k] = v;
    }
  }
}

const pool = new Pool({
  host: envVars.YUGABYTE_HOST || process.env.YUGABYTE_HOST,
  port: parseInt(envVars.YUGABYTE_PORT || process.env.YUGABYTE_PORT || '5433', 10),
  user: envVars.YUGABYTE_USER || process.env.YUGABYTE_USER,
  password: envVars.YUGABYTE_PASSWORD || process.env.YUGABYTE_PASSWORD,
  database: envVars.YUGABYTE_DATABASE || process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false },
  max: 2,
  connectionTimeoutMillis: 10000,
  statement_timeout: 15000
});

async function run() {
  try {
    const res = await pool.query(`
      SELECT 
        m.work_id, 
        w.title,
        w.published as work_published,
        count(*) as staged_chapters,
        min(m.chapter_number) as min_num,
        max(m.chapter_number) as max_num
      FROM importer_chapter_mappings m
      LEFT JOIN works w ON m.work_id = w.id
      WHERE m.status = 'STAGED'
      GROUP BY m.work_id, w.title, w.published
      ORDER BY staged_chapters DESC
      LIMIT 15;
    `);

    console.log('Top works with STAGED chapters:');
    for (const r of res.rows) {
      console.log(`- "${r.title || 'UNKNOWN'}" (${r.work_id}): ${r.staged_chapters} staged chs (nums ${r.min_num}..${r.max_num}) | work_published=${r.work_published}`);
    }

    if (res.rows.length > 0) {
      const sampleWorkId = res.rows[0].work_id;
      console.log(`\nChecking barrier for top work: ${res.rows[0].title} (${sampleWorkId})`);
      const sampleCh = await pool.query(`
        SELECT chapter_id, chapter_number, chapter_sort_key
        FROM importer_chapter_mappings
        WHERE work_id = $1 AND status = 'STAGED'
        ORDER BY chapter_sort_key ASC
        LIMIT 3;
      `, [sampleWorkId]);

      for (const ch of sampleCh.rows) {
        const barrier = await pool.query(`
          SELECT * FROM importer_check_publication_barrier($1::uuid, $2::numeric);
        `, [sampleWorkId, ch.chapter_sort_key]);
        console.log(`  Ch ${ch.chapter_number} (sort ${ch.chapter_sort_key}):`, barrier.rows);
      }
    }
  } finally {
    await pool.end();
  }
}

run().catch(console.error);
