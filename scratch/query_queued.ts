import { Pool } from 'pg';
import * as dotenv from 'dotenv';
dotenv.config();

async function main() {
  const pool = new Pool({
    host: process.env.YUGABYTE_HOST,
    port: parseInt(process.env.YUGABYTE_PORT || '5433'),
    user: process.env.YUGABYTE_USER,
    password: process.env.YUGABYTE_PASSWORD,
    database: process.env.YUGABYTE_DATABASE,
    ssl: { rejectUnauthorized: false }
  });

  const res = await pool.query(`
    SELECT work_id, source, min_sort, max_sort, queued_count,
           (SELECT max(chapter_sort_key) FROM importer_chapter_mappings m WHERE m.work_id::text = q.work_id AND m.status = 'COMPLETED') as max_pub
    FROM (
      SELECT (payload->>'workId') as work_id, source, min(chapter_sort_key) as min_sort, max(chapter_sort_key) as max_sort, count(*) as queued_count
      FROM importer_queue 
      WHERE status IN ('QUEUED') AND task_type = 'IMPORT_CHAPTER' 
      GROUP BY payload->>'workId', source
    ) q
    LIMIT 10
  `);
  console.table(res.rows);

  await pool.end();
}

main().catch(console.error).then(() => process.exit(0));
