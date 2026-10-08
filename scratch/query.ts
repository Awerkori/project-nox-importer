import { Pool } from 'pg';
const pool = new Pool({ connectionString: process.env.DATABASE_URL || 'postgres://postgres:postgres@localhost:5432/nox' });

async function run() {
  const res = await pool.query(`
    SELECT q.source, s.status, s.blocked_reason, COUNT(*) as cnt
    FROM importer_queue q
    LEFT JOIN importer_sources s ON q.source = s.id
    WHERE q.status IN ('QUEUED', 'RETRY') 
      AND (q.next_run_at IS NULL OR q.next_run_at <= NOW())
      AND q.task_type = 'IMPORT_CHAPTER'
    GROUP BY 1, 2, 3
    ORDER BY cnt DESC
  `);
  console.log(res.rows);
  await pool.end();
}
run().catch(console.error);
