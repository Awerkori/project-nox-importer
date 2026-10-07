import pg from 'pg';
import dotenv from 'dotenv';
dotenv.config();

const client = new pg.Client({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false }
});

async function main() {
  await client.connect();

  const res = await client.query(`
    SELECT
      to_char(date_trunc('minute', published_at) AT TIME ZONE 'America/Sao_Paulo', 'HH24:MI') as minute_sp,
      COUNT(*) as count
    FROM chapters
    WHERE published_at >= NOW() - INTERVAL '15 minutes'
    GROUP BY date_trunc('minute', published_at)
    ORDER BY date_trunc('minute', published_at) ASC;
  `);

  const activeRes = await client.query(`
    SELECT count(*) as active_running
    FROM importer_queue
    WHERE status = 'IMPORTING' AND task_type = 'IMPORT_CHAPTER'
  `);

  console.log('--- ACTIVE RUNNING JOBS ---');
  console.log('IMPORT_CHAPTER running:', activeRes.rows[0].active_running);

  console.log('\n--- VISIBLE PUBLICATIONS PER MINUTE (NON-ZERO ONLY FROM QUERY) ---');
  console.table(res.rows);

  await client.end();
}

main().catch(console.error);
