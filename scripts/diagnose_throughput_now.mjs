import fs from 'node:fs';
import pg from 'pg';
import dotenv from 'dotenv';

const envFile = '/home/awerkori/.Projects/project-nox-importer/.env';
const env = dotenv.parse(fs.readFileSync(envFile));

const pool = new pg.Pool({
  host: env.YUGABYTE_HOST, port: Number(env.YUGABYTE_PORT || 5433), user: env.YUGABYTE_USER,
  password: env.YUGABYTE_PASSWORD, database: env.YUGABYTE_DATABASE,
  ssl: {rejectUnauthorized:true,ca:fs.readFileSync(env.YUGABYTE_SSL_CERT)},
  max:1, connectionTimeoutMillis:5000, query_timeout:7000
});

async function main() {
  const windows = [1, 5, 10, 30, 60];
  for (const m of windows) {
    const res = await pool.query(`
      SELECT count(*) as count
      FROM chapters
      WHERE published_at >= NOW() - interval '${m} minutes'
    `);
    const count = parseInt(res.rows[0].count, 10);
    console.log(`${m}m window: ${count} chapters (${(count / m).toFixed(2)} cap/min)`);
  }
  pool.end();
}
main().catch(console.error);
