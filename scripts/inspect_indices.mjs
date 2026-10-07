import fs from 'node:fs';
import pg from 'pg';
import dotenv from 'dotenv';
const envFile = '/home/awerkori/.Projects/project-nox-importer/.env';
const env = dotenv.parse(fs.readFileSync(envFile));
const pool = new pg.Pool({
  host: env.YUGABYTE_HOST, port: Number(env.YUGABYTE_PORT || 5433), user: env.YUGABYTE_USER,
  password: env.YUGABYTE_PASSWORD, database: env.YUGABYTE_DATABASE,
  ssl: {rejectUnauthorized:true,ca:fs.readFileSync(env.YUGABYTE_SSL_CERT)}
});
async function main() {
  const res = await pool.query(`SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'importer_queue'`);
  console.log(res.rows.map(r => r.indexdef).join('\n'));
  pool.end();
}
main().catch(console.error);
