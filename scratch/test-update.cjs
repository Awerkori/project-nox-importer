const { Pool } = require('pg');
const pool = new Pool({ connectionString: 'postgres://postgres:postgres@localhost:5432/nox_test' });
async function main() {
  const client = await pool.connect();
  try {
    const res = await client.query(`
      CREATE TEMP TABLE test_queue (id INT, status TEXT, attempts INT);
      INSERT INTO test_queue VALUES (1, 'QUEUED', 0);
      WITH to_lock AS (SELECT id FROM test_queue LIMIT 1)
      UPDATE test_queue q_base
      SET status = 'IMPORTING', attempts = q_base.attempts + 1
      FROM to_lock
      WHERE q_base.id = to_lock.id
      RETURNING q_base.id, q_base.status, q_base.attempts;
    `);
    console.log('Result:', res.rows);
  } catch(e) {
    console.error('Error:', e);
  } finally {
    client.release();
    pool.end();
  }
}
main();
