import pg from 'pg';
import dotenv from 'dotenv';
import fs from 'fs';
dotenv.config();

const pool = new pg.Pool({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: {
    rejectUnauthorized: true,
    ca: fs.readFileSync('./config/root.crt').toString()
  }
});

async function main() {
  const client = await pool.connect();
  try {
    console.log("=== 1. YUGABYTE SERVERS / NODES ===");
    try {
      const servers = await client.query("SELECT * FROM yb_servers()");
      console.log(JSON.stringify(servers.rows, null, 2));
    } catch (e) {
      console.log("yb_servers error:", e.message);
    }

    console.log("\n=== 2. TOTAL & ACTIVE CONNECTIONS BY CLIENT ===");
    const conns = await client.query(`
      SELECT 
        COALESCE(client_addr::text, 'local') as client,
        usename,
        state,
        count(*) as count
      FROM pg_stat_activity
      GROUP BY client_addr, usename, state
      ORDER BY count DESC;
    `);
    console.log(JSON.stringify(conns.rows, null, 2));

    console.log("\n=== 3. ACTIVE RUNNING QUERIES (NON-IDLE) ===");
    const act = await client.query(`
      SELECT 
        pid,
        usename,
        client_addr,
        state,
        wait_event_type,
        wait_event,
        query_start,
        now() - query_start as duration,
        left(query, 160) as short_query
      FROM pg_stat_activity
      WHERE state != 'idle' AND pid != pg_backend_pid()
      ORDER BY query_start ASC;
    `);
    console.log(JSON.stringify(act.rows, null, 2));

    console.log("\n=== 4. PG_STAT_STATEMENTS TOP QUERIES BY TOTAL/MEAN TIME ===");
    try {
      const stats = await client.query(`
        SELECT 
          calls,
          round(total_exec_time::numeric, 2) as total_ms,
          round(mean_exec_time::numeric, 2) as mean_ms,
          round(max_exec_time::numeric, 2) as max_ms,
          rows,
          left(query, 140) as query_prefix
        FROM pg_stat_statements
        ORDER BY total_exec_time DESC
        LIMIT 15;
      `);
      console.log(JSON.stringify(stats.rows, null, 2));
    } catch (e) {
      console.log("pg_stat_statements error:", e.message);
    }

  } finally {
    client.release();
    await pool.end();
  }
}

main().catch(console.error);
