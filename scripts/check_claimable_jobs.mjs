import pg from "pg";
import fs from "fs";

const envVars = Object.fromEntries(
  fs.readFileSync("/home/awerkori/.config/project-nox/yugabyte.env", "utf8")
    .split("\n")
    .filter(l => l.includes("=") && !l.startsWith("#"))
    .map(l => [l.split("=")[0].trim(), l.substring(l.indexOf("=") + 1).trim().replace(/^['"]|['"]$/g, "")])
);

const pool = new pg.Pool({
  host: envVars.YUGABYTE_HOST,
  port: parseInt(envVars.YUGABYTE_PORT || "5433", 10),
  user: envVars.YUGABYTE_USER,
  password: envVars.YUGABYTE_PASSWORD,
  database: envVars.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false },
  max: 2,
  connectionTimeoutMillis: 10000
});

async function run() {
  const claimable = await pool.query(`
    SELECT task_type, priority, count(*)
    FROM importer_queue
    WHERE status IN ('QUEUED', 'RETRY')
      AND (next_run_at IS NULL OR next_run_at <= NOW())
    GROUP BY task_type, priority
    ORDER BY priority DESC, count(*) DESC;
  `);
  console.log("Claimable jobs by type and priority:", claimable.rows);

  const topClaimable = await pool.query(`
    SELECT q.id, q.task_type, q.priority, q.source, q.chapter_sort_key, q.attempts, q.payload->>'workId' as work_id, w.title
    FROM importer_queue q
    LEFT JOIN works w ON (q.payload->>'workId')::uuid = w.id
    WHERE q.status IN ('QUEUED', 'RETRY')
      AND (q.next_run_at IS NULL OR q.next_run_at <= NOW())
    ORDER BY q.priority DESC, q.created_at ASC
    LIMIT 15;
  `);
  console.log("Top claimable jobs:", topClaimable.rows);

  await pool.end();
}

run().catch(console.error);
