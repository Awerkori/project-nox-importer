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

async function main() {
  const fnRes = await pool.query("SELECT pg_get_functiondef(oid) FROM pg_proc WHERE proname = 'importer_replace_pages'");
  console.log("=== FUNCTION importer_replace_pages ===");
  console.log(fnRes.rows[0]?.pg_get_functiondef);

  console.log("\n=== RECENT JOBS WITH TELEMETRY IN importer_queue ===");
  const jobs = await pool.query(`
    SELECT id, task_type, status, source, duration_ms,
           payload->'telemetry' as telemetry,
           payload->>'workId' as work_id
    FROM importer_queue
    WHERE task_type = 'IMPORT_CHAPTER' AND status = 'COMPLETED'
    ORDER BY updated_at DESC
    LIMIT 5;
  `);
  for (const j of jobs.rows) {
    console.log(`Job ${j.id} (${j.source}) work=${j.work_id} duration=${j.duration_ms}ms:`, JSON.stringify(j.telemetry, null, 2));
  }

  await pool.end();
}

main().catch(console.error);
