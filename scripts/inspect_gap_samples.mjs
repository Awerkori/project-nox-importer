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
  const sample = [
    { title: 'Wind Breaker', id: 'f87a32b6-4554-474d-9657-fc712c7594d4', max_pub: 31, missing: 32 },
    { title: 'Attack on Titan', id: '2c5a1a1f-4f51-4045-885e-e47bbda5f5c0', max_pub: 42, missing: 43 },
    { title: 'Solo Leveling', id: '0032b49c-f1bc-4e20-94cb-551737e42f9e', max_pub: 58, missing: 59 },
    { title: 'Love Hina', id: '857b2ffc-1349-43c2-a40c-26d11cbfe7ea', max_pub: 47, missing: 48 },
    { title: 'Crepúsculo', id: 'e4bbd050-482d-450f-90ea-a4f6bbd6bc2e', max_pub: 124, missing: 125 }
  ];

  for (const s of sample) {
    console.log(`\n=== Checking ${s.title} (missing ch ${s.missing}) ===`);
    const m = await pool.query(
      `SELECT id, chapter_number, chapter_sort_key, status, source, is_gap, last_error
       FROM importer_chapter_mappings
       WHERE work_id = $1 AND chapter_sort_key = $2`,
      [s.id, s.missing]
    );
    console.log('Mapping:', m.rows);

    const q = await pool.query(
      `SELECT id, status, task_type, source, chapter_sort_key, attempts, last_error, locked_by
       FROM importer_queue
       WHERE (payload->>'workId') = $1 AND chapter_sort_key = $2`,
      [s.id, s.missing]
    );
    console.log('Queue:', q.rows);
  }
  await pool.end();
}

run().catch(console.error);
