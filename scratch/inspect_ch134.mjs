import dotenv from "dotenv";
import pg from "pg";
dotenv.config({ path: "/home/awerkori/.Projects/project-nox-importer/.env" });
const client = new pg.Client({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || "5433", 10),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false }
});
await client.connect();

const chRes = await client.query(`
  SELECT id, work_id, chapter_number, status, updated_at
  FROM importer_chapter_mappings
  WHERE work_id = '6ae9ce62-4aed-4d54-89cb-f94595f278b0' AND chapter_number IN ('133.00', '134.00', '135.00')
  ORDER BY chapter_number;
`);
console.log("Mappings 133-135:", JSON.stringify(chRes.rows, null, 2));

const qRes = await client.query(`
  SELECT id, work_id, chapter_mapping_id, chapter_number, status, priority, attempts, error_message, updated_at
  FROM importer_queue
  WHERE work_id = '6ae9ce62-4aed-4d54-89cb-f94595f278b0' AND chapter_number = '134.00';
`).catch(async () => {
  return await client.query(`
    SELECT *
    FROM importer_queue
    WHERE work_id = '6ae9ce62-4aed-4d54-89cb-f94595f278b0'
    LIMIT 2;
  `);
});
console.log("Queue for work:", JSON.stringify(qRes.rows, null, 2));

// Also check chapters table for work
const publishedRes = await client.query(`
  SELECT id, work_id, number, title, published_at
  FROM chapters
  WHERE work_id = '6ae9ce62-4aed-4d54-89cb-f94595f278b0'
  ORDER BY number::numeric DESC
  LIMIT 5;
`);
console.log("Latest published chapters for Standard of Reincarnation:", JSON.stringify(publishedRes.rows, null, 2));

await client.end();
