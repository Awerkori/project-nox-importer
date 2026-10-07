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

const works = [
  { id: "6ae9ce62-4aed-4d54-89cb-f94595f278b0", title: "Standard of Reincarnation", min_staged: 135 },
  { id: "eb5d1543-beb3-46be-bc2c-d513fcfba60f", title: "Mestre Supremo de Artes Marciais", min_staged: 311 },
  { id: "d8b402cd-b42a-4e08-98d4-1eace5976642", title: "Berserk", min_staged: 101 },
  { id: "34fd58db-df00-4a65-a470-0f6d40005fd5", title: "Desire BL's Ex Sponsor (Novel)", min_staged: 155 },
  { id: "8c870b74-8bd3-448e-96b4-5e28e7626e2e", title: "Eu sou o Vilão Predestinado", min_staged: 77 },
  { id: "10d6e5c7-e0df-4d25-8500-6240577d42c9", title: "Slime Life", min_staged: 19 },
  { id: "7eb3e4e4-32be-4fb3-9ffd-4b1206701dd5", title: "Pico Marcial", min_staged: 269 }
];

for (const w of works) {
  // Find highest published chapter before min_staged
  const pub = await client.query(`
    SELECT number, published_at FROM chapters 
    WHERE work_id = $1 AND number::numeric < $2
    ORDER BY number::numeric DESC LIMIT 1;
  `, [w.id, w.min_staged]);

  const lastPub = pub.rows[0]?.number || 'NONE';

  // Find mapping between lastPub and min_staged
  const missing = await client.query(`
    SELECT id, source, source_chapter_id, chapter_number, status, is_gap
    FROM importer_chapter_mappings
    WHERE work_id = $1 AND chapter_number::numeric < $2
      ${lastPub !== 'NONE' ? `AND chapter_number::numeric > ${lastPub}` : ''}
    ORDER BY chapter_number::numeric ASC;
  `, [w.id, w.min_staged]);

  console.log(`Work: ${w.title} (${w.id})`);
  console.log(`  Last Published before ${w.min_staged}: ${lastPub}`);
  console.log(`  Missing mappings in between:`, missing.rows);
}

await client.end();
