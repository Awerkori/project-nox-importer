import dotenv from 'dotenv';
import pg from 'pg';

dotenv.config({ path: '/home/awerkori/.Projects/project-nox-importer/.env' });

const { Client } = pg;

async function run() {
  const client = new Client({
    host: process.env.YUGABYTE_HOST,
    port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
    user: process.env.YUGABYTE_USER,
    password: process.env.YUGABYTE_PASSWORD,
    database: process.env.YUGABYTE_DATABASE,
    ssl: { rejectUnauthorized: false }
  });

  await client.connect();

  const canonicalId = '4b1f452c-b223-404c-83f4-e0d626563397'; // Imperador Mágico
  const duplicateId = '122b1026-0f0b-45c5-b8af-ed88a3ea2ce7'; // Imperador Demoníaco

  const [wCanon, wDup] = await Promise.all([
    client.query('SELECT * FROM works WHERE id = $1', [canonicalId]),
    client.query('SELECT * FROM works WHERE id = $1', [duplicateId])
  ]);

  const [mapCanon, mapDup] = await Promise.all([
    client.query('SELECT * FROM importer_work_mappings WHERE work_id = $1', [canonicalId]),
    client.query('SELECT * FROM importer_work_mappings WHERE work_id = $1', [duplicateId])
  ]);

  const [chCanon, chDup] = await Promise.all([
    client.query('SELECT id, number, published_at, origin FROM chapters WHERE work_id = $1 ORDER BY number::numeric ASC', [canonicalId]),
    client.query('SELECT id, number, published_at, origin FROM chapters WHERE work_id = $1 ORDER BY number::numeric ASC', [duplicateId])
  ]);

  // User data
  const getUserData = async (workId) => {
    const data = {};
    for (const tbl of ['user_library', 'reading_history', 'comments', 'work_views', 'ratings', 'work_ratings']) {
      try {
        const res = await client.query(`SELECT count(*)::int as c FROM ${tbl} WHERE work_id = $1`, [workId]);
        data[tbl] = res.rows[0].c;
      } catch (e) {
        data[tbl] = `ERR: ${e.message.split('\n')[0]}`;
      }
    }
    return data;
  };

  const [userCanon, userDup] = await Promise.all([
    getUserData(canonicalId),
    getUserData(duplicateId)
  ]);

  const canonNumbers = new Set(chCanon.rows.map(r => r.number));
  const dupNumbers = new Set(chDup.rows.map(r => r.number));

  const overlapping = chDup.rows.filter(r => canonNumbers.has(r.number));
  const uniqueOnDup = chDup.rows.filter(r => !canonNumbers.has(r.number));
  const uniqueOnCanon = chCanon.rows.filter(r => !dupNumbers.has(r.number));

  console.log('=== AUDITORIA COMPLETA DE DUPLICATA ===\n');
  console.log(`CANONICAL WORK ID: ${canonicalId}`);
  console.log(`DUPLICATE WORK ID: ${duplicateId}`);
  console.log(`CANONICAL TITLE: "${wCanon.rows[0]?.title}"`);
  console.log(`DUPLICATE TITLE: "${wDup.rows[0]?.title}"`);
  console.log(`CANONICAL SLUG: ${wCanon.rows[0]?.slug}`);
  console.log(`DUPLICATE SLUG: ${wDup.rows[0]?.slug}`);
  console.log(`CANONICAL COVER: ${wCanon.rows[0]?.cover_id}`);
  console.log(`DUPLICATE COVER: ${wDup.rows[0]?.cover_id}`);
  console.log(`CANONICAL CREATED_AT: ${wCanon.rows[0]?.created_at}`);
  console.log(`DUPLICATE CREATED_AT: ${wDup.rows[0]?.created_at}`);
  
  console.log('\n--- SOURCES & MAPPINGS ---');
  console.log('CANONICAL SOURCES:', mapCanon.rows.map(m => ({ source: m.source, source_work_id: m.source_work_id, source_title: m.source_title })));
  console.log('DUPLICATE SOURCES:', mapDup.rows.map(m => ({ source: m.source, source_work_id: m.source_work_id, source_title: m.source_title })));

  console.log('\n--- CHAPTER AUDIT ---');
  console.log(`CANONICAL CHAPTER COUNT: ${chCanon.rows.length}`);
  console.log(`DUPLICATE CHAPTER COUNT: ${chDup.rows.length}`);
  console.log(`CANONICAL MIN/MAX: ${chCanon.rows[0]?.number} .. ${chCanon.rows[chCanon.rows.length - 1]?.number}`);
  console.log(`DUPLICATE MIN/MAX: ${chDup.rows[0]?.number} .. ${chDup.rows[chDup.rows.length - 1]?.number}`);
  console.log(`OVERLAPPING CHAPTERS COUNT: ${overlapping.length}`);
  console.log(`UNIQUE ON DUPLICATE COUNT: ${uniqueOnDup.length}`);
  if (uniqueOnDup.length > 0) {
    console.log(`Unique chapters on duplicate:`, uniqueOnDup.map(c => c.number));
  }
  console.log(`UNIQUE ON CANONICAL COUNT: ${uniqueOnCanon.length}`);

  console.log('\n--- USER DATA ---');
  console.log('CANONICAL USER DATA:', userCanon);
  console.log('DUPLICATE USER DATA:', userDup);

  await client.end();
}

run().catch(console.error);
