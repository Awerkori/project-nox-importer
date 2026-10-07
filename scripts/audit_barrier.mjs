import pg from 'pg';
import fs from 'fs';

const env = Object.fromEntries(
  fs.readFileSync('/home/awerkori/.config/project-nox/yugabyte.env', 'utf8')
    .split('\n')
    .map(l => l.trim())
    .filter(l => l && !l.startsWith('#'))
    .map(l => {
      const idx = l.indexOf('=');
      let v = l.slice(idx + 1).trim();
      if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) v = v.slice(1, -1);
      return [l.slice(0, idx).trim(), v];
    })
);

const pool = new pg.Pool({
  host: env.YUGABYTE_HOST,
  port: parseInt(env.YUGABYTE_PORT || '5433'),
  user: env.YUGABYTE_USER,
  password: env.YUGABYTE_PASSWORD,
  database: env.YUGABYTE_DATABASE,
  ssl: { ca: fs.readFileSync('/home/awerkori/.config/project-nox/root.crt', 'utf8'), rejectUnauthorized: true },
  max: 1,
  application_name: 'barrier-audit-script'
});

async function run() {
  const client = await pool.connect();
  try {
    const workId = '122b1026-0f0b-45c5-b8af-ed88a3ea2ce7';
    console.log('=== AUDITING WORK 122b1026-0f0b-45c5-b8af-ed88a3ea2ce7 (Imperador Demoníaco) ===');

    const workRes = await client.query('SELECT id, title, slug FROM works WHERE id = $1', [workId]);
    console.log('Work:', workRes.rows[0]);

    const pubRes = await client.query(
      'SELECT COUNT(*) as published_count, MIN(number) as min_pub, MAX(number) as max_pub FROM chapters WHERE work_id = $1 AND published_at IS NOT NULL',
      [workId]
    );
    console.log('Published in chapters table:', pubRes.rows[0]);

    const mapStats = await client.query(
      'SELECT source, status, COUNT(*) as cnt FROM importer_chapter_mappings WHERE work_id = $1 GROUP BY source, status ORDER BY source, status',
      [workId]
    );
    console.log('Mappings by source & status:');
    console.table(mapStats.rows);

    const precRes = await client.query(
      `SELECT chapter_sort_key, source, status, is_gap
       FROM importer_chapter_mappings
       WHERE work_id = $1
         AND chapter_sort_key < 252
         AND status != 'COMPLETED'
         AND is_gap IS NOT TRUE
       ORDER BY chapter_sort_key ASC`,
      [workId]
    );
    console.log('Current barrier blocker query count for ch 252:', precRes.rowCount);
    console.log('First 10 blocker rows from current query:');
    console.table(precRes.rows.slice(0, 10));

    // Check how many distinct chapter numbers < 252 are published in chapters table
    const pubChapters = await client.query(
      `SELECT number FROM chapters WHERE work_id = $1 AND number < 252 AND published_at IS NOT NULL ORDER BY number ASC`,
      [workId]
    );
    console.log('Published chapters count < 252:', pubChapters.rowCount);

    // Find any missing numbers between 1 and 251
    const pubNumbersSet = new Set(pubChapters.rows.map(r => parseFloat(r.number)));
    const missingBetween1and251 = [];
    for (let i = 1; i < 252; i++) {
      if (!pubNumbersSet.has(i)) {
        missingBetween1and251.push(i);
      }
    }
    console.log('Numbers between 1 and 251 TRULY missing from chapters table:', missingBetween1and251.length, missingBetween1and251);

    // Staged chapters for this work
    const stagedRes = await client.query(
      `SELECT chapter_sort_key, source, status, updated_at
       FROM importer_chapter_mappings
       WHERE work_id = $1 AND status = 'STAGED'
       ORDER BY chapter_sort_key ASC`,
      [workId]
    );
    console.log('STAGED chapters for this work:', stagedRes.rowCount);
    console.table(stagedRes.rows.slice(0, 10));

    // All staged chapters across the whole database
    console.log('\n=== GLOBAL STAGED CHAPTERS AUDIT ===');
    const globalStaged = await client.query(
      `SELECT m.id, m.work_id, w.title, m.source, m.chapter_sort_key, m.updated_at,
              EXTRACT(EPOCH FROM (NOW() - m.updated_at)) as age_sec
       FROM importer_chapter_mappings m
       JOIN works w ON w.id = m.work_id
       WHERE m.status = 'STAGED'
       ORDER BY m.updated_at ASC`
    );
    console.log('TOTAL STAGED:', globalStaged.rowCount);
    console.log('STAGED >5m:', globalStaged.rows.filter(r => r.age_sec > 300).length);
    console.log('STAGED >15m:', globalStaged.rows.filter(r => r.age_sec > 900).length);
    console.log('STAGED >60m:', globalStaged.rows.filter(r => r.age_sec > 3600).length);

    const stagedByWork = new Map();
    for (const r of globalStaged.rows) {
      if (!stagedByWork.has(r.work_id)) {
        stagedByWork.set(r.work_id, { title: r.title, count: 0, sources: new Set(), minCh: r.chapter_sort_key, maxCh: r.chapter_sort_key });
      }
      const entry = stagedByWork.get(r.work_id);
      entry.count++;
      entry.sources.add(r.source);
      if (parseFloat(r.chapter_sort_key) < parseFloat(entry.minCh)) entry.minCh = r.chapter_sort_key;
      if (parseFloat(r.chapter_sort_key) > parseFloat(entry.maxCh)) entry.maxCh = r.chapter_sort_key;
    }

    console.log('WORKS WITH STAGED CHAPTERS:', stagedByWork.size);
    for (const [wid, info] of stagedByWork.entries()) {
      console.log(` - [${wid}] "${info.title}": ${info.count} staged chapters (${info.minCh}..${info.maxCh}), sources: ${Array.from(info.sources).join(', ')}`);
    }

    // Check publications in last 5 minutes
    console.log('\n=== LAST 5 MINUTES PROGRESS ===');
    const pub5m = await client.query(
      `SELECT COUNT(*) as cnt FROM chapters WHERE published_at > NOW() - INTERVAL '5 minutes'`
    );
    const staged5m = await client.query(
      `SELECT COUNT(*) as cnt FROM importer_chapter_mappings WHERE status = 'STAGED' AND updated_at > NOW() - INTERVAL '5 minutes'`
    );
    const completed5m = await client.query(
      `SELECT COUNT(*) as cnt FROM importer_chapter_mappings WHERE status = 'COMPLETED' AND updated_at > NOW() - INTERVAL '5 minutes'`
    );
    console.log('CHAPTERS_PUBLISHED_LAST_5M:', pub5m.rows[0].cnt);
    console.log('CHAPTERS_STAGED_LAST_5M:', staged5m.rows[0].cnt);
    console.log('CHAPTERS_COMPLETED_LAST_5M:', completed5m.rows[0].cnt);

  } finally {
    client.release();
    await pool.end();
  }
}

run().catch(console.error);
