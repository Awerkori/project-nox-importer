import pg from 'pg';
import fs from 'fs';

const { Pool } = pg;
const envVars = Object.fromEntries(
  fs.readFileSync('/home/awerkori/.config/project-nox/yugabyte.env', 'utf8')
    .split('\n')
    .filter(l => l.includes('=') && !l.startsWith('#'))
    .map(l => [l.split('=')[0].trim(), l.substring(l.indexOf('=') + 1).trim().replace(/^['"]|['"]$/g, '')])
);

const pool = new Pool({
  host: envVars.YUGABYTE_HOST,
  port: parseInt(envVars.YUGABYTE_PORT || '5433', 10),
  user: envVars.YUGABYTE_USER,
  password: envVars.YUGABYTE_PASSWORD,
  database: envVars.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false },
  max: 2,
  connectionTimeoutMillis: 10000,
  statement_timeout: 15000
});

async function main() {
  console.log('=== STEP 1: YUGABYTE & DB HEALTH ===');
  const connRes = await pool.query(`
    SELECT 
      count(*) as total_conns,
      count(*) FILTER (WHERE state = 'active') as active,
      count(*) FILTER (WHERE state = 'idle') as idle,
      count(*) FILTER (WHERE state = 'idle in transaction') as idle_in_tx
    FROM pg_stat_activity 
    WHERE datname = current_database();
  `);
  console.log('Connections:', connRes.rows[0]);

  console.log('\n=== STEP 2: IMPORTER QUEUE & JOBS ===');
  const qStats = await pool.query(`
    SELECT 
      status, 
      count(*) as count,
      count(*) FILTER (WHERE updated_at < NOW() - INTERVAL '15 minutes') as stale_count
    FROM importer_queue
    GROUP BY status
    ORDER BY count DESC;
  `);
  console.log('Queue by status:', qStats.rows);

  const importingJobs = await pool.query(`
    SELECT id, task_type, source, chapter_sort_key, attempts, locked_by, locked_at, updated_at,
           EXTRACT(EPOCH FROM (NOW() - locked_at)) as locked_seconds_ago
    FROM importer_queue
    WHERE status = 'IMPORTING'
    ORDER BY locked_at ASC;
  `);
  console.log('Currently IMPORTING jobs (count ' + importingJobs.rows.length + '):', importingJobs.rows);

  console.log('\n=== STEP 3: PUBLICATION & STAGED TIMESTAMPS ===');
  const pubTimes = await pool.query(`
    SELECT 
      MAX(created_at) as last_created,
      MAX(published_at) as last_published,
      count(*) FILTER (WHERE published_at >= NOW() - INTERVAL '15 minutes') as pub_last_15m,
      count(*) FILTER (WHERE published_at >= NOW() - INTERVAL '30 minutes') as pub_last_30m,
      count(*) FILTER (WHERE published_at >= NOW() - INTERVAL '60 minutes') as pub_last_60m,
      EXTRACT(EPOCH FROM (NOW() - MAX(published_at))) / 60 as mins_since_last_published
    FROM chapters;
  `);
  console.log('Chapters publication telemetry:', pubTimes.rows[0]);

  const mapTimes = await pool.query(`
    SELECT 
      count(*) FILTER (WHERE status = 'STAGED') as staged_count,
      MIN(updated_at) FILTER (WHERE status = 'STAGED') as oldest_staged,
      MAX(updated_at) FILTER (WHERE status = 'STAGED') as newest_staged,
      MAX(updated_at) as last_mapped,
      count(*) FILTER (WHERE status = 'COMPLETED' AND updated_at >= NOW() - INTERVAL '15 minutes') as completed_mappings_15m,
      count(*) FILTER (WHERE status = 'STAGED' AND updated_at >= NOW() - INTERVAL '15 minutes') as staged_mappings_15m
    FROM importer_chapter_mappings;
  `);
  console.log('Mappings telemetry:', mapTimes.rows[0]);

  console.log('\n=== STEP 4: SETTINGS & PROTECTIVE STOP ===');
  const settings = await pool.query(`
    SELECT key, value
    FROM settings
    WHERE key IN ('importer_heartbeat', 'importer_status', 'importer_state', 'importer_protective_stop', 'publication_safety_barrier', 'importer_metrics');
  `);
  console.log('Settings:', settings.rows);

  console.log('\n=== STEP 5: BARRIER & STAGED BLOCKERS ===');
  // Check which works have STAGED chapters and why they aren't publishing
  const stagedWorks = await pool.query(`
    SELECT 
      m.work_id,
      w.title,
      count(*) as staged_count,
      MIN(m.chapter_sort_key) as min_staged_sort,
      MAX(m.chapter_sort_key) as max_staged_sort
    FROM importer_chapter_mappings m
    LEFT JOIN works w ON m.work_id = w.id
    WHERE m.status = 'STAGED'
    GROUP BY m.work_id, w.title
    ORDER BY staged_count DESC
    LIMIT 10;
  `);
  console.log('Top staged works:', stagedWorks.rows);

  for (const sw of stagedWorks.rows.slice(0, 5)) {
    if (!sw.work_id) continue;
    const pubMax = await pool.query(`
      SELECT COALESCE(MAX(number), -1) as max_pub FROM chapters WHERE work_id = $1::uuid AND published_at IS NOT NULL;
    `, [sw.work_id]);
    const maxP = parseFloat(pubMax.rows[0].max_pub);
    const minS = parseFloat(sw.min_staged_sort);
    console.log(`Work "${sw.title}" (${sw.work_id}): max published = ${maxP}, min staged = ${minS}, diff = ${(minS - maxP).toFixed(2)}`);
    
    // Check if intermediate chapters exist in mappings or queue
    const gapQuery = await pool.query(`
      SELECT chapter_sort_key, status, is_gap, source
      FROM importer_chapter_mappings
      WHERE work_id = $1::uuid AND chapter_sort_key > $2::numeric AND chapter_sort_key < $3::numeric
      ORDER BY chapter_sort_key ASC
      LIMIT 10;
    `, [sw.work_id, maxP, minS]);
    console.log(`   Intermediate mappings between ${maxP} and ${minS} (${gapQuery.rows.length}):`, gapQuery.rows);

    const gapQueue = await pool.query(`
      SELECT chapter_sort_key, status, source, attempts, last_error
      FROM importer_queue
      WHERE (payload->>'workId') = $1 AND chapter_sort_key > $2::numeric AND chapter_sort_key < $3::numeric
      ORDER BY chapter_sort_key ASC
      LIMIT 10;
    `, [sw.work_id, maxP, minS]);
    console.log(`   Intermediate queue jobs between ${maxP} and ${minS} (${gapQueue.rows.length}):`, gapQueue.rows);
  }
}

main().catch(console.error).finally(() => pool.end());
