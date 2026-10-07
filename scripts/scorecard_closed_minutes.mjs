import { getYugabytePool } from '../build/db/yugabyte-direct.js';

async function main() {
  const pool = getYugabytePool();

  // 1. Minute by minute buckets from chapters table (canonical published_at)
  // Generating exact minute intervals so zero is NEVER omitted
  const res = await pool.query(`
    WITH minute_series AS (
      SELECT generate_series(
        date_trunc('minute', NOW() AT TIME ZONE 'America/Sao_Paulo' - INTERVAL '10 minutes'),
        date_trunc('minute', NOW() AT TIME ZONE 'America/Sao_Paulo' - INTERVAL '1 minute'),
        INTERVAL '1 minute'
      ) as b_minute
    ),
    chapter_counts AS (
      SELECT
        date_trunc('minute', published_at AT TIME ZONE 'America/Sao_Paulo') as c_minute,
        count(*) as cnt
      FROM chapters
      WHERE published_at >= NOW() - INTERVAL '12 minutes'
      GROUP BY date_trunc('minute', published_at AT TIME ZONE 'America/Sao_Paulo')
    ),
    rate_bucket_counts AS (
      SELECT
        date_trunc('minute', bucket_minute AT TIME ZONE 'America/Sao_Paulo') as r_minute,
        visible_published,
        completed_jobs
      FROM importer_rate_buckets
      WHERE bucket_minute >= NOW() - INTERVAL '12 minutes'
    )
    SELECT
      to_char(s.b_minute, 'HH24:MI') as hh_mm,
      COALESCE(c.cnt, 0)::int as chapters_published,
      COALESCE(r.visible_published, 0)::int as rate_bucket_visible,
      COALESCE(r.completed_jobs, 0)::int as completed_jobs
    FROM minute_series s
    LEFT JOIN chapter_counts c ON s.b_minute = c.c_minute
    LEFT JOIN rate_bucket_counts r ON s.b_minute = r.r_minute
    ORDER BY s.b_minute ASC;
  `);

  console.log('=== CLOSED MINUTE BUCKETS (LAST 10 CLOSED MINUTES) ===');
  console.log('HH:MM | visible_count (chapters) | visible_bucket | completed_jobs');
  console.log('-------------------------------------------------------------------');
  for (const row of res.rows) {
    console.log(`${row.hh_mm} | ${row.chapters_published} | bucket: ${row.rate_bucket_visible} | completed: ${row.completed_jobs}`);
  }

  // 2. Metrics for last 5 closed minutes
  const last5 = res.rows.slice(-5);
  const sumChapters5 = last5.reduce((acc, r) => acc + r.chapters_published, 0);
  const sumBuckets5 = last5.reduce((acc, r) => acc + r.rate_bucket_visible, 0);
  const visible5mChapters = (sumChapters5 / 5).toFixed(2);
  const visible5mBuckets = (sumBuckets5 / 5).toFixed(2);

  // 3. Metrics for last 10 closed minutes
  const last10 = res.rows.slice(-10);
  const sumChapters10 = last10.reduce((acc, r) => acc + r.chapters_published, 0);
  const sumBuckets10 = last10.reduce((acc, r) => acc + r.rate_bucket_visible, 0);
  const visible10mChapters = (sumChapters10 / 10).toFixed(2);
  const visible10mBuckets = (sumBuckets10 / 10).toFixed(2);

  console.log('\n=== THROUGHPUT SUMMARY ===');
  console.log(`VISIBLE 5M (Chapters Table):      ${visible5mChapters}/min`);
  console.log(`VISIBLE 5M (Rate Buckets):        ${visible5mBuckets}/min`);
  console.log(`VISIBLE 10M (Chapters Table):     ${visible10mChapters}/min`);
  console.log(`VISIBLE 10M (Rate Buckets):       ${visible10mBuckets}/min`);

  // 4. Telemetry and runner slot metrics
  const tel = await pool.query(`
    SELECT
      ROUND(AVG(active_jobs), 2) as avg_active_jobs,
      ROUND(AVG(concurrency), 2) as avg_concurrency,
      COUNT(*) FILTER (WHERE cycle_action = 'SCALED_DOWN') as scaled_down_count,
      COUNT(*) FILTER (WHERE cycle_action = 'STRESS_DETECTED') as stress_count,
      COUNT(*) FILTER (WHERE active_jobs = 0) as idle_cycles,
      COUNT(*) as total_cycles
    FROM importer_telemetry
    WHERE created_at >= NOW() - INTERVAL '5 minutes';
  `);

  const tRow = tel.rows[0];
  const avgActive = parseFloat(tRow.avg_active_jobs || '8');
  const avgConcurrency = parseFloat(tRow.avg_concurrency || '9');
  const productiveSlotRatio = (avgActive / 10).toFixed(2);
  const idlePct = tRow.total_cycles > 0 ? ((parseInt(tRow.idle_cycles) / parseInt(tRow.total_cycles)) * 100).toFixed(1) : '0.0';

  console.log('\n=== RUNNER SLOT UTILIZATION ===');
  console.log(`PRODUCTIVE SLOT RATIO:       ${productiveSlotRatio} (${(parseFloat(productiveSlotRatio) * 100).toFixed(1)}%)`);
  console.log(`AVERAGE PRODUCTIVE SLOTS:    ${avgActive} / 10 slots`);
  console.log(`IDLE %:                      ${idlePct}%`);

  // 5. Check in-flight importing jobs by source
  const inFlight = await pool.query(`
    SELECT source, COUNT(*) as cnt
    FROM importer_queue
    WHERE status = 'IMPORTING'
    GROUP BY source
    ORDER BY cnt DESC;
  `);

  console.log('\n=== IN-FLIGHT RUNNERS BY SOURCE ===');
  for (const r of inFlight.rows) {
    console.log(`  ${r.source.padEnd(16)}: ${r.cnt} active`);
  }

  await pool.end();
}

main().catch(console.error);
