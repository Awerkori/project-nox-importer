import { getYugabytePool } from '../build/db/yugabyte-direct.js';

const START_MINUTE_STR = process.env.START_MINUTE || '01:39';

export async function getMinuteStats(startMinute = START_MINUTE_STR) {
  const pool = getYugabytePool();
  const q = `
    WITH bounds AS (
      SELECT 
        date_trunc('minute', (NOW() AT TIME ZONE 'America/Sao_Paulo')) - interval '1 minute' as last_closed_minute
    ),
    mins AS (
      SELECT generate_series(
        to_timestamp(to_char(NOW() AT TIME ZONE 'America/Sao_Paulo', 'YYYY-MM-DD ') || $1, 'YYYY-MM-DD HH24:MI'),
        (SELECT last_closed_minute FROM bounds),
        interval '1 minute'
      ) as m
    ),
    ch AS (
      SELECT 
        date_trunc('minute', published_at AT TIME ZONE 'America/Sao_Paulo') as m,
        count(*) as count
      FROM chapters
      WHERE published_at >= to_timestamp(to_char(NOW() AT TIME ZONE 'America/Sao_Paulo', 'YYYY-MM-DD ') || $1, 'YYYY-MM-DD HH24:MI')
      GROUP BY 1
    ),
    rb AS (
      SELECT 
        date_trunc('minute', bucket_minute AT TIME ZONE 'America/Sao_Paulo') as m,
        sum(visible_published) as count
      FROM importer_rate_buckets
      WHERE bucket_minute >= to_timestamp(to_char(NOW() AT TIME ZONE 'America/Sao_Paulo', 'YYYY-MM-DD ') || $1, 'YYYY-MM-DD HH24:MI')
      GROUP BY 1
    ),
    cm AS (
      SELECT 
        date_trunc('minute', updated_at AT TIME ZONE 'America/Sao_Paulo') as m,
        count(*) as count
      FROM importer_chapter_mappings
      WHERE status = 'COMPLETED' 
        AND updated_at >= to_timestamp(to_char(NOW() AT TIME ZONE 'America/Sao_Paulo', 'YYYY-MM-DD ') || $1, 'YYYY-MM-DD HH24:MI')
      GROUP BY 1
    )
    SELECT 
      to_char(mins.m, 'HH24:MI') as minute_brt,
      COALESCE(ch.count, 0)::int as canonical_chapters,
      COALESCE(rb.count, 0)::int as rate_bucket_visible,
      COALESCE(cm.count, 0)::int as completed_mappings
    FROM mins
    LEFT JOIN ch ON mins.m = ch.m
    LEFT JOIN rb ON mins.m = rb.m
    LEFT JOIN cm ON mins.m = cm.m
    ORDER BY mins.m ASC;
  `;
  const res = await pool.query(q, [startMinute]);
  return res.rows;
}

if (process.argv[1]?.endsWith('monitor_scorecard_live.mjs')) {
  getMinuteStats().then(rows => {
    console.table(rows);
    const totalCan = rows.reduce((acc, r) => acc + Number(r.canonical_chapters), 0);
    const totalRb = rows.reduce((acc, r) => acc + Number(r.rate_bucket_visible), 0);
    const totalMins = rows.length;
    console.log(`Total Closed Minutes: ${totalMins}`);
    console.log(`Total Canonical Chapters: ${totalCan} (avg: ${(totalCan / (totalMins || 1)).toFixed(2)}/min)`);
    console.log(`Total Rate Bucket Visible: ${totalRb} (avg: ${(totalRb / (totalMins || 1)).toFixed(2)}/min)`);
    console.log(`Discrepancy: ${Math.abs(totalRb - totalCan)} (${totalRb === 0 && totalCan === 0 ? '0.0%' : ((Math.abs(totalRb - totalCan) / Math.max(1, totalCan)) * 100).toFixed(1)}%)`);
    process.exit(0);
  }).catch(err => {
    console.error(err);
    process.exit(1);
  });
}
