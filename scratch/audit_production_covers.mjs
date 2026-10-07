import pg from 'pg';
import dotenv from 'dotenv';
dotenv.config({ path: '/home/awerkori/.Projects/project-nox-importer/.env' });

const { Client } = pg;
const client = new Client({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false }
});

async function main() {
  await client.connect();
  console.log('Connected to YugabyteDB production for Cover Audit\n');

  // 1. Overall Works Count
  const worksStats = await client.query(`
    SELECT 
      count(*) as total_works,
      count(*) FILTER (WHERE published = true) as published_works,
      count(*) FILTER (WHERE published = false) as unpublished_works,
      count(*) FILTER (WHERE cover_id IS NULL) as null_covers,
      count(*) FILTER (WHERE published = true AND cover_id IS NULL) as published_null_covers,
      count(*) FILTER (WHERE published = false AND cover_id IS NULL) as unpublished_null_covers,
      count(*) FILTER (WHERE cover_id IS NOT NULL) as non_null_covers,
      count(*) FILTER (WHERE published = true AND cover_id IS NOT NULL) as published_with_cover
    FROM works
  `);
  console.log('=== 1. WORKS COVER SUMMARY ===');
  console.table(worksStats.rows);

  // 2. Audit cover_id integrity in media table
  const mediaIntegrity = await client.query(`
    SELECT 
      count(*) as total_assigned_covers,
      count(*) FILTER (WHERE m.id IS NULL) as dangling_media_references,
      count(*) FILTER (WHERE m.id IS NOT NULL AND m.storage_ready != true) as not_storage_ready,
      count(*) FILTER (WHERE m.id IS NOT NULL AND m.storage_ready = true) as storage_ready_covers,
      count(*) FILTER (WHERE m.id IS NOT NULL AND (m.width <= 50 OR m.height <= 50)) as tiny_placeholder_dimensions,
      count(*) FILTER (WHERE m.id IS NOT NULL AND m.bytes < 1500) as suspicious_tiny_bytes,
      count(*) FILTER (WHERE m.id IS NOT NULL AND m.provider_key IS NULL) as missing_provider_key
    FROM works w
    LEFT JOIN media m ON m.id = w.cover_id::uuid
    WHERE w.cover_id IS NOT NULL
  `);
  console.log('\n=== 2. MEDIA TABLE INTEGRITY FOR ASSIGNED COVERS ===');
  console.table(mediaIntegrity.rows);

  // 3. Media Provider & Shard distribution for covers
  const providerDist = await client.query(`
    SELECT 
      m.provider,
      m.status,
      count(*) as count
    FROM works w
    JOIN media m ON m.id = w.cover_id::uuid
    GROUP BY m.provider, m.status
  `);
  console.log('\n=== 3. STORAGE PROVIDER & STATUS FOR COVERS ===');
  console.table(providerDist.rows);

  // 4. Published works with cover_id: inspect a sample
  const samplePublishedCovers = await client.query(`
    SELECT 
      w.id as work_id,
      w.title,
      w.cover_id,
      m.storage_ready,
      m.provider,
      m.provider_key,
      m.width,
      m.height,
      m.bytes,
      m.mime
    FROM works w
    LEFT JOIN media m ON m.id = w.cover_id::uuid
    WHERE w.published = true
    ORDER BY w.updated_at DESC
    LIMIT 15
  `);
  console.log('\n=== 4. SAMPLE OF PUBLISHED WORKS AND THEIR COVERS ===');
  console.table(samplePublishedCovers.rows);

  // 5. Check if published works have covers that resolve on the site
  console.log('\n=== 5. PROBING PUBLISHED COVERS ON SITE CDN ===');
  for (const row of samplePublishedCovers.rows.slice(0, 5)) {
    if (!row.cover_id) {
      console.log(`[CDN PROBE] ${row.title}: NO COVER ID`);
      continue;
    }
    const cdnUrl = `https://manga.project-nox-awerkori.workers.dev/media/${row.cover_id}`;
    try {
      const t0 = performance.now();
      const res = await fetch(cdnUrl, { signal: AbortSignal.timeout(5000) });
      const duration = Math.round(performance.now() - t0);
      const contentType = res.headers.get('content-type');
      const contentLength = res.headers.get('content-length');
      const buf = await res.arrayBuffer();
      const bytes = new Uint8Array(buf);
      
      // Check magic numbers
      let magic = 'UNKNOWN';
      if (bytes.length >= 3 && bytes[0] === 0xFF && bytes[1] === 0xD8 && bytes[2] === 0xFF) magic = 'JPEG';
      else if (bytes.length >= 8 && bytes[0] === 0x89 && bytes[1] === 0x50 && bytes[2] === 0x4E && bytes[3] === 0x47) magic = 'PNG';
      else if (bytes.length >= 12 && bytes[0] === 0x52 && bytes[1] === 0x49 && bytes[2] === 0x46 && bytes[3] === 0x46 && bytes[8] === 0x57 && bytes[9] === 0x45 && bytes[10] === 0x42 && bytes[11] === 0x50) magic = 'WEBP';
      else if (new TextDecoder().decode(bytes.slice(0, 30)).toLowerCase().includes('<html')) magic = 'HTML_CORRUPT';

      console.log(`[CDN PROBE] ${row.title} (${row.cover_id}): HTTP ${res.status} | Content-Type: ${contentType} | Size: ${bytes.length} bytes | Magic: ${magic} | TTFB: ${duration}ms`);
    } catch (err) {
      console.error(`[CDN PROBE ERROR] ${row.title} (${row.cover_id}): ${err.message}`);
    }
  }

  // 6. Check mappings metadata for upstream cover URLs
  const mappingsCoverRes = await client.query(`
    SELECT 
      count(*) as total_mappings,
      count(*) FILTER (WHERE metadata->>'cover_url' IS NOT NULL OR metadata->>'coverUrl' IS NOT NULL) as mappings_with_cover_url,
      count(*) FILTER (WHERE metadata->>'cover_url' IS NULL AND metadata->>'coverUrl' IS NULL) as mappings_without_cover_url
    FROM importer_work_mappings
  `);
  console.log('\n=== 6. WORK MAPPINGS UPSTREAM COVER URL COVERAGE ===');
  console.table(mappingsCoverRes.rows);

  await client.end();
}

main().catch(err => {
  console.error('Fatal cover audit error:', err);
  process.exit(1);
});
