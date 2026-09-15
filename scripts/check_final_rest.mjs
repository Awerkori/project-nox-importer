const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
const url = 'https://izregkwaqdygwioqzwwo.supabase.co';

async function run() {
  const qR = await fetch(`${url}/rest/v1/importer_queue?select=status`, {
    headers: { 'apikey': key, 'Authorization': `Bearer ${key}` }
  }).then(r => r.json());
  
  let qCounts = {};
  if (Array.isArray(qR)) {
    qR.forEach(r => qCounts[r.status] = (qCounts[r.status] || 0) + 1);
  } else {
    qCounts = qR;
  }
  
  // Calculate a 5 min ago timestamp
  const d = new Date(Date.now() - 5 * 60000).toISOString();
  
  const pubR = await fetch(`${url}/rest/v1/chapters?select=id&published_at=gt.${d}`, {
    headers: { 'apikey': key, 'Authorization': `Bearer ${key}`, 'Prefer': 'count=exact' },
    method: 'HEAD'
  });
  
  console.log("== QUEUE STATUS ==");
  console.log(qCounts);
  
  console.log("\n== PUBLISHED LAST 5 MIN ==");
  console.log(pubR.headers.get('content-range') || '0-0/0');
}

run();
