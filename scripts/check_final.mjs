const jwt = process.env.SUPABASE_SERVICE_ROLE_KEY;

async function queryDB(query) {
  const r = await fetch('https://api.supabase.com/v1/projects/izregkwaqdygwioqzwwo/database/query', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${jwt}` },
    body: JSON.stringify({ query })
  });
  return r.json();
}

async function run() {
  const queueData = await queryDB(`
    SELECT status, COUNT(*) 
    FROM importer_queue 
    GROUP BY status 
    ORDER BY status;
  `);
  
  const pubData = await queryDB(`
    SELECT COUNT(*) 
    FROM chapters 
    WHERE published_at > NOW() - INTERVAL '5 minutes';
  `);
  
  const acquireData = await queryDB(`
    SELECT COUNT(*) 
    FROM importer_queue 
    WHERE status = 'ACQUIRED' AND updated_at > NOW() - INTERVAL '5 minutes';
  `);
  
  console.log("== QUEUE STATUS ==");
  console.log(JSON.stringify(queueData, null, 2));
  console.log("\n== PUBLISHED LAST 5 MIN ==");
  console.log(JSON.stringify(pubData, null, 2));
  console.log("\n== ACQUIRED LAST 5 MIN ==");
  console.log(JSON.stringify(acquireData, null, 2));
}

run();
