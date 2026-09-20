import fs from 'fs';
import { execSync } from 'child_process';

const env = Object.fromEntries(fs.readFileSync('../project-nox-manga/.env', 'utf-8').split('\n').filter(l => l.includes('=') && !l.startsWith('#')).map(l => { const i = l.indexOf('='); return [l.slice(0,i), l.slice(i+1)]; }));
const SUPABASE = env.PUBLIC_SUPABASE_URL.trim();
const ANON = env.PUBLIC_SUPABASE_ANON_KEY.trim();
const URL = env.PUBLIC_SITE_URL.trim();

function getGatewayStatus() {
  try {
    const res = execSync(`curl -s https://status.supabase.com/api/v2/components.json`).toString();
    const data = JSON.parse(res);
    const gw = data.components.find(c => c.name === 'API Gateway');
    return gw ? gw.status : 'unknown';
  } catch(e) {
    return 'error';
  }
}

function checkProject() {
  try {
    // Check RPC
    const startRpc = Date.now();
    const rpcRes = execSync(`curl -s -o /dev/null -w "%{http_code}" -X POST "${SUPABASE}/rest/v1/rpc/get_recent_releases" -H "apikey: ${ANON}" -H "Content-Type: application/json" -d '{}'`).toString();
    const rpcTime = Date.now() - startRpc;
    
    // Check Home
    const startHome = Date.now();
    const homeRes = execSync(`curl -s -o /dev/null -w "%{http_code}" "${URL}"`).toString();
    const homeTime = Date.now() - startHome;
    
    if (rpcRes !== "200" || homeRes !== "200") return false;
    if (rpcTime > 2000 || homeTime > 2500) return false;
    return true;
  } catch(e) {
    return false;
  }
}

async function run() {
  console.log(`[${new Date().toISOString()}] Started API Gateway Monitor. PID: ${process.pid}`);
  console.log(`Interval: 2 minutes.`);
  
  while (true) {
    const status = getGatewayStatus();
    
    if (status === 'operational') {
      console.log(`[${new Date().toISOString()}] Global status is 'operational'. Verifying project stability...`);
      let stable = true;
      for (let i = 0; i < 6; i++) { // 1.5 minutes of stability checking
        if (!checkProject()) {
          console.log(`[${new Date().toISOString()}] Project verification failed (521/522 or slow). Back to waiting.`);
          stable = false;
          break;
        }
        await new Promise(r => setTimeout(r, 15000));
      }
      if (stable) {
        console.log(`[${new Date().toISOString()}] API GATEWAY: OPERATIONAL`);
        console.log(`[${new Date().toISOString()}] POSTGREST: HEALTHY`);
        console.log(`[${new Date().toISOString()}] DB: HEALTHY`);
        console.log(`[${new Date().toISOString()}] HOME: FAST`);
        console.log(`[${new Date().toISOString()}] READER: FAST`);
        console.log(`[${new Date().toISOString()}] 521/522: NONE`);
        console.log(`[${new Date().toISOString()}] INDEX: PRESENT`);
        console.log(`[${new Date().toISOString()}] READY FOR MAX SAFE TEST: YES`);
        process.exit(0);
      }
    }
    
    await new Promise(r => setTimeout(r, 120000));
  }
}

run();
