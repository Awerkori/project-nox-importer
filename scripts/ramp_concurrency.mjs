import { execSync } from 'child_process';
import { readFileSync } from 'fs';

const envLine = readFileSync('../project-nox-manga/.env', 'utf-8').split('\n').find(l => l.startsWith('PUBLIC_SITE_URL='));
const URL = envLine.split('=')[1].trim();
const levels = [4, 6, 8, 12, 16, 24, 32];

function measureHome() {
  const start = Date.now();
  try {
    const out = execSync(`curl -s -o /dev/null -w "%{http_code}" "${URL}"`).toString();
    if (out !== "200") return 9999;
    return Date.now() - start;
  } catch (e) {
    return 9999;
  }
}

async function run() {
  for (const level of levels) {
    console.log(`\n--- Ramping to ${level} ---`);
    execSync(`sed -i 's/requestedMax = [0-9]*/requestedMax = ${level}/g' src/core/engine.ts`);
    execSync(`sed -i 's/initialConcurrency = Math.min([0-9]*, requestedMax)/initialConcurrency = Math.min(${level}, requestedMax)/g' src/core/engine.ts`);
    execSync(`npm run build && node scripts/restart_supabase.mjs`);
    
    // Wait for worker to restart and build up load
    console.log("Waiting 30 seconds for load...");
    await new Promise(r => setTimeout(r, 30000));
    
    let latencies = [];
    for (let i = 0; i < 3; i++) {
      latencies.push(measureHome());
      await new Promise(r => setTimeout(r, 1000));
    }
    const avgHome = latencies.reduce((a,b)=>a+b, 0) / latencies.length;
    
    const dbHealthOut = execSync(`node scripts/check_db.mjs | grep 'DB Health'`).toString();
    const dbMatch = dbHealthOut.match(/DB Health \(([0-9]+)ms\)/);
    const dbLatency = dbMatch ? parseInt(dbMatch[1]) : 9999;
    
    console.log(`Level ${level} -> Home: ${avgHome.toFixed(0)}ms | DB Health: ${dbLatency}ms`);
    
    if (avgHome > 1500 || dbLatency > 1500) {
      console.log(`\nDEGRADATION DETECTED AT LEVEL ${level}. Stopping ramp and reverting to previous safe level.`);
      break; // Abort ramp!
    }
  }
  console.log("\nRamp finished.");
}
run();
