import { execSync } from 'child_process';
const seen = new Set();
let p1Rejects = 0;
let lastUpdate = Date.now();

console.log('Observing Discloud logs for 30 seconds...');
for (let i = 0; i < 15; i++) {
  try {
    const logs = execSync('node scripts/fetch_discloud_logs.mjs', { encoding: 'utf8' });
    const lines = logs.split('\n');
    for (const line of lines) {
      if (line.includes('DEBUG_ADM_CANDIDATES') && !seen.has(line)) {
        seen.add(line);
        try {
          const match = line.match(/\[DEBUG_ADM_CANDIDATES\] (\[.*\])/);
          if (match) {
            const candidates = JSON.parse(match[1]);
            p1Rejects += candidates.length;
            console.log(`[Sweep] Evaluated ${candidates.length} candidates. Total so far: ${p1Rejects}`);
            lastUpdate = Date.now();
          }
        } catch (e) {}
      }
    }
  } catch (e) {}
  execSync('sleep 2');
}
console.log(`Observation complete. Last evaluation was ${Math.round((Date.now() - lastUpdate)/1000)}s ago.`);
