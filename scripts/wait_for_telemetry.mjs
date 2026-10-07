import { execSync } from 'child_process';
const start = Date.now();
while (Date.now() - start < 120000) {
  try {
    execSync('node scripts/fetch_discloud_logs.mjs > /tmp/discloud.log');
    const logs = execSync('grep "SCHEDULER_TELEMETRY" /tmp/discloud.log || true').toString();
    if (logs.trim().length > 0) {
      console.log(logs);
      process.exit(0);
    }
  } catch (e) {}
  execSync('sleep 5');
}
console.log("No telemetry found in 2 minutes");
