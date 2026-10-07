import { execSync } from 'child_process';
const start = Date.now();
while (Date.now() - start < 120000) {
  try {
    execSync('node scripts/fetch_discloud_logs.mjs > /tmp/discloud6.log');
    const logs = execSync('grep "Starting Nox Importer Service" /tmp/discloud6.log || true').toString();
    if (logs.trim().length > 0) {
      console.log("Restarted!");
      process.exit(0);
    }
  } catch (e) {}
  execSync('sleep 5');
}
