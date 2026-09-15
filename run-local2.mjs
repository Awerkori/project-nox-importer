import { spawn } from 'child_process';
const p = spawn('npm', ['run', 'start'], { stdio: 'inherit' });
setTimeout(() => {
  p.kill('SIGINT');
  console.log("Stopped local importer.");
  process.exit(0);
}, 60000);
