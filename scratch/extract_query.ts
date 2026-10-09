import fs from 'fs';
const code = fs.readFileSync('src/core/scheduler/work-affinity-scheduler.ts', 'utf8');
const p0Start = code.indexOf('private async claimP0Job');
const p0End = code.indexOf('private async claimP1Job');
fs.writeFileSync('scratch/claimP0Job.txt', code.substring(p0Start, p0End));
