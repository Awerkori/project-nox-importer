import fs from 'fs';

const log = fs.readFileSync('/home/awerkori/.gemini/antigravity-cli/brain/7e49a166-bd1d-4a8c-98cf-cef458fa3174/.system_generated/tasks/task-3110.log', 'utf8');
const lines = log.split('\n');

const home = [];
const reader = [];
const media = [];

for (const line of lines) {
  const m = line.match(/Site TTFB:\s*H:(\d+)ms\s*R:(\d+)ms\s*M:(\d+)ms/);
  if (m) {
    const h = parseInt(m[1]);
    const r = parseInt(m[2]);
    const med = parseInt(m[3]);
    if (h < 9000) home.push(h);
    if (r < 9000) reader.push(r);
    if (med < 9000) media.push(med);
  }
}

function stats(arr) {
  if (!arr.length) return null;
  const sorted = [...arr].sort((a,b) => a - b);
  const p = (pct) => sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * pct))];
  const avg = Math.round(sorted.reduce((a,b)=>a+b, 0) / sorted.length);
  return {
    count: sorted.length,
    p50: p(0.50),
    p90: p(0.90),
    p95: p(0.95),
    p99: p(0.99),
    min: sorted[0],
    max: sorted[sorted.length - 1],
    avg
  };
}

console.log('HOME 3H STATS:', JSON.stringify(stats(home), null, 2));
console.log('READER 3H STATS:', JSON.stringify(stats(reader), null, 2));
console.log('MEDIA 3H STATS:', JSON.stringify(stats(media), null, 2));
