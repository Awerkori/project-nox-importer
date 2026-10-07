const mode = process.argv[2] ?? 'hang';
const durationMs = Number(process.argv[3] ?? 5000);

function stop() {
  clearTimeout(timer);
  process.exit(0);
}

process.on('SIGTERM', stop);
process.on('SIGINT', stop);

const timer = mode === 'slow'
  ? setTimeout(stop, durationMs)
  : setInterval(() => {}, 1000);

