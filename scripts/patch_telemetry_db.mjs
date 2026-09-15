import fs from 'fs';

let content = fs.readFileSync('src/core/engine.ts', 'utf-8');

// Replace the logger.info with an update to the database
content = content.replace(
  "this.logger.info('TELEMETRY_JOB_STAGED', telemetry);",
  `this.logger.info('TELEMETRY_JOB_STAGED', telemetry);
          // SAVE TO DB FOR BENCHMARK
          this.supabase.from('importer_queue').update({
            payload: { ...job.payload, telemetry }
          }).eq('id', job.id).then(() => {}).catch(() => {});`
);

fs.writeFileSync('src/core/engine.ts', content, 'utf-8');
console.log("Telemetry DB patch applied");
