import fs from 'fs';
let content = fs.readFileSync('src/core/engine.ts', 'utf-8');

if (!content.includes('TELEMETRY_JOB_STAGED')) {
  content = content.replace(
    'successfulExecution = true;\n        break;\n      }',
    `successfulExecution = true;
        telemetry.tStaged = Date.now();
        this.logger.info('TELEMETRY_JOB_STAGED', telemetry);
        this.supabase.from('importer_queue').update({
          payload: { ...job.payload, telemetry }
        }).eq('id', job.id).then(() => {}).catch(() => {});
        break;
      }`
  );
  fs.writeFileSync('src/core/engine.ts', content, 'utf-8');
  console.log("Staged telemetry patched.");
} else {
  console.log("Already patched staged.");
}
