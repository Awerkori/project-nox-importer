import fs from 'fs';
let engine = fs.readFileSync('src/core/engine.ts', 'utf8');

// 1. Modify the fetch timeout from 120_000 to 15_000
engine = engine.replace(
  /signal: AbortSignal.timeout\(120_000\),/g,
  'signal: AbortSignal.timeout(15_000),'
);

// 2. Reduce maxJobDurationMs from 12m to 5m for IMPORT_CHAPTER
engine = engine.replace(
  /job.task_type === 'IMPORT_CHAPTER' \? 12 \* 60 \* 1000 : 5 \* 60 \* 1000;/g,
  "job.task_type === 'IMPORT_CHAPTER' ? 5 * 60 * 1000 : 3 * 60 * 1000;"
);

// 3. Add admission check in executeJobDirectly
const executeJobStart = `  private async executeJobDirectly(job: QueueJob): Promise<void> {`;

const admissionCheck = `
    // ADMISSION GATE: Circuit Breaker / Health Check
    if (job.task_type === 'IMPORT_CHAPTER') {
      const isAvailable = await this.checkSourceAvailability(job.source);
      if (!isAvailable) {
        // Source is down. Do we have fallbacks?
        const fallbacks = job.payload?.fallbackSources || [];
        if (!Array.isArray(fallbacks) || fallbacks.length === 0) {
          this.logger.warn(\`Source \${job.source} is blocked/tarpitting and job \${job.id} has no fallbacks. Rejecting at admission gate.\`, { source: job.source, jobId: job.id });
          await this.supabase.from('importer_queue').update({
            status: 'RETRY',
            next_run_at: new Date(Date.now() + 15 * 60 * 1000).toISOString(),
            locked_by: null,
            locked_at: null,
            last_error: 'Source circuit breaker OPEN or UPSTREAM_BLOCKED. No fallbacks available.'
          }).eq('id', job.id);
          return;
        }
      }
    }
`;

engine = engine.replace(executeJobStart, executeJobStart + admissionCheck);

fs.writeFileSync('src/core/engine.ts', engine);
