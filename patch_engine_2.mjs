import fs from 'fs';
let engine = fs.readFileSync('src/core/engine.ts', 'utf8');

const oldCode = `      const classification = RetryPolicy.classify(err);
      const isStaffPriority = Boolean(job.payload?.staffRequested) || (job.priority >= 100);
      const decision = RetryPolicy.decide(classification, job.attempts, job.max_attempts, { isStaffPriority });`;

const newCode = `      const classification = RetryPolicy.classify(err);
      
      // If it's a network/timeout error, record it in the circuit breaker!
      if (classification === 'NETWORK_TRANSIENT' || errorMessage.toLowerCase().includes('timeout') || errorMessage.toLowerCase().includes('abort')) {
        this.logger.warn(\`Recording timeout/network failure for \${job.source} in circuit breaker.\`);
        const { tripped, cooldownMs } = this.circuitBreaker.recordFailure(job.source, 'TIMEOUT_TARPIT' as any);
        if (tripped) {
          // If tripped, set the DB source status as well
          this.supabase.from('importer_sources').update({
            status: 'COOLDOWN',
            cooldown_until: new Date(Date.now() + cooldownMs).toISOString(),
            blocked_reason: 'TIMEOUT_TARPIT'
          }).eq('id', job.source).then();
        }
      }

      const isStaffPriority = Boolean(job.payload?.staffRequested) || (job.priority >= 100);
      const decision = RetryPolicy.decide(classification, job.attempts, job.max_attempts, { isStaffPriority });`;

engine = engine.replace(oldCode, newCode);
fs.writeFileSync('src/core/engine.ts', engine);
