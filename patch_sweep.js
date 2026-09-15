import fs from 'fs';

const p = 'src/core/publication.ts';
let code = fs.readFileSync(p, 'utf8');

const target = `      // Round-robin iteration across distinct works (one round per sweep)
      for (const workId of activeWorkIds) {
        if (publishedTotal >= maxTotalPublications) break;

        const lock = this.getWorkLock(workId);
        const count = await lock.runExclusive(async () => {
          return this.runCascadeUnderLock(workId, perWorkBurst);
        });

        publishedTotal += count;
      }`;

const replacement = `      // Round-robin iteration across distinct works (one round per sweep)
      for (const workId of activeWorkIds) {
        if (publishedTotal >= maxTotalPublications) break;

        try {
          const lock = this.getWorkLock(workId);
          const count = await lock.runExclusive(async () => {
            return this.runCascadeUnderLock(workId, perWorkBurst);
          });
          publishedTotal += count;
        } catch (workErr: any) {
          this.logger.error('Error cascading work in sweep', { workId, error: workErr?.message });
        }
      }`;

code = code.replace(target, replacement);
fs.writeFileSync(p, code);
console.log('Patched sweep!');
