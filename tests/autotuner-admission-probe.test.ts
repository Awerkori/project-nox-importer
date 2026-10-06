import { describe, expect, it } from 'vitest';
import { readFileSync } from 'node:fs';

describe('autotuner admission probe', () => {
  it('does not run a queue-wide COUNT or admission nudge while claims pressure the pool', () => {
    const source = readFileSync('src/core/engine.ts', 'utf8');
    const start = source.indexOf('if (slotSnapshot.productiveSlotRatio < 70)');
    const end = source.indexOf('this.logger.info(', start);
    const probe = source.slice(start, end);

    expect(probe).toMatch(/waitingCount/);
    expect(probe).toMatch(/idleCount/);
    expect(probe).toMatch(/SELECT 1[\s\S]{0,300}LIMIT 1/);
    expect(probe).not.toMatch(/SELECT COUNT\(\*\) as claimable_cnt/);
  });
});
