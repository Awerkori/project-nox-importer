import { describe, expect, it } from 'vitest';
import { shouldReserveP0AfterStaffBurst } from '../scheduler/work-affinity-scheduler.js';

describe('Staff/P0 scheduling boundary', () => {
  it('never reserves P0 while STAFF may remain executable', () => {
    expect(shouldReserveP0AfterStaffBurst(0, 4, true)).toBe(false);
    expect(shouldReserveP0AfterStaffBurst(3, 4, true)).toBe(false);
    expect(shouldReserveP0AfterStaffBurst(4, 4, true)).toBe(false);
  });

  it('does not reserve a slot when no real P0 is waiting or fairness is disabled', () => {
    expect(shouldReserveP0AfterStaffBurst(100, 4, false)).toBe(false);
    expect(shouldReserveP0AfterStaffBurst(100, 0, true)).toBe(false);
  });
});
