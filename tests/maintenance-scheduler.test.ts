import { afterEach, describe, expect, it, vi } from 'vitest';
import { MaintenanceScheduler } from '../src/core/maintenance-scheduler.js';

describe('maintenance lifecycle', () => {
  afterEach(()=>vi.useRealTimers());
  it('serializes tasks, coalesces repeated initialization, and never overlaps slow SQL', async () => {
    vi.useFakeTimers();
    const m=new MaintenanceScheduler();
    let release!:()=>void;
    const slow=vi.fn(()=>new Promise<number>(r=>{release=()=>r(3);}));
    const other=vi.fn(async()=>1);
    m.register('slow',1000,10,slow);
    m.register('other',1000,20,other);
    m.register('slow',1000,10,slow);
    await vi.advanceTimersByTimeAsync(5000);
    expect(slow).toHaveBeenCalledTimes(1);expect(other).not.toHaveBeenCalled();
    expect(m.snapshot().slow.currentlyRunning).toBe(true);
    release();await vi.advanceTimersByTimeAsync(1);
    expect(other).toHaveBeenCalledTimes(1);
    expect(m.snapshot().slow.rowsTouched).toBe(3);
    m.stop();await vi.advanceTimersByTimeAsync(10000);
    expect(slow).toHaveBeenCalledTimes(1);expect(other).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(0);
  });
  it('stopping an in-flight job does not schedule its next cycle', async () => {
    vi.useFakeTimers();const m=new MaintenanceScheduler();let done!:()=>void;
    m.register('task',1000,0,()=>new Promise<void>(r=>{done=r;}));
    await vi.advanceTimersByTimeAsync(1);m.stop();done();await vi.advanceTimersByTimeAsync(5000);
    expect(vi.getTimerCount()).toBe(0);expect(m.snapshot()).toEqual({});
  });
});
