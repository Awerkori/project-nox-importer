import { describe, expect, it } from 'vitest';
import { PageByteBudget } from '../page-byte-budget.js';

describe('PageByteBudget', () => {
  it('bounds retained page bytes and lets an in-flight body upgrade before a new page starts', async () => {
    const budget = new PageByteBudget(5);
    const first = await budget.reserve(2);
    const second = await budget.reserve(2);
    const third = await budget.reserve(1);
    let newPageAdmitted = false;
    const newPage = budget.reserve(2).then((reservation) => {
      newPageAdmitted = true;
      return reservation;
    });
    let upgraded = false;
    const upgrade = first.upgrade(3).then(() => { upgraded = true; });

    second.release();
    await upgrade;
    expect(upgraded).toBe(true);
    expect(newPageAdmitted).toBe(false);
    expect(budget.snapshot()).toEqual({ capacityBytes: 5, reservedBytes: 4, waiting: 1 });

    first.release();
    const fourth = await newPage;
    expect(newPageAdmitted).toBe(true);
    fourth.release();
    third.release();
    expect(budget.snapshot()).toEqual({ capacityBytes: 5, reservedBytes: 0, waiting: 0 });
  });

  it('removes an aborted waiter without retaining budget', async () => {
    const budget = new PageByteBudget(2);
    const held = await budget.reserve(2);
    const controller = new AbortController();
    const waiting = budget.reserve(1, controller.signal);
    controller.abort(new Error('stop'));
    await expect(waiting).rejects.toThrow('stop');
    expect(budget.snapshot()).toEqual({ capacityBytes: 2, reservedBytes: 2, waiting: 0 });
    held.release();
    expect(budget.snapshot()).toEqual({ capacityBytes: 2, reservedBytes: 0, waiting: 0 });
  });

  it('admits a useful lengthless wave while retaining terminal-upgrade headroom', async () => {
    const budget = new PageByteBudget(20);
    const first = await budget.reserve(4, undefined, { completionHeadroomBytes: 16 });
    let secondAdmitted = false;
    const second = budget.reserve(4, undefined, { completionHeadroomBytes: 16 }).then((reservation) => {
      secondAdmitted = true;
      return reservation;
    });

    // The initial reservation leaves 16 bytes for this started body to reach
    // its terminal bound. A speculative second body cannot consume it.
    await first.upgrade(20);
    expect(secondAdmitted).toBe(false);
    expect(budget.snapshot()).toEqual({ capacityBytes: 20, reservedBytes: 20, waiting: 1 });

    first.release();
    const admitted = await second;
    expect(secondAdmitted).toBe(true);
    admitted.release();
    expect(budget.snapshot()).toEqual({ capacityBytes: 20, reservedBytes: 0, waiting: 0 });
  });

  it('lets existing pages drain when memory pressure lowers capacity', async () => {
    const budget = new PageByteBudget(6);
    const first = await budget.reserve(3);
    const second = await budget.reserve(3);

    budget.setCapacityBytes(2);
    let admitted = false;
    const waiting = budget.reserve(1).then((reservation) => {
      admitted = true;
      return reservation;
    });
    expect(budget.snapshot()).toEqual({ capacityBytes: 2, reservedBytes: 6, waiting: 1 });

    first.release();
    expect(admitted).toBe(false);
    second.release();
    const third = await waiting;
    expect(admitted).toBe(true);
    third.release();
  });
});
