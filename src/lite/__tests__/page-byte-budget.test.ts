import { describe, expect, it } from 'vitest';
import {
  LITE_PAGE_BUFFER_UNKNOWN_NORMAL_INITIAL_BYTES,
  LITE_PAGE_BUFFER_UNKNOWN_REDUCED_INITIAL_BYTES,
  PageByteBudget,
  unknownPageInitialReservationBytes,
} from '../page-byte-budget.js';

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

  it('returns unused provisional bytes when a response declares a smaller body', async () => {
    const budget = new PageByteBudget(10);
    const reservation = await budget.reserve(8);
    reservation.shrink(3);
    expect(reservation.reservedBytes).toBe(3);
    expect(budget.snapshot()).toEqual({ capacityBytes: 10, reservedBytes: 3, waiting: 0 });
    reservation.release();
    expect(budget.snapshot()).toEqual({ capacityBytes: 10, reservedBytes: 0, waiting: 0 });
  });

  it('admits a useful lengthless wave while retaining terminal-upgrade headroom', async () => {
    const budget = new PageByteBudget(20);
    const first = await budget.reserve(2, undefined, { completionHeadroomBytes: 18 });
    let secondAdmitted = false;
    const second = budget.reserve(2, undefined, { completionHeadroomBytes: 18 }).then((reservation) => {
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

  it('admits the 24-request lengthless wave inside the reduced byte ceiling', async () => {
    const mib = 1024 * 1024;
    const budget = new PageByteBudget(48 * mib);
    const reservations = [];

    for (let index = 0; index < 24; index++) {
      reservations.push(await budget.reserve(
        LITE_PAGE_BUFFER_UNKNOWN_REDUCED_INITIAL_BYTES,
        undefined,
        { completionHeadroomBytes: 20 * mib - LITE_PAGE_BUFFER_UNKNOWN_REDUCED_INITIAL_BYTES },
      ));
    }

    expect(budget.snapshot()).toEqual({ capacityBytes: 48 * mib, reservedBytes: 24 * mib, waiting: 0 });

    await reservations[0].upgrade(20 * mib, undefined, { intent: 'COMPLETION' });
    // Once one active body takes its terminal promotion, that real 20 MiB
    // reservation replaces the one virtual completion headroom. A following
    // small producer can use the remaining bytes, but a second terminal
    // promotion still waits for the first to drain.
    let admitted = false;
    const waiting = budget.reserve(
      LITE_PAGE_BUFFER_UNKNOWN_REDUCED_INITIAL_BYTES,
      undefined,
      { completionHeadroomBytes: 20 * mib - LITE_PAGE_BUFFER_UNKNOWN_REDUCED_INITIAL_BYTES },
    ).then((reservation) => {
      admitted = true;
      return reservation;
    });
    const next = await waiting;
    expect(admitted).toBe(true);
    expect(budget.snapshot()).toEqual({ capacityBytes: 48 * mib, reservedBytes: 44 * mib, waiting: 0 });

    let secondUpgradeDone = false;
    const secondUpgrade = reservations[1].upgrade(20 * mib, undefined, { intent: 'COMPLETION' }).then(() => { secondUpgradeDone = true; });
    await Promise.resolve();
    expect(secondUpgradeDone).toBe(false);

    reservations[0].release();
    await secondUpgrade;
    expect(secondUpgradeDone).toBe(true);
    next.release();
    for (const reservation of reservations.slice(1)) reservation.release();
    expect(budget.snapshot()).toEqual({ capacityBytes: 48 * mib, reservedBytes: 0, waiting: 0 });
  });

  it('uses the p95-sized provisional reservation only in the normal profile', async () => {
    const mib = 1024 * 1024;
    expect(unknownPageInitialReservationBytes(48 * mib)).toBe(LITE_PAGE_BUFFER_UNKNOWN_REDUCED_INITIAL_BYTES);
    expect(unknownPageInitialReservationBytes(64 * mib)).toBe(LITE_PAGE_BUFFER_UNKNOWN_NORMAL_INITIAL_BYTES);

    const budget = new PageByteBudget(64 * mib);
    const reservations = [];
    for (let index = 0; index < 23; index++) {
      reservations.push(await budget.reserve(
        LITE_PAGE_BUFFER_UNKNOWN_NORMAL_INITIAL_BYTES,
        undefined,
        { completionHeadroomBytes: 20 * mib - LITE_PAGE_BUFFER_UNKNOWN_NORMAL_INITIAL_BYTES },
      ));
    }
    expect(budget.snapshot()).toEqual({ capacityBytes: 64 * mib, reservedBytes: 46 * mib, waiting: 0 });
    await reservations[0].upgrade(20 * mib, undefined, { intent: 'COMPLETION' });
    expect(budget.snapshot()).toEqual({ capacityBytes: 64 * mib, reservedBytes: 64 * mib, waiting: 0 });
    for (const reservation of reservations) reservation.release();
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
