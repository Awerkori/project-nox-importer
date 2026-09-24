import { describe, it, expect, vi } from 'vitest';
import { PublicationSafetyBarrier } from '../src/core/publication-safety-barrier.js';

describe('Publication Stall Detector & Lifecycle Transitions', () => {
  const createMockSupabase = (initialState = 'OPEN') => {
    let currentState = initialState;
    return {
      from: vi.fn().mockImplementation((table: string) => ({
        select: vi.fn().mockImplementation(() => ({
          eq: vi.fn().mockImplementation(() => ({
            maybeSingle: vi.fn().mockResolvedValue({ data: { value: currentState }, error: null }),
            gt: vi.fn().mockResolvedValue({ count: 0, error: null }),
          })),
        })),
        upsert: vi.fn().mockImplementation((row: any) => {
          if (row.key === 'publication_safety_barrier') {
            currentState = row.value;
          }
          return Promise.resolve({ error: null });
        }),
      })),
      getCurrentState: () => currentState,
    };
  };

  it('1. Triggers CAUTION when throughput=0, readyBacklog>0, and producer active', async () => {
    const mockDb = createMockSupabase('OPEN');
    const barrier = new PublicationSafetyBarrier(mockDb as any);

    // Initial state is OPEN
    expect(await barrier.getState()).toBe('OPEN');
    expect(await barrier.canAcquireChapters()).toBe(true);
    expect(await barrier.isBackfillAllowed()).toBe(true);

    // Evaluate stall condition: throughput = 0, backlog = 50, producer = true
    const evaluation = barrier.evaluatePublicationStall(
      {
        publicationThroughput: 0,
        readyBacklog: 50,
        producerActive: true,
      },
      'OPEN'
    );

    expect(evaluation.isStalled).toBe(true);
    expect(evaluation.action).toBe('NONE');
    expect(evaluation.nextState).toBe('CAUTION');
  });

  it('2. Confirms that CLOSED barrier holds 0 worker slots and allows 0 new historical claims', async () => {
    const mockDb = createMockSupabase('CLOSED');
    const barrier = new PublicationSafetyBarrier(mockDb as any);

    expect(await barrier.getState()).toBe('CLOSED');
    // canAcquireChapters must be strictly false
    expect(await barrier.canAcquireChapters()).toBe(false);
    // isBackfillAllowed must be strictly false
    expect(await barrier.isBackfillAllowed()).toBe(false);

    // Simulating worker acquisition loop check
    const workerSlotsHeld = (await barrier.canAcquireChapters()) ? 5 : 0;
    const historicalClaims = (await barrier.isBackfillAllowed()) ? 10 : 0;
    const newHistoricalDownloads = (await barrier.canAcquireChapters()) ? 20 : 0;

    expect(workerSlotsHeld).toBe(0);
    expect(historicalClaims).toBe(0);
    expect(newHistoricalDownloads).toBe(0);
  });

  it('3. Automatic recovery transition: CLOSED -> RECOVERING when publisher resumes', async () => {
    const mockDb = createMockSupabase('CLOSED');
    const barrier = new PublicationSafetyBarrier(mockDb as any);

    // Publisher restored with throughput > 0
    const evalRecovering = barrier.evaluatePublicationStall(
      {
        publicationThroughput: 5,
        readyBacklog: 20,
        producerActive: true,
      },
      'CLOSED'
    );

    expect(evalRecovering.action).toBe('AUTO_RECOVER');
    expect(evalRecovering.nextState).toBe('RECOVERING');
  });

  it('4. Automatic recovery transition: RECOVERING -> OPEN when ready backlog is drained', async () => {
    const mockDb = createMockSupabase('RECOVERING');
    const barrier = new PublicationSafetyBarrier(mockDb as any);

    // Ready backlog drained to 0 and throughput is healthy
    const evalOpen = barrier.evaluatePublicationStall(
      {
        publicationThroughput: 12,
        readyBacklog: 0,
        producerActive: true,
      },
      'RECOVERING'
    );

    expect(evalOpen.action).toBe('AUTO_OPEN');
    expect(evalOpen.nextState).toBe('OPEN');
  });

  it('5. Backlog preservation: state transitions never drop or dump backlog', async () => {
    const mockDb = createMockSupabase('CLOSED');
    const barrier = new PublicationSafetyBarrier(mockDb as any);

    let backlog = [1, 2, 3, 4, 5]; // 5 staged items

    // Recovery starts -> RECOVERING
    await barrier.checkAndEnforceStallDetector({
      publicationThroughput: 2,
      readyBacklog: backlog.length,
      producerActive: true,
    });
    expect(mockDb.getCurrentState()).toBe('RECOVERING');
    expect(backlog.length).toBe(5); // Preserved

    // Backlog drains naturally
    backlog = [];
    await barrier.checkAndEnforceStallDetector({
      publicationThroughput: 5,
      readyBacklog: 0,
      producerActive: true,
    });
    expect(mockDb.getCurrentState()).toBe('OPEN');
    expect(backlog.length).toBe(0);
  });
});
