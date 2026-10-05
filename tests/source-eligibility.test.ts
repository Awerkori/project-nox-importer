import { describe, expect, it } from 'vitest';
import {
  hasSuccessfulSourceRecovery,
  isCatalogExecutionEligible,
  isSourceExecutionEligible,
  shouldProbePersistedSource,
} from '../src/core/source-eligibility.js';

describe('source eligibility policy', () => {
  it('fails closed for ACTIVE sources with an unresolved persisted block', () => {
    const source = {
      status: 'ACTIVE',
      enabled: true,
      chapterIngestionEnabled: true,
      blockedReason: 'TURNSTILE',
      blockedDetails: { last_checked_at: '2026-10-05T05:54:40.834Z' },
    };

    expect(isSourceExecutionEligible(source)).toBe(false);
    expect(shouldProbePersistedSource(source)).toBe(true);
  });

  it('keeps an ACTIVE source executable after a successful probe marker', () => {
    const source = {
      status: 'ACTIVE',
      enabled: true,
      chapterIngestionEnabled: true,
      blockedReason: 'TIMEOUT_TARPIT',
      blockedDetails: { probe_success: true, recovered_at: '2026-10-05T05:00:00.000Z' },
    };

    expect(hasSuccessfulSourceRecovery(source)).toBe(true);
    expect(isSourceExecutionEligible(source)).toBe(true);
    expect(shouldProbePersistedSource(source)).toBe(false);
  });

  it('fails closed for catalog discovery on ACTIVE sources with stale blocks', () => {
    expect(isCatalogExecutionEligible({
      status: 'ACTIVE',
      enabled: true,
      catalogDiscoveryEnabled: true,
      blockedReason: 'TIMEOUT_TARPIT',
      blockedDetails: { last_checked_at: '2026-10-05T05:54:40.834Z' },
    })).toBe(false);

    expect(isCatalogExecutionEligible({
      status: 'ACTIVE',
      enabled: true,
      catalogDiscoveryEnabled: true,
      blockedReason: 'TIMEOUT_TARPIT',
      blockedDetails: { probe_success: true, recovered_at: '2026-10-05T06:00:00.000Z' },
    })).toBe(true);
  });

  it('allows an expired transient cooldown but not an active one', () => {
    const now = Date.parse('2026-10-05T06:00:00.000Z');
    const base = { status: 'COOLDOWN', enabled: true, chapterIngestionEnabled: true };

    expect(isSourceExecutionEligible({ ...base, cooldownUntil: now - 1 }, now)).toBe(true);
    expect(isSourceExecutionEligible({ ...base, cooldownUntil: now + 60_000 }, now)).toBe(false);
    expect(shouldProbePersistedSource({ ...base, cooldownUntil: now - 1 })).toBe(true);
  });

  it('keeps an expired cooldown blocked until a probe records recovery', () => {
    const now = Date.parse('2026-10-05T06:00:00.000Z');
    const source = {
      status: 'COOLDOWN',
      enabled: true,
      chapterIngestionEnabled: true,
      cooldownUntil: now - 1,
      blockedReason: 'JS_CHALLENGE',
      blockedDetails: { last_checked_at: '2026-10-05T05:00:00.000Z' },
    };

    expect(isSourceExecutionEligible(source, now)).toBe(false);
    expect(shouldProbePersistedSource(source)).toBe(true);
  });
});
