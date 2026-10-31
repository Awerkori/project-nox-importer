import { describe, it, expect } from 'vitest';
import { discloudStatus } from '../scripts/discloud-status-classify.mjs';
describe('DIScloud status classification', () => {
  it('recognizes explicit states', () => {
    expect(discloudStatus(true, { apps: { container: 'Online' } })).toBe('ONLINE');
    expect(discloudStatus(true, { apps: { online: false } })).toBe('OFFLINE');
  });
  it('does not treat errors or missing data as offline', () => {
    for (const [ok, value] of [[false, { online: false }], [true, { status: 'error', online: false }], [true, {}], [true, null], [true, { apps: [] }]]) {
      expect(discloudStatus(ok, value)).toBe('UNKNOWN');
    }
  });
});
