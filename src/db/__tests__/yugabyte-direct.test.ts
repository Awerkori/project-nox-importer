import { describe, expect, it } from 'vitest';
import { getSqlFingerprint } from '../yugabyte-direct.js';

describe('getSqlFingerprint', () => {
  it('keeps static operation labels without exposing SQL or values', () => {
    expect(getSqlFingerprint('/* nox-sql:upload-receipt-batch */ WITH input AS (SELECT $1) SELECT * FROM input'))
      .toBe('OP upload-receipt-batch');
  });

  it('continues to classify untagged RPCs and table statements', () => {
    expect(getSqlFingerprint('SELECT public.importer_lite_claim_task_focused($1)'))
      .toBe('SELECT rpc:importer_lite_claim_task_focused');
    expect(getSqlFingerprint('UPDATE importer_lite_tasks SET state=$1 WHERE id=$2'))
      .toBe('UPDATE importer_lite_tasks');
  });
});
