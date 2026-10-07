import { describe, expect, it } from 'vitest';
import { getSqlFingerprint } from '../src/db/yugabyte-direct.js';

describe('Yugabyte query fingerprint telemetry', () => {
  it('normalizes quoted QueryBuilder tables without recording SQL values', () => {
    expect(getSqlFingerprint('SELECT * FROM "chapters" WHERE "id" = $1')).toBe('SELECT chapters');
    expect(getSqlFingerprint('UPDATE "importer_queue" SET "status" = $1')).toBe('UPDATE importer_queue');
    expect(getSqlFingerprint('INSERT INTO "media" ("id") VALUES ($1)')).toBe('INSERT media');
  });

  it('distinguishes importer RPCs from ordinary SQL functions', () => {
    expect(getSqlFingerprint('SELECT importer_replace_pages($1::uuid, $2::jsonb)')).toBe('SELECT rpc:importer_replace_pages');
    expect(getSqlFingerprint('SELECT COALESCE(MAX(number), -1) FROM chapters')).toBe('SELECT chapters');
  });
});
