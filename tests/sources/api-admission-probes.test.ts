import { describe, expect, it } from 'vitest';
import { MaidScanAdapter } from '../../src/sources/maidscan/maidscan-adapter.js';
import { TaimuMangasAdapter } from '../../src/sources/taimumangas/taimumangas-adapter.js';
import { VegitoonsAdapter } from '../../src/sources/vegitoons/vegitoons-adapter.js';

describe('API-backed source admission probes', () => {
  it.each([
    ['Maid Scan', new MaidScanAdapter(), 'https://api.verdinha.wtf/obras/atualizacoes?pagina=1&limite=1&gen_id=4', '3'],
    ['Vegitoons', new VegitoonsAdapter(), 'https://api.vegitoons.black/obras/atualizacoes?pagina=1&limite=1&gen_id=1', '1'],
    ['TaimuMangás', new TaimuMangasAdapter(), 'https://api.taimumangas.com/api/v1/reader/updates?page=1&per_page=1&adult_mode=true', undefined],
  ])('probes the public API used by %s', (_name, adapter, url, scanId) => {
    expect(adapter.admissionProbe.url).toBe(url);
    expect(adapter.admissionProbe.expectedType).toBe('json');
    expect(adapter.admissionProbe.headers?.Accept).toContain('application/json');
    if (scanId) expect(adapter.admissionProbe.headers?.['scan-id']).toBe(scanId);
  });
});
