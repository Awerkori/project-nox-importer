import { describe, it, expect } from 'vitest';
import { DeduplicationEngine } from '../deduplication.js';
describe('Tag Normalization', () => {
    it('normalizes Yaoi aliases', () => {
        const engine = new DeduplicationEngine({});
        expect(engine['normalizeTagName']('BL')).toBe('Yaoi');
        expect(engine['normalizeTagName']('Boys Love')).toBe('Yaoi');
        expect(engine['normalizeTagName']('shounen-ai')).toBe('Yaoi');
    });
    it('normalizes Adult aliases', () => {
        const engine = new DeduplicationEngine({});
        expect(engine['normalizeTagName']('18+')).toBe('Adulto');
        expect(engine['normalizeTagName']('mature')).toBe('Adulto');
    });
    it('filters garbage tags', () => {
        const engine = new DeduplicationEngine({});
        expect(engine['isGarbageTag']('Leia no nosso site')).toBe(true);
        expect(engine['isGarbageTag']('Completo')).toBe(true);
        expect(engine['isGarbageTag']('Action')).toBe(false);
    });
    it('provides default tags for specialized sources', () => {
        const engine = new DeduplicationEngine({});
        expect(engine['getProviderDefaultTags']('yaoifanclub')).toContain('Yaoi');
        expect(engine['getProviderDefaultTags']('megahentai')).toContain('Hentai');
        expect(engine['getProviderDefaultTags']('randomscan')).toHaveLength(0);
    });
});
