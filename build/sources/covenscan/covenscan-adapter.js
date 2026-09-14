import { MadaraAdapter } from '../common/madara-adapter.js';
// NOTE: Covenscan migrated WordPress to /bruxonas/ sub-path (verified 2026-09-14)
// All URLs now: https://covendasbruxonas.com/bruxonas/manga/[slug]/capitulo-N/
export class CovenScanAdapter extends MadaraAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'covenscan',
            name: 'Coven Scan',
            baseUrl: 'https://covendasbruxonas.com/bruxonas',
            mangaSubString: 'manga',
            rateLimitRps: 2.0,
        }, rateLimiter, transport);
    }
}
