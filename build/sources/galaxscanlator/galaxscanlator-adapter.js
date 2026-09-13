import { ZeistMangaAdapter } from '../common/zeistmanga-adapter.js';
export class GalaxScanlatorAdapter extends ZeistMangaAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'galaxscanlator',
            name: 'GALAX Scans',
            baseUrl: 'https://galaxscanlator.blogspot.com',
            seriesCategory: 'Series',
            rateLimitRps: 2.0,
        }, rateLimiter, transport);
    }
}
