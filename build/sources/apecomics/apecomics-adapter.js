import { MangaWorkAdapter } from '../common/mangawork-adapter.js';
export class ApeComicsAdapter extends MangaWorkAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'apecomics',
            name: 'Capitoons',
            baseUrl: 'https://capitoons.com',
            seriesPath: 'series',
            rateLimitRps: 2.0,
        }, rateLimiter, transport);
    }
}
