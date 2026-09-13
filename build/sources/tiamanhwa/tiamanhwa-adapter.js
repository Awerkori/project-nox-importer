import { MadaraAdapter } from '../common/madara-adapter.js';
export class TiaManhwaAdapter extends MadaraAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'tiamanhwa',
            name: 'Tia Manhwa',
            baseUrl: 'https://tiamanhwa.com',
            mangaSubString: 'manhwa',
            rateLimitRps: 2.0,
        }, rateLimiter, transport);
    }
}
