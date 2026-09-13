import { MadaraAdapter } from '../common/madara-adapter.js';
export class MrTenzusAdapter extends MadaraAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'mrtenzus',
            name: 'MR Tenzus',
            baseUrl: 'https://mrtenzus.com',
            mangaSubString: 'manga',
            rateLimitRps: 2.0,
        }, rateLimiter, transport);
    }
}
