import { MadaraAdapter } from '../common/madara-adapter.js';
export class YuriVersoAdapter extends MadaraAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'yuriverso',
            name: 'Yuri on Air',
            baseUrl: 'https://yurionair.top',
            mangaSubString: 'manga',
            rateLimitRps: 2.0,
        }, rateLimiter, transport);
    }
}
