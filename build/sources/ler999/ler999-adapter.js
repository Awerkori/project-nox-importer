import { ZeistMangaAdapter } from '../common/zeistmanga-adapter.js';
export class Ler999Adapter extends ZeistMangaAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'ler999',
            name: 'Ler 999',
            baseUrl: 'https://ler999.blogspot.com',
            seriesCategory: 'Series',
            rateLimitRps: 2.0,
        }, rateLimiter, transport);
    }
}
