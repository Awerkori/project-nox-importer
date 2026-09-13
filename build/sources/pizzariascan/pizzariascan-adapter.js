import { MangaWorkAdapter } from '../common/mangawork-adapter.js';
export class PizzariaScanAdapter extends MangaWorkAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'pizzariascan',
            name: 'Pizzaria Scan',
            baseUrl: 'https://pizzariacomics.com',
            seriesPath: 'todas-as-obras',
            rateLimitRps: 2.0,
        }, rateLimiter, transport);
    }
}
