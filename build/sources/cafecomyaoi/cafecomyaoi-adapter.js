import { MadaraAdapter } from '../common/madara-adapter.js';
export class CafeComYaoiAdapter extends MadaraAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'cafecomyaoi',
            name: 'Café com Yaoi',
            baseUrl: 'https://cafecomyaoi.com.br',
            mangaSubString: 'manga',
        }, rateLimiter, transport);
    }
}
