import { MadaraAdapter } from '../common/madara-adapter.js';
export class TankouHentaiAdapter extends MadaraAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'tankouhentai',
            name: 'Tankou Hentai',
            baseUrl: 'https://tankouhentai.com',
            mangaSubString: 'manga',
        }, rateLimiter, transport);
    }
}
