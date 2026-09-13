import { MadaraAdapter } from '../common/madara-adapter.js';
export class MangaOnlineAdapter extends MadaraAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'mangaonline',
            name: 'Manga Online RED',
            baseUrl: 'https://mangaonline.red',
            mangaSubString: 'manga',
            rateLimitRps: 2.0,
        }, rateLimiter, transport);
    }
}
