import { MadaraAdapter } from '../common/madara-adapter.js';
export class MangaOnlineTvAdapter extends MadaraAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'mangaonlinetv',
            name: 'Manga Online tv',
            baseUrl: 'https://mangaonline.tv',
            mangaSubString: 'manga',
            rateLimitRps: 2.0,
        }, rateLimiter, transport);
    }
}
