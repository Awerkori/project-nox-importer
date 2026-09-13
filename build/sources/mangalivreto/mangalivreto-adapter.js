import { MadaraAdapter } from '../common/madara-adapter.js';
export class MangaLivreToAdapter extends MadaraAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'mangalivreto',
            name: 'Manga Livre.to',
            baseUrl: 'https://mangalivre.to',
            mangaSubString: 'manga',
        }, rateLimiter, transport);
    }
}
