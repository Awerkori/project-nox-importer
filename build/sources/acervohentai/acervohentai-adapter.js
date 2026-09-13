import { MadaraAdapter } from '../common/madara-adapter.js';
export class AcervoHentaiAdapter extends MadaraAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'acervohentai',
            name: 'Acervo Hentai',
            baseUrl: 'https://acervohentai.com',
            mangaSubString: 'manhwa',
            rateLimitRps: 2.0,
        }, rateLimiter, transport);
    }
}
