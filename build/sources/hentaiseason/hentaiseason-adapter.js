import { WpGalleryAdapter } from '../common/wpgallery-adapter.js';
export class HentaiSeasonAdapter extends WpGalleryAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'hentaiseason',
            name: 'Hentai Season',
            baseUrl: 'https://hentaiseason.com',
            rateLimitRps: 2.0,
        }, rateLimiter, transport);
    }
}
