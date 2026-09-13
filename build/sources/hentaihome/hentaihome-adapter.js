import { WpGalleryAdapter } from '../common/wpgallery-adapter.js';
export class HentaiHomeAdapter extends WpGalleryAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'hentaihome',
            name: 'Hentai Home',
            baseUrl: 'https://www.hentaihome.net',
            rateLimitRps: 2.0,
        }, rateLimiter, transport);
    }
}
