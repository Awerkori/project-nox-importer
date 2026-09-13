import { WpGalleryAdapter } from '../common/wpgallery-adapter.js';
export class HentaiTokyoAdapter extends WpGalleryAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'hentaitokyo',
            name: 'Hentai Tokyo',
            baseUrl: 'https://hentaitokyo.net',
            rateLimitRps: 2.0,
        }, rateLimiter, transport);
    }
}
