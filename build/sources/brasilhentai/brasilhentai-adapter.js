import { WpGalleryAdapter } from '../common/wpgallery-adapter.js';
export class BrasilHentaiAdapter extends WpGalleryAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'brasilhentai',
            name: 'Brasil Hentai',
            baseUrl: 'https://brasilhentai.com',
            rateLimitRps: 2.0,
        }, rateLimiter, transport);
    }
}
