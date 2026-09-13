import { WpGalleryAdapter } from '../common/wpgallery-adapter.js';
export class UniversoHentaiAdapter extends WpGalleryAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'universohentai',
            name: 'Universo Hentai',
            baseUrl: 'https://universohentai.com',
            rateLimitRps: 2.0,
        }, rateLimiter, transport);
    }
}
