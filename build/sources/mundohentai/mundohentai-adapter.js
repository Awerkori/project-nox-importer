import { WpGalleryAdapter } from '../common/wpgallery-adapter.js';
export class MundoHentaiAdapter extends WpGalleryAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'mundohentai',
            name: 'Mundo Hentai',
            baseUrl: 'https://mundohentaioficial.com',
            rateLimitRps: 2.0,
        }, rateLimiter, transport);
    }
}
