import { WpGalleryAdapter } from '../common/wpgallery-adapter.js';
export class NHentaiBrAdapter extends WpGalleryAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'nhentaibr',
            name: 'NHentai.net.br',
            baseUrl: 'https://nhentai.net.br',
            rateLimitRps: 2.0,
        }, rateLimiter, transport);
    }
}
