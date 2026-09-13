import { WpGalleryAdapter } from '../common/wpgallery-adapter.js';
export class HentaiFusionAdapter extends WpGalleryAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'hentaifusion',
            name: 'Hentai Fusion',
            baseUrl: 'https://hentaifusion.me',
            rateLimitRps: 2.0,
        }, rateLimiter, transport);
    }
}
