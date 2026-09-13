import { WpGalleryAdapter } from '../common/wpgallery-adapter.js';
export class ZettaHqAdapter extends WpGalleryAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'zettahq',
            name: 'ZettaHQ',
            baseUrl: 'https://zettahq.com',
            rateLimitRps: 2.0,
        }, rateLimiter, transport);
    }
}
