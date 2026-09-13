import { ZeistMangaAdapter } from '../common/zeistmanga-adapter.js';
export class PinkRosaAdapter extends ZeistMangaAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'pinkrosa',
            name: 'Pink Rosa',
            baseUrl: 'https://scanpinkrosa.blogspot.com',
            seriesCategory: 'Series',
            rateLimitRps: 2.0,
        }, rateLimiter, transport);
    }
}
