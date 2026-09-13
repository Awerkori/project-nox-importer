import { ZeistMangaAdapter } from '../common/zeistmanga-adapter.js';
export class ApenasUmaFaAdapter extends ZeistMangaAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'apenasumafa',
            name: 'Apenas Uma Fã',
            baseUrl: 'https://apenasuma-fa.blogspot.com',
            seriesCategory: 'Series',
            rateLimitRps: 2.0,
        }, rateLimiter, transport);
    }
}
