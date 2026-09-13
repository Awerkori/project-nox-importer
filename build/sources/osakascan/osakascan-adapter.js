import { ZeistMangaAdapter } from '../common/zeistmanga-adapter.js';
export class OsakaScanAdapter extends ZeistMangaAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'osakascan',
            name: 'Osaka Scan',
            baseUrl: 'https://www.osakascan.com',
            seriesCategory: 'Series',
            rateLimitRps: 2.0,
        }, rateLimiter, transport);
    }
}
