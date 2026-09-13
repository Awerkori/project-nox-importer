import { MangaThemesiaAdapter } from '../common/mangathemesia-adapter.js';
export class PointZeroToonsAdapter extends MangaThemesiaAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'pointzerotoons',
            name: 'Point Zero Toons',
            baseUrl: 'https://kitsuneyako.com',
            mangaSubString: 'manga',
            rateLimitRps: 2.0,
        }, rateLimiter, transport);
    }
}
