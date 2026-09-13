import { MadaraAdapter } from '../common/madara-adapter.js';
export class AmuyAdapter extends MadaraAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'amuy',
            name: 'AMUY Scan',
            baseUrl: 'https://www.apenasmaisumyaoi.com',
            mangaSubString: 'manga',
            rateLimitRps: 2.0,
        }, rateLimiter, transport);
    }
}
