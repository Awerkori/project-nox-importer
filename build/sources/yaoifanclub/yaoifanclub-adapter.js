import { MadaraAdapter } from '../common/madara-adapter.js';
export class YaoiFanClubAdapter extends MadaraAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'yaoifanclub',
            name: 'Yaoi Fan Club',
            baseUrl: 'https://yaoifanclub.com',
            mangaSubString: 'obra',
            rateLimitRps: 2.0,
        }, rateLimiter, transport);
    }
}
