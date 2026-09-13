import { MadaraAdapter } from '../common/madara-adapter.js';
export class InkapkAdapter extends MadaraAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'inkapk',
            name: 'Inkapk',
            baseUrl: 'https://inkapk.net',
            mangaSubString: 'obras',
            rateLimitRps: 2.0,
        }, rateLimiter, transport);
    }
}
