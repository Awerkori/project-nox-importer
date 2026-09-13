import { MadaraAdapter } from '../common/madara-adapter.js';
export class MonteTaiAdapter extends MadaraAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'montetai',
            name: 'Monte Tai',
            baseUrl: 'https://montetaiscanlator.xyz',
            mangaSubString: 'manga',
        }, rateLimiter, transport);
    }
}
