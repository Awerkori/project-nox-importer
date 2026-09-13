import { MadaraAdapter } from '../common/madara-adapter.js';
export class FleurBlancheAdapter extends MadaraAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'fleurblanche',
            name: 'Fleur Blanche',
            baseUrl: 'https://fbsquadx.com',
            mangaSubString: 'manga',
        }, rateLimiter, transport);
    }
}
