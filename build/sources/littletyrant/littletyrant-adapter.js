import { MadaraAdapter } from '../common/madara-adapter.js';
export class LittleTyrantAdapter extends MadaraAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'littletyrant',
            name: 'Little Tyrant',
            baseUrl: 'https://tiraninha.world',
            mangaSubString: 'manga',
        }, rateLimiter, transport);
    }
}
