import { MadaraAdapter } from '../common/madara-adapter.js';
export class NocturneSummerAdapter extends MadaraAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'nocturnesummer',
            name: 'Nocturne Summer',
            baseUrl: 'https://nocfsb.com',
            mangaSubString: 'manga',
        }, rateLimiter, transport);
    }
}
