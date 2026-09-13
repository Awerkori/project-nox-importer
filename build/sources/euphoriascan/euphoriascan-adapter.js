import { MadaraAdapter } from '../common/madara-adapter.js';
export class EuphoriaScanAdapter extends MadaraAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'euphoriascan',
            name: 'Euphoria Scan',
            baseUrl: 'https://euphoriascan.com',
            mangaSubString: 'manga',
        }, rateLimiter, transport);
    }
}
