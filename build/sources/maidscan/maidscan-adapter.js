import { GreenShitAdapter } from '../common/greenshit-adapter.js';
export class MaidScanAdapter extends GreenShitAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'maidscan',
            name: 'Maid Scan',
            baseUrl: 'https://empreguetes.wtf',
            apiUrl: 'https://api.verdinha.wtf',
            cdnUrl: 'https://cdn.verdinha.wtf',
            scanId: '3',
            defaultGenreId: '4',
            rateLimitRps: 2.0,
        }, rateLimiter, transport);
    }
}
