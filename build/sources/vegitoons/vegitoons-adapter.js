import { GreenShitAdapter } from '../common/greenshit-adapter.js';
export class VegitoonsAdapter extends GreenShitAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'vegitoons',
            name: 'Vegitoons',
            baseUrl: 'https://vegitoons.black',
            apiUrl: 'https://api.vegitoons.black',
            cdnUrl: 'https://cdn.vegitoons.black',
            scanId: '1',
            defaultGenreId: '1',
            rateLimitRps: 2.0,
        }, rateLimiter, transport);
    }
}
