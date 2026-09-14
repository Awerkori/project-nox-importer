import { MadaraAdapter } from '../common/madara-adapter.js';
// NOTE: mangaonline.red permanently redirected to mangaonline.love (verified 2026-09-14)
// Site is currently TEMPORARILY_UNAVAILABLE - mangaonline.love times out from datacenter.
// Adapter updated to use new domain so recovery is automatic when site returns.
export class MangaOnlineAdapter extends MadaraAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'mangaonline',
            name: 'Manga Online',
            baseUrl: 'https://mangaonline.love',
            mangaSubString: 'manga',
            rateLimitRps: 1.5,
        }, rateLimiter, transport);
    }
}
