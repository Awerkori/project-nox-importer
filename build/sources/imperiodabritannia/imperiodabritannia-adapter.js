import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { HostRateLimiter } from '../../core/rate-limiter.js';
import { Logger } from '../../core/logger.js';
import { decryptMangoPayload } from '../mangotoons/mangotoons-decryptor.js';
function normalizeText(text) {
    return text
        .normalize('NFKD')
        .replace(/[\u0300-\u036f]/g, '')
        .normalize('NFC')
        .toLowerCase()
        .replace(/[^\p{L}\p{N}]+/gu, ' ')
        .trim();
}
const BASE_URL = 'https://imperiodabritannia.net';
const API_URL = 'https://api.imperiodabritannia.net/api';
const CDN_URL = 'https://cdn.imperiodabritannia.net';
const MANGO_KEY = 'mangotoons_encryption_key_2025';
function slugify(value) {
    return value.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase()
        .replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '');
}
function asUrl(value, cdn = CDN_URL) {
    if (typeof value !== 'string' || !value.trim())
        return null;
    return value.startsWith('http') ? value : `${cdn}/${value.replace(/^\/+/, '')}`;
}
function chapterNumber(value) {
    const parsed = Number.parseFloat(String(value ?? ''));
    return Number.isFinite(parsed) ? parsed : 0;
}
/**
 * Current implementation of the public MangoTheme API used by Sagrado
 * Império da Britannia. The token is an optional public cache hint exposed by
 * its own `env` asset; the adapter continues to work when that hint rotates.
 */
export class ImperioDaBritanniaAdapter {
    rateLimiter;
    transport;
    id = 'imperiodabritannia';
    name = 'Sagrado Império da Britannia';
    baseUrl = BASE_URL;
    admissionProbe = {
        url: `${API_URL}/obras?pagina=1&limite=1`,
        headers: { Accept: 'application/json', 'X-Brit-Cache': 'true', 'X-Noencryptionbritta': '1' },
        expectedType: 'json',
    };
    apiToken = null;
    tokenFetchedAt = 0;
    logger = new Logger('ImperioDaBritanniaAdapter');
    catalogItems = [];
    catalogByNormalizedTitle = new Map();
    catalogBySlug = new Map();
    catalogById = new Map();
    catalogLoaded = false;
    constructor(rateLimiter = new HostRateLimiter(4), transport = fetch) {
        this.rateLimiter = rateLimiter;
        this.transport = transport;
        this.rateLimiter.setHostRate(new URL(API_URL).host, 4, 8, 8);
        this.rateLimiter.setHostRate(new URL(CDN_URL).host, 4, 8, 8);
    }
    ensureCatalogLoaded() {
        if (this.catalogLoaded)
            return true;
        try {
            const currentDir = typeof __dirname !== 'undefined'
                ? __dirname
                : path.dirname(fileURLToPath(import.meta.url));
            const catalogPath = path.resolve(currentDir, 'imperiodabritannia-catalog.json');
            if (fs.existsSync(catalogPath)) {
                const raw = fs.readFileSync(catalogPath, 'utf8');
                this.catalogItems = JSON.parse(raw);
                for (const item of this.catalogItems) {
                    this.catalogById.set(String(item.id), item);
                    if (item.slug)
                        this.catalogBySlug.set(String(item.slug), item);
                    const titles = [item.nome, item.title, item.titulo_alternativo, item.titulos_alternativos].filter(Boolean);
                    for (const title of titles) {
                        const norm = normalizeText(String(title));
                        if (norm) {
                            const list = this.catalogByNormalizedTitle.get(norm) || [];
                            list.push(item);
                            this.catalogByNormalizedTitle.set(norm, list);
                        }
                    }
                }
                this.catalogLoaded = true;
                this.logger.info(`Loaded ImperioDaBritannia local catalog index with ${this.catalogItems.length} works`);
                return true;
            }
        }
        catch (err) {
            this.logger.warn('Failed to load local ImperioDaBritannia catalog cache', { error: err?.message });
        }
        return false;
    }
    async refreshApiToken(force = false) {
        if (!force && this.apiToken && Date.now() - this.tokenFetchedAt < 30 * 60_000)
            return this.apiToken;
        try {
            const page = await this.transport(BASE_URL, { signal: AbortSignal.timeout(15_000) });
            const html = await page.text();
            const envPath = html.match(/<link[^>]+href=["']([^"']*env[^"']*)/i)?.[1];
            if (!envPath)
                return this.apiToken;
            const envUrl = new URL(envPath, BASE_URL).href;
            const asset = await this.transport(envUrl, { signal: AbortSignal.timeout(15_000) });
            const text = await asset.text();
            const token = text.split('apiToken:')[1]?.split(String.fromCharCode(96))[0]
                ?.replace(/^[^A-Za-z0-9_-]+/, '').trim();
            if (token)
                this.apiToken = token;
            this.tokenFetchedAt = Date.now();
        }
        catch (error) {
            this.logger.warn('Could not refresh Britannia public API token', { error: error?.message });
        }
        return this.apiToken;
    }
    async request(path, retry = true) {
        const url = path.startsWith('http') ? path : `${API_URL}${path}`;
        const host = new URL(url).host;
        await this.rateLimiter.acquire(host);
        const token = await this.refreshApiToken();
        const response = await this.transport(url, {
            headers: {
                Accept: 'application/json',
                'X-Brit-Cache': 'true',
                'X-Noencryptionbritta': '1',
                ...(token ? { 'X-API-Token': token } : {}),
            },
            signal: AbortSignal.timeout(20_000),
        });
        if ((response.status === 401 || response.status === 403) && retry) {
            await this.refreshApiToken(true);
            return this.request(path, false);
        }
        if (!response.ok)
            throw new Error(`Britannia HTTP ${response.status} for ${url}`);
        this.rateLimiter.recordSuccess(host);
        const text = await response.text();
        if (response.headers.get('x-encrypted') === 'true') {
            return decryptMangoPayload(text, MANGO_KEY);
        }
        return JSON.parse(text);
    }
    toSummary(work) {
        const title = String(work.nome || work.title || `Obra ${work.id}`);
        return {
            sourceWorkId: String(work.id),
            title,
            slug: String(work.slug || slugify(title)),
            coverUrl: asUrl(work.imagem || work.coverImage || work.banner),
            updatedAt: work.atualizada_em || work.feed_at || undefined,
        };
    }
    async fetchUpdatedWorks(cursor, options) {
        const page = Math.max(1, Number.parseInt(cursor || '1', 10) || 1);
        const endpoint = options?.mode === 'maintenance'
            ? `/capitulos/recentes?pagina=${page}&limite=24`
            : `/obras?pagina=${page}&limite=24`;
        const response = await this.request(endpoint);
        const works = (response.obras || response.dados || response.data || [])
            .filter((work) => work?.id)
            .map((work) => this.toSummary(work));
        return { works, nextCursor: response.pagination?.hasNextPage ? String(page + 1) : null };
    }
    searchCatalog(query) {
        const norm = normalizeText(query);
        const slug = slugify(query);
        const matches = [];
        const seen = new Set();
        const bySlug = this.catalogBySlug.get(slug);
        if (bySlug && !seen.has(String(bySlug.id))) {
            matches.push(bySlug);
            seen.add(String(bySlug.id));
        }
        const exact = this.catalogByNormalizedTitle.get(norm) || [];
        for (const item of exact) {
            if (!seen.has(String(item.id))) {
                matches.push(item);
                seen.add(String(item.id));
            }
        }
        if (matches.length < 5 && norm.length >= 4) {
            for (const item of this.catalogItems) {
                const idStr = String(item.id);
                if (seen.has(idStr))
                    continue;
                const itemNorm = normalizeText(String(item.nome || item.title || ''));
                const altNorm = item.titulo_alternativo ? normalizeText(String(item.titulo_alternativo)) : '';
                const matched = (itemNorm.length >= 4 && (itemNorm.includes(norm) || norm.includes(itemNorm))) ||
                    (altNorm.length >= 4 && (altNorm.includes(norm) || norm.includes(altNorm)));
                if (matched) {
                    matches.push(item);
                    seen.add(idStr);
                    if (matches.length >= 10)
                        break;
                }
            }
        }
        return matches.map((work) => this.toSummary(work));
    }
    async searchWorks(query) {
        const clean = query.trim();
        if (!clean)
            return [];
        if (this.transport === fetch && this.ensureCatalogLoaded()) {
            return this.searchCatalog(clean);
        }
        const response = await this.request(`/obras?pagina=1&limite=20&busca=${encodeURIComponent(query)}`);
        return (response.obras || response.dados || response.data || [])
            .filter((work) => work?.id)
            .map((work) => this.toSummary(work));
    }
    async work(sourceWorkId) {
        if (this.transport === fetch && this.ensureCatalogLoaded()) {
            const cached = this.catalogById.get(String(sourceWorkId)) || this.catalogBySlug.get(String(sourceWorkId));
            if (cached)
                return cached;
        }
        const response = await this.request(`/obras/${encodeURIComponent(sourceWorkId)}`);
        return response.obra || response.dados || response.data || response;
    }
    async fetchWorkDetails(sourceWorkId) {
        const work = await this.work(sourceWorkId);
        const title = String(work.nome || work.title || sourceWorkId);
        const status = String(work.status_nome || '').toLowerCase();
        const rawAlt = work.titulo_alternativo || work.titulos_alternativos || work.nome_alternativo || work.alternative_titles || work.alternativeTitles || work.aliases;
        let alternativeTitles;
        if (Array.isArray(rawAlt)) {
            alternativeTitles = rawAlt.map(String).map((s) => s.trim()).filter(Boolean);
        }
        else if (typeof rawAlt === 'string' && rawAlt.trim().length > 0) {
            const trimmed = rawAlt.trim();
            if (trimmed.startsWith('[') && trimmed.endsWith(']')) {
                try {
                    const parsed = JSON.parse(trimmed);
                    if (Array.isArray(parsed)) {
                        alternativeTitles = parsed.map(String).map((s) => s.trim()).filter(Boolean);
                    }
                }
                catch {
                    // Fallback to text delimiters below
                }
            }
            if (!alternativeTitles || alternativeTitles.length === 0) {
                if (/[\r\n]+/.test(trimmed)) {
                    alternativeTitles = trimmed.split(/[\r\n]+/).map((s) => s.trim()).filter(Boolean);
                }
                else if (/;+/.test(trimmed)) {
                    alternativeTitles = trimmed.split(/;+/).map((s) => s.trim()).filter(Boolean);
                }
                else if (/\s+\/\s+/.test(trimmed)) {
                    alternativeTitles = trimmed.split(/\s+\/\s+/).map((s) => s.trim()).filter(Boolean);
                }
                else {
                    alternativeTitles = [trimmed];
                }
            }
        }
        return {
            sourceWorkId: String(work.id || sourceWorkId),
            title,
            slug: String(work.slug || slugify(title)),
            coverUrl: asUrl(work.imagem || work.coverImage || work.banner),
            synopsis: typeof work.descricao === 'string' ? work.descricao : undefined,
            status: status.includes('andamento') || status.includes('ativo') ? 'ONGOING'
                : status.includes('conclu') ? 'COMPLETED'
                    : status.includes('hiato') || status.includes('paus') ? 'HIATUS'
                        : status.includes('cancel') ? 'CANCELLED' : 'UNKNOWN',
            genres: (work.tags || []).map((tag) => typeof tag === 'string' ? tag : tag.nome || tag.name).filter(Boolean),
            alternativeTitles: alternativeTitles && alternativeTitles.length > 0 ? alternativeTitles : undefined,
            raw: work,
        };
    }
    async fetchChapters(sourceWorkId) {
        const work = await this.work(sourceWorkId);
        return (work.capitulos || []).map((chapter) => {
            const display = String(chapter.numero ?? chapter.id);
            return {
                sourceChapterId: `${sourceWorkId}:${encodeURIComponent(display)}`,
                displayNumber: display,
                number: chapterNumber(display),
                title: chapter.nome || chapter.title || undefined,
                createdAt: chapter.criado_em || chapter.atualizado_em || undefined,
                pageCount: chapter.total_paginas ?? null,
            };
        }).sort((a, b) => a.number - b.number);
    }
    isChapterOwnedByWork(sourceWorkId, sourceChapterId) {
        return sourceChapterId.startsWith(`${sourceWorkId}:`);
    }
    async fetchChapterPages(sourceChapterId) {
        const separator = sourceChapterId.indexOf(':');
        if (separator <= 0)
            return [];
        const workId = sourceChapterId.slice(0, separator);
        const number = decodeURIComponent(sourceChapterId.slice(separator + 1));
        const response = await this.request(`/obras/${encodeURIComponent(workId)}/capitulos/${encodeURIComponent(number)}`);
        const chapter = response.capitulo || response.dados || response.data || response;
        return (chapter.paginas || []).sort((a, b) => Number(a.numero || 0) - Number(b.numero || 0))
            .map((page) => asUrl(page.url || page.cdn_id || page.imagem || page.image))
            .filter((url) => Boolean(url));
    }
    getImageHeaders() {
        return { Referer: `${BASE_URL}/` };
    }
}
