import { createHash } from 'node:crypto';
import { Logger } from '../core/logger.js';
const logger = new Logger('MediaPipeline');
const text = (a, start, length) => String.fromCharCode(...a.slice(start, start + length));
function parseAvifDimensions(a, d) {
    function findBox(start, end, targetType) {
        let offset = start;
        while (offset + 8 <= end) {
            let size = d.getUint32(offset);
            const type = text(a, offset + 4, 4);
            if (size === 1) {
                if (offset + 16 > end)
                    break;
                size = Number(d.getBigUint64(offset + 8));
                offset += 8;
            }
            else if (size === 0) {
                size = end - offset;
            }
            if (size < 8 || offset + size > end)
                break;
            if (type === targetType) {
                return { offset, size };
            }
            if (['meta', 'iprp', 'ipco'].includes(type)) {
                const headerSize = type === 'meta' ? 12 : 8;
                const found = findBox(offset + headerSize, offset + size, targetType);
                if (found)
                    return found;
            }
            offset += size;
        }
        return null;
    }
    const ispe = findBox(0, a.length, 'ispe');
    if (!ispe || ispe.offset + 20 > a.length) {
        throw new Error('AVIF inválido: dimensões não encontradas.');
    }
    return {
        width: d.getUint32(ispe.offset + 12),
        height: d.getUint32(ispe.offset + 16),
    };
}
/**
 * Inspect image bytes directly in binary with bounds and integrity checks.
 * Exact equivalent of Project Nox Manga inspectImage.
 */
export function inspectImage(a) {
    if (a.length < 24 || a.length > 19_000_000) {
        throw new Error('Cada página deve ter no máximo 19 MB e no mínimo 24 bytes.');
    }
    const d = new DataView(a.buffer, a.byteOffset, a.byteLength);
    let width = 0;
    let height = 0;
    let mime;
    if (a[0] === 137 && text(a, 1, 3) === 'PNG' && d.getUint32(4) === 0x0d0a1a0a) {
        mime = 'image/png';
        if (text(a, 12, 4) !== 'IHDR' || d.getUint32(8) !== 13) {
            throw new Error('PNG inválido.');
        }
        width = d.getUint32(16);
        height = d.getUint32(20);
        let offset = 8;
        let idat = false;
        let end = false;
        while (offset + 12 <= a.length) {
            const len = d.getUint32(offset);
            const type = text(a, offset + 4, 4);
            if (offset + len + 12 > a.length)
                throw new Error('PNG incompleto.');
            if (type === 'IDAT')
                idat = true;
            if (type === 'IEND') {
                end = len === 0 && offset + 12 === a.length;
                break;
            }
            offset += len + 12;
        }
        if (!idat || !end)
            throw new Error('PNG incompleto.');
    }
    else if (a[0] === 255 && a[1] === 216 && a[a.length - 2] === 255 && a[a.length - 1] === 217) {
        mime = 'image/jpeg';
        let offset = 2;
        while (offset + 4 < a.length) {
            if (a[offset] !== 255)
                throw new Error('JPEG inválido.');
            const marker = a[offset + 1];
            if (marker === 0xda)
                break; // Start of scan
            const len = d.getUint16(offset + 2);
            if (len < 2 || offset + 2 + len > a.length)
                throw new Error('JPEG inválido.');
            if ([0xc0, 0xc1, 0xc2].includes(marker)) {
                height = d.getUint16(offset + 5);
                width = d.getUint16(offset + 7);
                break;
            }
            offset += 2 + len;
        }
    }
    else if (text(a, 0, 4) === 'RIFF' && text(a, 8, 4) === 'WEBP' && d.getUint32(4, true) + 8 === a.length) {
        mime = 'image/webp';
        const format = text(a, 12, 4);
        if (format === 'VP8X' && a.length >= 30) {
            if (a[20] & 2)
                throw new Error('Imagens animadas não são aceitas.');
            width = 1 + a[24] + (a[25] << 8) + (a[26] << 16);
            height = 1 + a[27] + (a[28] << 8) + (a[29] << 16);
        }
        else if (format === 'VP8 ' && a.length >= 30 && a[23] === 0x9d && a[24] === 1 && a[25] === 0x2a) {
            width = d.getUint16(26, true) & 0x3fff;
            height = d.getUint16(28, true) & 0x3fff;
        }
        else if (format === 'VP8L' && a.length >= 25 && a[20] === 0x2f) {
            const bits = d.getUint32(21, true);
            width = (bits & 0x3fff) + 1;
            height = ((bits >> 14) & 0x3fff) + 1;
        }
    }
    else if (text(a, 0, 3) === 'GIF' && (text(a, 3, 3) === '89a' || text(a, 3, 3) === '87a')) {
        mime = 'image/gif';
        width = d.getUint16(6, true);
        height = d.getUint16(8, true);
    }
    else if (a.length >= 16 && text(a, 4, 4) === 'ftyp') {
        const ftypLen = d.getUint32(0);
        const majorBrand = text(a, 8, 4);
        let isAvif = majorBrand === 'avif' || majorBrand === 'avis';
        if (!isAvif) {
            const maxCheck = Math.min(ftypLen, a.length, 64);
            for (let offset = 16; offset + 4 <= maxCheck; offset += 4) {
                const brand = text(a, offset, 4);
                if (brand === 'avif' || brand === 'avis') {
                    isAvif = true;
                    break;
                }
            }
        }
        if (isAvif) {
            mime = 'image/avif';
            const dims = parseAvifDimensions(a, d);
            width = dims.width;
            height = dims.height;
        }
        else {
            throw new Error('Formato não permitido. Use PNG, JPEG, WebP, GIF ou AVIF.');
        }
    }
    else {
        throw new Error('Formato não permitido. Use PNG, JPEG, WebP, GIF ou AVIF.');
    }
    if (!width || !height || width > 10000 || height > 60000 || width * height > 80_000_000) {
        throw new Error(`Dimensões inválidas ou imagem muito grande (${width}x${height}).`);
    }
    return { mime, width, height };
}
export function calculateSha256(bytes) {
    return createHash('sha256').update(bytes).digest('hex');
}
/**
 * Process a single image:
 * 1. Validate binary structure & dimensions.
 * 2. Calculate SHA-256.
 * 3. Check public.media for existing hash (deduplication).
 * 4. If not found, upload via StorageProvider and insert into public.media.
 */
export async function processAndStoreMedia(supabase, storage, bytes, userId, purpose = 'editorial') {
    const info = inspectImage(bytes);
    const sha256 = calculateSha256(bytes);
    // 1. Deduplication lookup
    const { data: existing, error: findErr } = await supabase
        .from('media')
        .select('id, width, height, bytes, mime')
        .eq('sha256', sha256)
        .eq('storage_ready', true)
        .limit(1)
        .maybeSingle();
    if (findErr) {
        logger.warn('Media hash lookup error', { error: findErr.message });
    }
    if (existing) {
        logger.debug('Media deduplicated via SHA-256', { id: existing.id, sha256 });
        return {
            mediaId: existing.id,
            reused: true,
            width: existing.width,
            height: existing.height,
            bytes: existing.bytes,
            mime: existing.mime,
        };
    }
    // 2. Upload to storage provider
    const mediaId = crypto.randomUUID();
    const providerKey = await storage.upload(bytes, info.mime, mediaId);
    // 3. Insert record into public.media
    const { error: insertErr } = await supabase.from('media').insert({
        id: mediaId,
        provider: storage.getProviderKey() === 'mock' ? 'telegram' : storage.getProviderKey(),
        provider_key: providerKey,
        mime: info.mime,
        width: info.width,
        height: info.height,
        bytes: bytes.length,
        sha256,
        created_by: userId,
        storage_ready: true,
        purpose,
    });
    if (insertErr) {
        logger.error('Failed to register media in database', { error: insertErr.message, mediaId });
        throw new Error(`Media registration failed: ${insertErr.message}`);
    }
    return {
        mediaId,
        reused: false,
        width: info.width,
        height: info.height,
        bytes: bytes.length,
        mime: info.mime,
    };
}
