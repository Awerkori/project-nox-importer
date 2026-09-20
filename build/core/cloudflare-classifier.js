export class CloudflareClassifier {
    /**
     * Magic bytes for image formats.
     */
    static isBinaryImage(buffer) {
        const bytes = buffer instanceof Uint8Array ? buffer : new Uint8Array(buffer);
        if (bytes.length < 8)
            return false;
        // JPEG: FF D8 (SOI marker)
        if (bytes[0] === 0xff && bytes[1] === 0xd8)
            return true;
        // PNG: 89 50 4E 47 0D 0A 1A 0A
        if (bytes[0] === 0x89 &&
            bytes[1] === 0x50 &&
            bytes[2] === 0x4e &&
            bytes[3] === 0x47 &&
            bytes[4] === 0x0d &&
            bytes[5] === 0x0a &&
            bytes[6] === 0x1a &&
            bytes[7] === 0x0a) {
            return true;
        }
        // WebP: RIFF .... WEBP
        if (bytes[0] === 0x52 &&
            bytes[1] === 0x49 &&
            bytes[2] === 0x46 &&
            bytes[3] === 0x46 &&
            bytes.length >= 12 &&
            bytes[8] === 0x57 &&
            bytes[9] === 0x45 &&
            bytes[10] === 0x42 &&
            bytes[11] === 0x50) {
            return true;
        }
        // GIF: GIF87a or GIF89a
        if (bytes[0] === 0x47 && bytes[1] === 0x49 && bytes[2] === 0x46 && bytes[3] === 0x38) {
            return true;
        }
        // AVIF / HEIF: ftypavif, ftypavis, ftypmif1, ftypmiaf
        if (bytes.length >= 16 && bytes[4] === 0x66 && bytes[5] === 0x74 && bytes[6] === 0x79 && bytes[7] === 0x70) {
            const brand = String.fromCharCode(...bytes.slice(8, Math.min(bytes.length, 32)));
            if (brand.includes('avif') || brand.includes('avis') || brand.includes('mif1') || brand.includes('miaf')) {
                return true;
            }
        }
        return false;
    }
    /**
     * Classify HTTP response from upstream.
     */
    static inspect(status, headers, bodyText, context) {
        const getHeader = (name) => {
            if (!headers)
                return null;
            if (headers instanceof Headers) {
                return headers.get(name);
            }
            return headers?.[name] || headers?.[name.toLowerCase()] || null;
        };
        const server = (getHeader('server') || '').toLowerCase();
        const cfRay = getHeader('cf-ray');
        const cfMitigated = getHeader('cf-mitigated');
        const retryAfter = getHeader('retry-after');
        const contentType = (getHeader('content-type') || '').toLowerCase();
        let retryAfterSeconds = null;
        if (retryAfter) {
            const parsed = parseInt(retryAfter, 10);
            if (!isNaN(parsed) && parsed > 0) {
                retryAfterSeconds = parsed;
            }
        }
        // Extract text snippet from buffer if available (to inspect challenges even if bodyText was omitted)
        let bufferSnippet = '';
        if (context?.buffer) {
            const b = context.buffer instanceof Uint8Array ? context.buffer : new Uint8Array(context.buffer);
            bufferSnippet = new TextDecoder('utf-8', { fatal: false }).decode(b.slice(0, 8192));
        }
        const combinedBody = ((bodyText || '') + ' ' + bufferSnippet).toLowerCase();
        const isCloudflare = server.includes('cloudflare') ||
            Boolean(cfRay) ||
            server.includes('hcdn') ||
            combinedBody.includes('cloudflare');
        const hasChallengeIndicators = combinedBody.includes('just a moment...') ||
            combinedBody.includes('cf-browser-verification') ||
            combinedBody.includes('/cdn-cgi/challenge-platform') ||
            combinedBody.includes('cf-turnstile') ||
            combinedBody.includes('turnstile.render') ||
            combinedBody.includes('/hcdn-cgi/jschallenge-validate') ||
            combinedBody.includes('challenge-form');
        const isTurnstile = combinedBody.includes('cf-turnstile') ||
            combinedBody.includes('turnstile.render') ||
            combinedBody.includes('challenges.cloudflare.com/turnstile');
        const isJsChallenge = combinedBody.includes('/cdn-cgi/challenge-platform') ||
            combinedBody.includes('cf-browser-verification') ||
            combinedBody.includes('/hcdn-cgi/jschallenge-validate') ||
            (hasChallengeIndicators && !isTurnstile);
        // 1. Fake content detection (e.g. Cloudflare returns HTTP 200 HTML when image or JSON was expected)
        let isFakeContent = false;
        let isValidImage = true;
        if (context?.expectedType === 'image') {
            const isHtml = contentType.includes('text/html') ||
                combinedBody.startsWith('<!doctype') ||
                combinedBody.startsWith('<html') ||
                combinedBody.includes('<title>');
            if (context.buffer) {
                // Strict verification: image MUST match magic bytes of JPEG, PNG, WebP, GIF or AVIF
                isValidImage = this.isBinaryImage(context.buffer);
            }
            else {
                isValidImage = !isHtml && (contentType.startsWith('image/') || !contentType);
            }
            if (isHtml || hasChallengeIndicators || !isValidImage) {
                isFakeContent = true;
            }
        }
        else if (context?.expectedType === 'json') {
            if (contentType.includes('text/html') || hasChallengeIndicators) {
                isFakeContent = true;
            }
        }
        // 2. Determine if blocked or challenge
        const isBlocked = status === 403 || status === 429 || (status === 503 && hasChallengeIndicators) || isFakeContent;
        const isChallenge = hasChallengeIndicators || isTurnstile || isJsChallenge;
        let classification = null;
        let reason = 'OK';
        if (isBlocked || isChallenge) {
            if (isTurnstile) {
                classification = 'TURNSTILE';
                reason = 'Cloudflare Turnstile interactive CAPTCHA challenge detected';
            }
            else if (isJsChallenge) {
                classification = 'JS_CHALLENGE';
                reason = 'Cloudflare/HCDN JavaScript execution challenge required';
            }
            else if (status === 429 || cfMitigated === 'rate-limit' || (status === 403 && retryAfterSeconds !== null)) {
                classification = 'RATE_LIMITED';
                reason = `Upstream rate limit reached (HTTP ${status}, Retry-After: ${retryAfterSeconds ?? 'none'}s)`;
            }
            else if (context?.expectedType === 'image' && status === 403) {
                // Image-specific block
                classification = 'IMAGE_CDN_BLOCK';
                reason = 'Image CDN returned HTTP 403 (anti-hotlink or CDN restriction)';
            }
            else if (context?.expectedType === 'json' && (status === 403 || status === 404)) {
                classification = 'API_BLOCK';
                reason = `API endpoint returned HTTP ${status} (WAF protected or endpoint gated)`;
            }
            else if (context?.isIsolatedRequest && status === 403 && context?.localStatus === 200) {
                classification = 'CLOUDFLARE_DATACENTER_BLOCK';
                reason = 'Datacenter / OVH ASN 16276 blocked by upstream Cloudflare WAF on clean isolated request';
            }
            else if (status === 503) {
                classification = 'TEMPORARY_WAF';
                reason = 'Upstream Cloudflare temporary WAF / Under Attack Mode (HTTP 503)';
            }
            else if (status === 403) {
                if (isCloudflare) {
                    classification = 'CLOUDFLARE_DATACENTER_BLOCK';
                    reason = 'Cloudflare WAF returned HTTP 403 on datacenter egress';
                }
                else {
                    classification = 'UNKNOWN_CLOUDFLARE';
                    reason = `Upstream returned HTTP ${status}`;
                }
            }
            else {
                classification = 'UNKNOWN_CLOUDFLARE';
                reason = `Cloudflare restriction encountered (HTTP ${status})`;
            }
        }
        return {
            isBlocked,
            isChallenge,
            classification,
            cfRay,
            server: server || null,
            retryAfterSeconds,
            isFakeContent,
            isValidImage,
            reason,
        };
    }
}
