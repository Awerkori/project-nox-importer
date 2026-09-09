import crypto from 'node:crypto';
const HOSTNAME_PART = 'kuromangas.com::v2';
const ANTIBOT = 'x9_4v2_b';
export const DEFAULT_ENC_KEY = 'i67ato8l6sai74jyIHfE2oMmieshoforanuYTusF4jKdqEwhUEft9dsadcxzsaipnjm8';
function uadd(a, b) {
    return (a + b) >>> 0;
}
function uless(a, b) {
    return (a >>> 0) < (b >>> 0);
}
function rotl(x, n) {
    return ((x << n) | (x >>> (32 - n))) >>> 0;
}
/**
 * Rabbit Stream Cipher (RFC 4503)
 */
export class RabbitCipher {
    x = new Uint32Array(8);
    c = new Uint32Array(8);
    b = 0;
    setup(key, iv) {
        if (key.length < 16)
            throw new Error('Key must be at least 16 bytes');
        const kw = new Uint32Array(4);
        for (let i = 0; i < 4; i++) {
            kw[i] = key.readUInt32LE(i * 4);
        }
        this.x[0] = kw[0];
        this.x[1] = ((kw[3] << 16) | (kw[2] >>> 16)) >>> 0;
        this.x[2] = kw[1];
        this.x[3] = ((kw[0] << 16) | (kw[3] >>> 16)) >>> 0;
        this.x[4] = kw[2];
        this.x[5] = ((kw[1] << 16) | (kw[0] >>> 16)) >>> 0;
        this.x[6] = kw[3];
        this.x[7] = ((kw[2] << 16) | (kw[1] >>> 16)) >>> 0;
        this.c[0] = ((kw[2] << 16) | (kw[2] >>> 16)) >>> 0;
        this.c[1] = ((kw[0] & 0xffff0000) | (kw[1] & 0x0000ffff)) >>> 0;
        this.c[2] = ((kw[3] << 16) | (kw[3] >>> 16)) >>> 0;
        this.c[3] = ((kw[1] & 0xffff0000) | (kw[2] & 0x0000ffff)) >>> 0;
        this.c[4] = ((kw[0] << 16) | (kw[0] >>> 16)) >>> 0;
        this.c[5] = ((kw[2] & 0xffff0000) | (kw[3] & 0x0000ffff)) >>> 0;
        this.c[6] = ((kw[1] << 16) | (kw[1] >>> 16)) >>> 0;
        this.c[7] = ((kw[3] & 0xffff0000) | (kw[0] & 0x0000ffff)) >>> 0;
        this.b = 0;
        for (let i = 0; i < 4; i++) {
            this.nextState();
        }
        for (let i = 0; i < 8; i++) {
            this.c[i] = (this.c[i] ^ this.x[(i + 4) & 7]) >>> 0;
        }
        if (iv && iv.length >= 8) {
            const iv0 = iv.readUInt32LE(0);
            const iv1 = iv.readUInt32LE(4);
            const i0 = iv0;
            const i2 = iv1;
            const i1 = ((i0 >>> 16) | (i2 & 0xffff0000)) >>> 0;
            const i3 = ((i2 << 16) | (i0 & 0x0000ffff)) >>> 0;
            this.c[0] = (this.c[0] ^ i0) >>> 0;
            this.c[1] = (this.c[1] ^ i1) >>> 0;
            this.c[2] = (this.c[2] ^ i2) >>> 0;
            this.c[3] = (this.c[3] ^ i3) >>> 0;
            this.c[4] = (this.c[4] ^ i0) >>> 0;
            this.c[5] = (this.c[5] ^ i1) >>> 0;
            this.c[6] = (this.c[6] ^ i2) >>> 0;
            this.c[7] = (this.c[7] ^ i3) >>> 0;
            for (let i = 0; i < 4; i++) {
                this.nextState();
            }
        }
    }
    nextState() {
        const cOld = new Uint32Array(this.c);
        this.c[0] = uadd(uadd(this.c[0], 0x4d34d34d), this.b);
        this.c[1] = uadd(uadd(this.c[1], 0xd34d34d3), uless(this.c[0], cOld[0]) ? 1 : 0);
        this.c[2] = uadd(uadd(this.c[2], 0x34d34d34), uless(this.c[1], cOld[1]) ? 1 : 0);
        this.c[3] = uadd(uadd(this.c[3], 0x4d34d34d), uless(this.c[2], cOld[2]) ? 1 : 0);
        this.c[4] = uadd(uadd(this.c[4], 0xd34d34d3), uless(this.c[3], cOld[3]) ? 1 : 0);
        this.c[5] = uadd(uadd(this.c[5], 0x34d34d34), uless(this.c[4], cOld[4]) ? 1 : 0);
        this.c[6] = uadd(uadd(this.c[6], 0x4d34d34d), uless(this.c[5], cOld[5]) ? 1 : 0);
        this.c[7] = uadd(uadd(this.c[7], 0xd34d34d3), uless(this.c[6], cOld[6]) ? 1 : 0);
        this.b = uless(this.c[7], cOld[7]) ? 1 : 0;
        const g = new Uint32Array(8);
        for (let i = 0; i < 8; i++) {
            const gx = uadd(this.x[i], this.c[i]);
            const gxBig = BigInt(gx);
            const sq = gxBig * gxBig;
            g[i] = Number((sq ^ (sq >> 32n)) & 0xffffffffn) >>> 0;
        }
        this.x[0] = uadd(uadd(g[0], rotl(g[7], 16)), rotl(g[6], 16));
        this.x[1] = uadd(uadd(g[1], rotl(g[0], 8)), g[7]);
        this.x[2] = uadd(uadd(g[2], rotl(g[1], 16)), rotl(g[0], 16));
        this.x[3] = uadd(uadd(g[3], rotl(g[2], 8)), g[1]);
        this.x[4] = uadd(uadd(g[4], rotl(g[3], 16)), rotl(g[2], 16));
        this.x[5] = uadd(uadd(g[5], rotl(g[4], 8)), g[3]);
        this.x[6] = uadd(uadd(g[6], rotl(g[5], 16)), rotl(g[4], 16));
        this.x[7] = uadd(uadd(g[7], rotl(g[6], 8)), g[5]);
    }
    crypt(data) {
        const out = Buffer.from(data);
        const wordsSize = Math.floor((out.length + 3) / 4);
        const words = new Uint32Array(wordsSize);
        for (let i = 0; i < wordsSize; i++) {
            let word = 0;
            for (let j = 0; j < 4; j++) {
                const byteIdx = i * 4 + j;
                if (byteIdx < out.length) {
                    word |= (out[byteIdx] & 0xff) << (j * 8);
                }
            }
            words[i] = word >>> 0;
        }
        let idx = 0;
        while (idx < words.length) {
            this.nextState();
            const s0 = (this.x[0] ^ (this.x[5] >>> 16) ^ (this.x[3] << 16)) >>> 0;
            const s1 = (this.x[2] ^ (this.x[7] >>> 16) ^ (this.x[5] << 16)) >>> 0;
            const s2 = (this.x[4] ^ (this.x[1] >>> 16) ^ (this.x[7] << 16)) >>> 0;
            const s3 = (this.x[6] ^ (this.x[3] >>> 16) ^ (this.x[1] << 16)) >>> 0;
            if (idx < words.length)
                words[idx] ^= s0;
            if (idx + 1 < words.length)
                words[idx + 1] ^= s1;
            if (idx + 2 < words.length)
                words[idx + 2] ^= s2;
            if (idx + 3 < words.length)
                words[idx + 3] ^= s3;
            idx += 4;
        }
        for (let byteIdx = 0; byteIdx < out.length; byteIdx++) {
            const wordIdx = Math.floor(byteIdx / 4);
            const shift = (byteIdx % 4) * 8;
            out[byteIdx] = (words[wordIdx] >>> shift) & 0xff;
        }
        return out;
    }
}
/**
 * OpenSSL EVP_BytesToKey key derivation using MD5
 */
export function evpBytesToKey(password, salt, keyLen = 16, ivLen = 8) {
    const derived = Buffer.alloc(keyLen + ivLen);
    let derivedPos = 0;
    let md5Hash = Buffer.alloc(0);
    while (derivedPos < derived.length) {
        const md = crypto.createHash('md5');
        if (md5Hash.length > 0)
            md.update(md5Hash);
        md.update(password);
        md.update(salt);
        md5Hash = md.digest();
        const toCopy = Math.min(md5Hash.length, derived.length - derivedPos);
        md5Hash.copy(derived, derivedPos, 0, toCopy);
        derivedPos += toCopy;
    }
    return {
        key: derived.subarray(0, keyLen),
        iv: derived.subarray(keyLen, keyLen + ivLen),
    };
}
export function derivePassword(dateStr, encKey = DEFAULT_ENC_KEY) {
    const d = dateStr || new Date().toISOString().split('T')[0];
    const toHash = `${d}${HOSTNAME_PART}${ANTIBOT}`;
    const md5Part = crypto.createHash('md5').update(toHash).digest('hex').substring(0, 8);
    return encKey + md5Part;
}
export function decryptVSecure(vSecure, dataKey, encKey = DEFAULT_ENC_KEY) {
    const password = derivePassword(undefined, encKey);
    const encrypted = Buffer.from(vSecure, 'base64');
    if (encrypted.length < 16) {
        throw new Error('Encrypted payload too short');
    }
    const salt = encrypted.subarray(8, 16);
    const ciphertext = encrypted.subarray(16);
    const { key, iv } = evpBytesToKey(Buffer.from(password, 'utf8'), salt);
    const cipher = new RabbitCipher();
    cipher.setup(key, iv);
    const plaintext = cipher.crypt(ciphertext);
    const jsonStr = plaintext.toString('utf8');
    const parsed = JSON.parse(jsonStr);
    return dataKey && parsed[dataKey] !== undefined ? parsed[dataKey] : parsed;
}
