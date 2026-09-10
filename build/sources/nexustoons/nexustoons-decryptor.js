import crypto from 'crypto';
export const NEXUS_TOONS_CRYPTO_SECRET = 'OrionNexus2025CryptoKey!Secure';
const NUM_KEYS = 5;
let keys = null;
function initializeKeys() {
    if (keys)
        return keys;
    const derived = [];
    for (let i = 0; i < NUM_KEYS; i++) {
        const pattern = `_orion_key_${i}_v2_${NEXUS_TOONS_CRYPTO_SECRET}`;
        const hash = crypto.createHash('sha256').update(pattern).digest();
        const keyBytes = Buffer.from(hash.toString('hex'), 'hex');
        const sbox = new Uint8Array(256);
        for (let k = 0; k < 256; k++)
            sbox[k] = k;
        let j = 0;
        for (let k = 0; k < 256; k++) {
            j = (j + sbox[k] + keyBytes[k % keyBytes.length]) % 256;
            const tmp = sbox[k];
            sbox[k] = sbox[j];
            sbox[j] = tmp;
        }
        const rsbox = new Uint8Array(256);
        for (let k = 0; k < 256; k++) {
            rsbox[sbox[k]] = k;
        }
        derived.push({ key: keyBytes, sbox, rsbox });
    }
    keys = derived;
    return keys;
}
function rotateRight(byte, shift) {
    const s = shift % 8;
    return ((byte >>> s) | (byte << (8 - s))) & 0xFF;
}
export function isEncryptedNexusToons(data) {
    return (data !== null &&
        typeof data === 'object' &&
        typeof data.d === 'string' &&
        (data.v === 1 || data.v === 2));
}
export function decryptNexusToonsPayload(data) {
    const allKeys = initializeKeys();
    const keyIndex = data.v === 1 ? 0 : (data.k || 0);
    if (keyIndex < 0 || keyIndex >= NUM_KEYS) {
        throw new Error(`Invalid NexusToons key index: ${keyIndex}`);
    }
    const { key, rsbox } = allKeys[keyIndex];
    const input = Buffer.from(data.d, 'base64');
    const output = Buffer.alloc(input.length);
    const keyLen = key.length;
    for (let i = input.length - 1; i >= 0; i--) {
        let byte = input[i];
        if (i > 0) {
            byte ^= input[i - 1];
        }
        else {
            byte ^= key[keyLen - 1];
        }
        byte = rsbox[byte];
        const rotAmount = (((key[(i + 3) % keyLen] + i) & 0xFF) % 7) + 1;
        byte = rotateRight(byte, rotAmount);
        byte ^= key[i % keyLen];
        output[i] = byte;
    }
    const jsonStr = output.toString('utf-8');
    return JSON.parse(jsonStr);
}
