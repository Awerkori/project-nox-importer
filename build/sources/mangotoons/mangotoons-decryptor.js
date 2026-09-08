import crypto from 'node:crypto';
export const DEFAULT_MANGOTOONS_ENC_KEY = process.env.MANGOTOONS_ENC_KEY || 'abmPisXlFjOLVTnYhbYQTpkWJtOGKwVttzLqstfjRBNVaEtQYG';
export const DEFAULT_MANGOTOONS_SALT = process.env.MANGOTOONS_SALT || 'salt';
/**
 * Decrypts MangoTheme encrypted payloads (AES-256-CBC with SHA-256 key derivation)
 */
export function decryptMangoPayload(payload, keyString = DEFAULT_MANGOTOONS_ENC_KEY, salt = DEFAULT_MANGOTOONS_SALT) {
    const trimmed = payload.trim();
    if (trimmed.startsWith('{') || trimmed.startsWith('[')) {
        return JSON.parse(trimmed);
    }
    const parts = trimmed.split(':', 2);
    if (parts.length !== 2) {
        throw new Error('Invalid encrypted payload format: missing colon separator');
    }
    const iv = Buffer.from(parts[0], 'hex');
    const ciphertext = Buffer.from(parts[1], 'hex');
    const keyBytes = crypto
        .createHash('sha256')
        .update(keyString + salt, 'utf8')
        .digest();
    const decipher = crypto.createDecipheriv('aes-256-cbc', keyBytes, iv);
    const decrypted = Buffer.concat([decipher.update(ciphertext), decipher.final()]);
    const jsonStr = decrypted.toString('utf8');
    return JSON.parse(jsonStr);
}
