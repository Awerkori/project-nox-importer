export declare const DEFAULT_MANGOTOONS_ENC_KEY: string;
export declare const DEFAULT_MANGOTOONS_SALT: string;
/**
 * Decrypts MangoTheme encrypted payloads (AES-256-CBC with SHA-256 key derivation)
 */
export declare function decryptMangoPayload(payload: string, keyString?: string, salt?: string): any;
