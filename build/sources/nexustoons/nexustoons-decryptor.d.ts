export declare const NEXUS_TOONS_CRYPTO_SECRET = "OrionNexus2025CryptoKey!Secure";
export declare function isEncryptedNexusToons(data: any): boolean;
export declare function decryptNexusToonsPayload<T = any>(data: {
    d: string;
    k?: number;
    v: number;
}): T;
