export declare const DEFAULT_ENC_KEY = "i7ato8l6sai74jyIHfE2oMmieshoforanuYTusF4jKdqEwhUEft9dsadcxzsaipnjm8";
/**
 * Rabbit Stream Cipher (RFC 4503)
 */
export declare class RabbitCipher {
    private x;
    private c;
    private b;
    setup(key: Buffer, iv?: Buffer): void;
    private nextState;
    crypt(data: Buffer): Buffer;
}
/**
 * OpenSSL EVP_BytesToKey key derivation using MD5
 */
export declare function evpBytesToKey(password: Buffer, salt: Buffer, keyLen?: number, ivLen?: number): {
    key: Buffer;
    iv: Buffer;
};
export declare function derivePassword(dateStr?: string, encKey?: string): string;
export declare function decryptVSecure(vSecure: string, dataKey?: string, encKey?: string): any;
export declare function fetchLiveEncryptionKey(baseUrl?: string): Promise<string>;
