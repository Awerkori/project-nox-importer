import { StorageProvider } from './provider.js';
export declare class TelegramStorageError extends Error {
    readonly stage: 'http' | 'payload' | 'network' | 'validation';
    readonly status?: number | undefined;
    constructor(stage?: 'http' | 'payload' | 'network' | 'validation', status?: number | undefined);
}
export declare class TelegramStorageProvider implements StorageProvider {
    private token;
    private chatId;
    private transport;
    private logger;
    constructor(token: string, chatId: string, transport?: typeof fetch);
    getProviderKey(): string;
    healthCheck(): Promise<boolean>;
    upload(bytes: Uint8Array, _mime: string, id: string): Promise<string>;
}
