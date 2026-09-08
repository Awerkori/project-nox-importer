import { StorageProvider } from './provider.js';
export declare class NoxWorkerStorageError extends Error {
    readonly stage: 'auth' | 'http' | 'payload' | 'network' | 'validation';
    readonly status?: number | undefined;
    constructor(stage?: 'auth' | 'http' | 'payload' | 'network' | 'validation', status?: number | undefined, message?: string);
}
export declare class NoxWorkerStorageProvider implements StorageProvider {
    private workerBaseUrl;
    private bridgeToken;
    private transport;
    private logger;
    constructor(workerBaseUrl: string, bridgeToken: string, transport?: typeof fetch);
    getProviderKey(): string;
    healthCheck(): Promise<boolean>;
    upload(bytes: Uint8Array, mime: string, id: string): Promise<string>;
}
