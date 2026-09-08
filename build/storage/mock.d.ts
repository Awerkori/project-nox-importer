import { StorageProvider } from './provider.js';
export declare class MockStorageProvider implements StorageProvider {
    uploads: Map<string, {
        bytes: Uint8Array;
        mime: string;
    }>;
    getProviderKey(): string;
    healthCheck(): Promise<boolean>;
    upload(bytes: Uint8Array, mime: string, id: string): Promise<string>;
}
