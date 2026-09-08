import { StorageProvider } from './provider.js';

export class MockStorageProvider implements StorageProvider {
  public uploads: Map<string, { bytes: Uint8Array; mime: string }> = new Map();

  getProviderKey(): string {
    return 'mock';
  }

  async healthCheck(): Promise<boolean> {
    return true;
  }

  async upload(bytes: Uint8Array, mime: string, id: string): Promise<string> {
    const mockFileId = `mock_file_${id}_${Date.now()}`;
    this.uploads.set(mockFileId, { bytes, mime });
    return mockFileId;
  }
}
