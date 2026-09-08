export class MockStorageProvider {
    uploads = new Map();
    getProviderKey() {
        return 'mock';
    }
    async healthCheck() {
        return true;
    }
    async upload(bytes, mime, id) {
        const mockFileId = `mock_file_${id}_${Date.now()}`;
        this.uploads.set(mockFileId, { bytes, mime });
        return mockFileId;
    }
}
