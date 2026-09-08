import { StorageProvider } from './provider.js';
import { Logger } from '../core/logger.js';

export class NoxWorkerStorageError extends Error {
  constructor(
    public readonly stage: 'auth' | 'http' | 'payload' | 'network' | 'validation' = 'validation',
    public readonly status?: number,
    message?: string
  ) {
    super(message || `NoxWorkerStorage operation failed at stage: ${stage}${status ? ` (status: ${status})` : ''}`);
    this.name = 'NoxWorkerStorageError';
  }
}

export class NoxWorkerStorageProvider implements StorageProvider {
  private logger = new Logger('NoxWorkerStorage');

  constructor(
    private workerBaseUrl: string,
    private bridgeToken: string,
    private transport: typeof fetch = fetch
  ) {
    if (!bridgeToken) {
      throw new Error('NoxWorkerStorageProvider requires valid NOX_STORAGE_BRIDGE_TOKEN');
    }
  }

  getProviderKey(): string {
    // Media provider key in database is 'telegram' so manga reader streams it correctly
    return 'telegram';
  }

  async healthCheck(): Promise<boolean> {
    try {
      const url = `${this.workerBaseUrl.replace(/\/$/, '')}/api/internal/storage/upload`;
      const res = await this.transport(url, {
        method: 'GET',
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 ProjectNox-Importer/1.0',
          Authorization: `Bearer ${this.bridgeToken}`,
          Accept: 'application/json',
        },
        signal: AbortSignal.timeout(15_000),
      });

      if (!res.ok) {
        this.logger.warn('Storage healthCheck received non-OK response', { status: res.status });
        return false;
      }

      const data = (await res.json().catch(() => null)) as { ok?: boolean; provider?: string } | null;
      return data?.ok === true && data?.provider === 'telegram';
    } catch (err: any) {
      this.logger.error('Storage healthCheck network error', { error: err?.message });
      return false;
    }
  }

  async upload(bytes: Uint8Array, mime: string, id: string): Promise<string> {
    const url = `${this.workerBaseUrl.replace(/\/$/, '')}/api/internal/storage/upload?id=${encodeURIComponent(id)}`;

    try {
      const blobPart = bytes as unknown as BlobPart;
      const res = await this.transport(url, {
        method: 'POST',
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 ProjectNox-Importer/1.0',
          Authorization: `Bearer ${this.bridgeToken}`,
          'Content-Type': mime || 'application/octet-stream',
          'Content-Length': String(bytes.byteLength),
        },
        body: new Blob([blobPart], { type: mime || 'application/octet-stream' }),
        signal: AbortSignal.timeout(60_000),
      });

      if (res.status === 401 || res.status === 403) {
        throw new NoxWorkerStorageError('auth', res.status, 'Authentication failed on internal storage endpoint');
      }

      if (!res.ok) {
        const errText = await res.text().catch(() => '');
        throw new NoxWorkerStorageError('http', res.status, `Internal storage upload failed: HTTP ${res.status} - ${errText.slice(0, 200)}`);
      }

      const data = (await res.json().catch(() => null)) as {
        providerKey?: string;
        mime?: string;
        width?: number;
        height?: number;
        bytes?: number;
      } | null;

      if (!data || typeof data.providerKey !== 'string' || !data.providerKey) {
        throw new NoxWorkerStorageError('payload', res.status, 'Invalid response payload from internal storage endpoint');
      }

      return data.providerKey;
    } catch (err: any) {
      if (err instanceof NoxWorkerStorageError) throw err;
      this.logger.error('Network error during internal storage upload', { id, error: err?.message });
      throw new NoxWorkerStorageError('network', undefined, err?.message);
    }
  }
}
