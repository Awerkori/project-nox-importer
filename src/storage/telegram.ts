import { StorageProvider } from './provider.js';
import { Logger } from '../core/logger.js';

export class TelegramStorageError extends Error {
  constructor(
    public readonly stage: 'http' | 'payload' | 'network' | 'validation' = 'validation',
    public readonly status?: number
  ) {
    super(`Telegram storage operation failed at stage: ${stage}${status ? ` (status: ${status})` : ''}`);
    this.name = 'TelegramStorageError';
  }
}

export class TelegramStorageProvider implements StorageProvider {
  private logger = new Logger('TelegramStorage');

  constructor(
    private token: string,
    private chatId: string,
    private transport: typeof fetch = fetch
  ) {
    if (!token || !chatId) {
      throw new Error('TelegramStorageProvider requires valid bot token and chat ID');
    }
  }

  getProviderKey(): string {
    return 'telegram';
  }

  async healthCheck(): Promise<boolean> {
    try {
      const res = await this.transport(`https://api.telegram.org/bot${this.token}/getMe`, {
        method: 'GET',
        signal: AbortSignal.timeout(10_000),
      });
      if (!res.ok) return false;
      const data = await res.json().catch(() => null) as { ok?: boolean } | null;
      return data?.ok === true;
    } catch {
      return false;
    }
  }

  async upload(bytes: Uint8Array, _mime: string, id: string): Promise<string> {
    const form = new FormData();
    form.append('chat_id', this.chatId);
    // Send as .bin with application/octet-stream to prevent Telegram from transcoding
    // webp pages into compressed animated/sticker formats.
    const blobPart = bytes as unknown as BlobPart;
    form.append('document', new Blob([blobPart], { type: 'application/octet-stream' }), `${id}.bin`);
    form.append('disable_content_type_detection', 'true');
    form.append('disable_notification', 'true');

    try {
      const response = await this.transport(`https://api.telegram.org/bot${this.token}/sendDocument`, {
        method: 'POST',
        body: form,
        redirect: 'manual',
        signal: AbortSignal.timeout(60_000),
      });

      if (!response.ok) {
        throw new TelegramStorageError('http', response.status);
      }

      const payload = await response.json().catch(() => {
        throw new TelegramStorageError('payload');
      }) as { ok?: boolean; result?: { document?: { file_id?: string } } } | null;

      if (!payload?.ok || !payload.result) {
        throw new TelegramStorageError('payload');
      }

      const fileId = payload.result.document?.file_id;
      if (typeof fileId !== 'string' || !/^[A-Za-z0-9_-]{1,512}$/.test(fileId)) {
        throw new TelegramStorageError('validation');
      }

      return fileId;
    } catch (err) {
      if (err instanceof TelegramStorageError) {
        throw err;
      }
      this.logger.error('Telegram network failure during upload', { id });
      throw new TelegramStorageError('network');
    }
  }
}
