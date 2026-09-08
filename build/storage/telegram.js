import { Logger } from '../core/logger.js';
export class TelegramStorageError extends Error {
    stage;
    status;
    constructor(stage = 'validation', status) {
        super(`Telegram storage operation failed at stage: ${stage}${status ? ` (status: ${status})` : ''}`);
        this.stage = stage;
        this.status = status;
        this.name = 'TelegramStorageError';
    }
}
export class TelegramStorageProvider {
    token;
    chatId;
    transport;
    logger = new Logger('TelegramStorage');
    constructor(token, chatId, transport = fetch) {
        this.token = token;
        this.chatId = chatId;
        this.transport = transport;
        if (!token || !chatId) {
            throw new Error('TelegramStorageProvider requires valid bot token and chat ID');
        }
    }
    getProviderKey() {
        return 'telegram';
    }
    async healthCheck() {
        try {
            const res = await this.transport(`https://api.telegram.org/bot${this.token}/getMe`, {
                method: 'GET',
                signal: AbortSignal.timeout(10_000),
            });
            if (!res.ok)
                return false;
            const data = await res.json().catch(() => null);
            return data?.ok === true;
        }
        catch {
            return false;
        }
    }
    async upload(bytes, _mime, id) {
        const form = new FormData();
        form.append('chat_id', this.chatId);
        // Send as .bin with application/octet-stream to prevent Telegram from transcoding
        // webp pages into compressed animated/sticker formats.
        const blobPart = bytes;
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
            });
            if (!payload?.ok || !payload.result) {
                throw new TelegramStorageError('payload');
            }
            const fileId = payload.result.document?.file_id;
            if (typeof fileId !== 'string' || !/^[A-Za-z0-9_-]{1,512}$/.test(fileId)) {
                throw new TelegramStorageError('validation');
            }
            return fileId;
        }
        catch (err) {
            if (err instanceof TelegramStorageError) {
                throw err;
            }
            this.logger.error('Telegram network failure during upload', { id });
            throw new TelegramStorageError('network');
        }
    }
}
