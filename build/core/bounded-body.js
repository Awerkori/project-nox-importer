// Match the existing media validator's size limit before buffering the response.
export async function readImageBody(response, maxBytes = 19_000_000) {
    const declared = Number(response.headers.get('content-length'));
    if (declared > maxBytes) {
        await response.body?.cancel();
        throw new Error('Image exceeds media byte limit');
    }
    if (!response.body)
        throw new Error('Image response has no body');
    const reader = response.body.getReader();
    const chunks = [];
    let length = 0;
    try {
        while (true) {
            const { value, done } = await reader.read();
            if (done)
                break;
            length += value.byteLength;
            if (length > maxBytes)
                throw new Error('Image exceeds media byte limit');
            chunks.push(value);
        }
        const result = new Uint8Array(length);
        let offset = 0;
        for (const chunk of chunks) {
            result.set(chunk, offset);
            offset += chunk.byteLength;
        }
        return result;
    }
    catch (error) {
        await reader.cancel().catch(() => { });
        throw error;
    }
    finally {
        chunks.length = 0;
        reader.releaseLock();
    }
}
