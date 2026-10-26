import { describe, expect, it } from 'vitest';
import { readImageBody } from '../bounded-body.js';

describe('readImageBody', () => {
  it('preserves a long lengthless stream across bounded assembly blocks', async () => {
    const chunkCount = 513;
    const chunkSize = 1024;
    let index = 0;
    const stream = new ReadableStream<Uint8Array>({
      pull(controller) {
        if (index >= chunkCount) {
          controller.close();
          return;
        }
        controller.enqueue(Uint8Array.from({ length: chunkSize }, () => index % 251));
        index++;
      },
    });

    const body = await readImageBody(new Response(stream), { maxBytes: 1024 * 1024 });

    expect(body).toHaveLength(chunkCount * chunkSize);
    for (const chunk of [0, 1, 255, 256, 512]) {
      expect(body[chunk * chunkSize]).toBe(chunk % 251);
      expect(body[(chunk + 1) * chunkSize - 1]).toBe(chunk % 251);
    }
  });
});
