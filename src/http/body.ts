import type { IncomingMessage } from 'node:http';
import { createGunzip, createInflate, createBrotliDecompress } from 'node:zlib';
import { pipeline } from 'node:stream/promises';
import { Readable } from 'node:stream';
import { payloadTooLarge, badRequest } from './errors.ts';

/**
 * Raw request body reader.
 *
 * Deliberately not a body-parser middleware. Chunks are collected and
 * concatenated exactly once, then handed to a single JSON.parse. Generic
 * parsers re-decode to string, re-buffer, and often re-validate, and at 15k
 * entries per second under a 0.5 CPU budget that overhead is not affordable.
 */

/** Fast path: most bodies arrive in a handful of chunks. */
async function collect(stream: Readable, maxBytes: number): Promise<Buffer> {
  const chunks: Buffer[] = [];
  let total = 0;

  for await (const chunk of stream) {
    const buffer = chunk as Buffer;
    total += buffer.length;
    if (total > maxBytes) {
      stream.destroy();
      throw payloadTooLarge(`request body exceeds ${maxBytes} bytes`);
    }
    chunks.push(buffer);
  }

  // Avoids a copy when the whole body arrived in one chunk, which is the
  // common case for batches under the socket buffer size.
  return chunks.length === 1 ? (chunks[0] as Buffer) : Buffer.concat(chunks, total);
}

export async function readBody(request: IncomingMessage, maxBytes: number): Promise<Buffer> {
  const encoding = request.headers['content-encoding'];

  if (encoding === undefined || encoding === 'identity') {
    return collect(request, maxBytes);
  }

  // Compressed bodies are supported for correctness, not for throughput:
  // decompression competes with validation for the same half core.
  const decompressor =
    encoding === 'gzip'
      ? createGunzip()
      : encoding === 'deflate'
        ? createInflate()
        : encoding === 'br'
          ? createBrotliDecompress()
          : null;

  if (decompressor === null) {
    throw badRequest(`unsupported content-encoding: ${String(encoding)}`);
  }

  const chunks: Buffer[] = [];
  let total = 0;

  decompressor.on('data', (chunk: Buffer) => {
    total += chunk.length;
    if (total > maxBytes) {
      decompressor.destroy(payloadTooLarge(`decompressed body exceeds ${maxBytes} bytes`));
      return;
    }
    chunks.push(chunk);
  });

  await pipeline(request, decompressor);

  return Buffer.concat(chunks, total);
}
