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

/**
 * Fast path: most bodies arrive in a handful of chunks.
 *
 * Written against the raw 'data'/'end' events rather than `for await`. Async
 * iteration over a stream allocates an iterator and a promise per chunk, and
 * for a body that arrives in one chunk - the norm here - that machinery costs
 * more than the read itself. The single-chunk case also avoids the array and
 * the copy entirely.
 */
function collect(stream: Readable, maxBytes: number): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    let single: Buffer | null = null;
    let chunks: Buffer[] | null = null;
    let total = 0;

    stream.on('data', (chunk: Buffer) => {
      total += chunk.length;
      if (total > maxBytes) {
        stream.destroy();
        reject(payloadTooLarge(`request body exceeds ${maxBytes} bytes`));
        return;
      }

      if (single === null && chunks === null) {
        single = chunk;
      } else if (chunks === null) {
        chunks = [single as Buffer, chunk];
        single = null;
      } else {
        chunks.push(chunk);
      }
    });

    stream.on('end', () => {
      if (single !== null) resolve(single);
      else if (chunks !== null) resolve(Buffer.concat(chunks, total));
      else resolve(EMPTY);
    });

    stream.on('error', reject);
  });
}

const EMPTY = Buffer.alloc(0);

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
