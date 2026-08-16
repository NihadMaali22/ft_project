import type { ServerResponse } from 'node:http';
import { HttpError } from './errors.ts';
import { logger } from '../logger.ts';
import { isDatabaseUnavailable } from '../db/errors.ts';

const JSON_CONTENT_TYPE = 'application/json; charset=utf-8';

/** True once the socket can no longer carry a response. */
function isWritable(response: ServerResponse): boolean {
  return !response.headersSent && !response.writableEnded && !response.destroyed;
}

export function sendJson(
  response: ServerResponse,
  status: number,
  body: unknown,
  headers?: Record<string, string>,
): void {
  const payload = Buffer.from(JSON.stringify(body), 'utf8');
  response.writeHead(status, {
    'content-type': JSON_CONTENT_TYPE,
    'content-length': payload.length,
    ...headers,
  });
  response.end(payload);
}

/**
 * Maps a thrown error onto the documented error shape, `{"error": "..."}`.
 *
 * Three cases, in order:
 *   - HttpError carries its own status, chosen by the handler.
 *   - A database outage is the dependency's fault, so it becomes 503 with
 *     Retry-After rather than 500. Clients buffer and retry a 503; many drop a
 *     500 outright, which would turn a brief PostgreSQL restart into permanent
 *     data loss at the shipper.
 *   - Anything else is a genuine bug: logged in full, reported as a bare 500,
 *     with no internal detail echoed back to the caller.
 */
export function sendError(response: ServerResponse, error: unknown): void {
  // Checked once here rather than guessing from the request object. A POST
  // whose body has been fully consumed reports request.destroyed === true even
  // though the client is still waiting, so using that as a disconnect signal
  // silently resets healthy connections instead of answering them.
  if (!isWritable(response)) {
    response.destroy();
    return;
  }

  if (error instanceof HttpError) {
    sendJson(response, error.status, { error: error.message }, error.headers);
    return;
  }

  if (isDatabaseUnavailable(error)) {
    logger.warn('database unavailable while serving request', { error });
    sendJson(
      response,
      503,
      { error: 'database temporarily unavailable; please retry' },
      { 'Retry-After': '1' },
    );
    return;
  }

  logger.error('unhandled request error', { error });
  sendJson(response, 500, { error: 'internal server error' });
}
