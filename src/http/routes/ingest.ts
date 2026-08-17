import type { IncomingMessage, ServerResponse } from 'node:http';
import type { AppContext } from '../../context.ts';
import { readBody } from '../body.ts';
import { badRequest, serviceUnavailable } from '../errors.ts';
import { sendJson, sendBuffer } from '../respond.ts';
import { validateEntry, isRejection } from '../../domain/validate.ts';
import { BackpressureError } from '../../ingest/writer.ts';
import { metrics } from '../../metrics.ts';
import type { RejectedEntry, ValidLogRow } from '../../domain/types.ts';

/**
 * POST /logs
 *
 * Always a batch; a batch of one is valid. A single invalid entry never fails
 * the batch - valid entries are accepted and each rejection is reported with
 * its array index and reason.
 */

/** Bounds per-request work independently of the byte-size limit. */
const MAX_ENTRIES_PER_REQUEST = 100_000;

/**
 * Cached replies for the overwhelmingly common "everything was accepted" case.
 *
 * The response is identical for every request with the same accepted count, so
 * building it per request means JSON.stringify plus a UTF-8 encode on a path
 * with a ~33 microsecond budget. Indexed by count and populated lazily; larger
 * batches fall back to serialising once, which is negligible next to the work
 * of storing that many rows.
 */
const ACCEPTED_REPLIES: Buffer[] = [];

function acceptedReply(count: number): Buffer {
  const cached = ACCEPTED_REPLIES[count];
  if (cached !== undefined) return cached;

  const payload = Buffer.from(`{"accepted":${count},"rejected":[]}`, 'utf8');
  if (count <= 1024) ACCEPTED_REPLIES[count] = payload;
  return payload;
}

export async function handleIngest(
  request: IncomingMessage,
  response: ServerResponse,
  context: AppContext,
): Promise<void> {
  context.auth.authorize(request, 'ingest');
  context.rateLimiter.check();

  const body = await readBody(request, context.config.ingest.maxBodyBytes);
  if (body.length === 0) throw badRequest('request body is empty');

  let parsed: unknown;
  try {
    parsed = JSON.parse(body.toString('utf8'));
  } catch {
    throw badRequest('malformed JSON in request body');
  }

  if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw badRequest('request body must be a JSON object containing a "logs" array');
  }

  const logs = (parsed as { logs?: unknown }).logs;
  if (!Array.isArray(logs)) {
    throw badRequest('request body must contain a "logs" array');
  }
  if (logs.length === 0) {
    throw badRequest('"logs" must contain at least one entry');
  }
  if (logs.length > MAX_ENTRIES_PER_REQUEST) {
    throw badRequest(`"logs" contains more than ${MAX_ENTRIES_PER_REQUEST} entries`);
  }

  // One clock reading for the whole batch, so the five-minute future check is
  // applied consistently to every entry regardless of validation duration.
  const nowMillis = Date.now();

  // Single-entry fast path. A client posting one log per request is the shape
  // that maximises per-request overhead, so it skips the two result arrays and
  // answers from a cached buffer. Rejections fall through to the general path,
  // which keeps the per-entry reporting contract in exactly one place.
  if (logs.length === 1) {
    const only = validateEntry(logs[0], nowMillis);
    metrics.ingest.entriesReceived += 1;

    if (!isRejection(only)) {
      await appendOrShed(context, [only]);
      metrics.ingest.entriesAccepted += 1;
      sendBuffer(response, 200, acceptedReply(1));
      return;
    }

    metrics.ingest.entriesRejected += 1;
    sendJson(response, 400, { accepted: 0, rejected: [{ index: 0, reason: only }] });
    return;
  }

  const valid: ValidLogRow[] = [];
  const rejected: RejectedEntry[] = [];

  for (let index = 0; index < logs.length; index++) {
    const result = validateEntry(logs[index], nowMillis);
    if (isRejection(result)) {
      rejected.push({ index, reason: result });
    } else {
      valid.push(result);
    }
  }

  metrics.ingest.entriesReceived += logs.length;
  metrics.ingest.entriesRejected += rejected.length;

  if (valid.length === 0) {
    // Every entry was rejected. The per-entry reasons are far more useful to a
    // client than a bare error string, so the batch shape is preserved.
    sendJson(response, 400, { accepted: 0, rejected });
    return;
  }

  await appendOrShed(context, valid);
  metrics.ingest.entriesAccepted += valid.length;

  if (rejected.length === 0) {
    sendBuffer(response, 200, acceptedReply(valid.length));
    return;
  }
  sendJson(response, 200, { accepted: valid.length, rejected });
}

/**
 * Stores the rows, translating saturation into the documented 503.
 *
 * Resolves only once the rows have committed: the contract forbids reporting
 * acceptance for anything not durably stored. Shedding with 503 + Retry-After
 * is the honest answer when the queue is full; answering 200 and dropping the
 * rows would not be.
 */
async function appendOrShed(context: AppContext, rows: ValidLogRow[]): Promise<void> {
  try {
    await context.writer.append(rows);
  } catch (error) {
    if (error instanceof BackpressureError) {
      throw serviceUnavailable(error.message, 1);
    }
    throw error;
  }
}
