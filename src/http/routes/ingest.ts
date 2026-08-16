import type { IncomingMessage, ServerResponse } from 'node:http';
import type { AppContext } from '../../context.ts';
import { readBody } from '../body.ts';
import { badRequest, serviceUnavailable } from '../errors.ts';
import { sendJson } from '../respond.ts';
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

  try {
    // Resolves only once these rows have committed. The contract forbids
    // reporting acceptance for anything not durably stored.
    await context.writer.append(valid);
  } catch (error) {
    if (error instanceof BackpressureError) {
      // Shedding with 503 + Retry-After is the honest response to saturation.
      // Answering 200 and dropping the rows would not be.
      throw serviceUnavailable(error.message, 1);
    }
    throw error;
  }

  metrics.ingest.entriesAccepted += valid.length;

  sendJson(response, 200, { accepted: valid.length, rejected });
}
