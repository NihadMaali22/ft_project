import type { IncomingMessage, ServerResponse } from 'node:http';
import type { AppContext } from '../../context.ts';
import { badRequest } from '../errors.ts';
import { sendJson } from '../respond.ts';
import { parseFilters } from '../../query/filters.ts';
import { queryLogs } from '../../query/logs.ts';
import { decodeCursor } from '../../domain/cursor.ts';
import { metrics } from '../../metrics.ts';

/** GET /logs - filtered, cursor-paginated log retrieval. */

const DIGITS_RE = /^\d+$/;

function parseLimit(raw: string | null, defaultLimit: number, maxLimit: number): number {
  if (raw === null) return defaultLimit;
  // Rejects "1.5", "-5", "1e3" and "" as well as non-numeric input.
  if (!DIGITS_RE.test(raw)) {
    throw badRequest(`invalid limit: ${JSON.stringify(raw)} is not a non-negative integer`);
  }
  const limit = Number(raw);
  if (limit < 1 || limit > maxLimit) {
    throw badRequest(`limit must be between 1 and ${maxLimit}`);
  }
  return limit;
}

export async function handleQueryLogs(
  request: IncomingMessage,
  response: ServerResponse,
  context: AppContext,
  queryString: string,
): Promise<void> {
  context.auth.authorize(request, 'query');
  context.rateLimiter.check();

  metrics.query.logsRequests += 1;
  const startedAt = performance.now();

  try {
    const params = new URLSearchParams(queryString);

    const filters = parseFilters(params);
    const limit = parseLimit(
      params.get('limit'),
      context.config.query.defaultLimit,
      context.config.query.maxLimit,
    );

    const rawCursor = params.get('cursor');
    let cursor = null;
    if (rawCursor !== null && rawCursor !== '') {
      cursor = decodeCursor(rawCursor);
      if (cursor === null) throw badRequest('invalid or malformed cursor');
    }

    const page = await queryLogs(context.pool, context.services, { filters, limit, cursor });
    sendJson(response, 200, page);
  } catch (error) {
    metrics.query.logsErrors += 1;
    throw error;
  } finally {
    metrics.query.logsLatency.record(performance.now() - startedAt);
  }
}
