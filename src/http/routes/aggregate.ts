import type { IncomingMessage, ServerResponse } from 'node:http';
import type { AppContext } from '../../context.ts';
import { sendJson } from '../respond.ts';
import { parseFilters } from '../../query/filters.ts';
import { aggregateLogs, parseBucketSize, parseGroupBy } from '../../query/aggregate.ts';
import { metrics } from '../../metrics.ts';

/** GET /logs/aggregate - time-bucketed counts over the same filter vocabulary. */

export async function handleAggregate(
  request: IncomingMessage,
  response: ServerResponse,
  context: AppContext,
  queryString: string,
): Promise<void> {
  context.auth.authorize(request, 'query');
  context.rateLimiter.check();

  metrics.query.aggregateRequests += 1;
  const startedAt = performance.now();

  try {
    const params = new URLSearchParams(queryString);

    const filters = parseFilters(params);
    const bucket = parseBucketSize(params.get('bucket'));
    const groupBy = parseGroupBy(params.get('group_by'));

    const buckets = await aggregateLogs(context.pool, context.services, {
      filters,
      bucket,
      groupBy,
    });

    sendJson(response, 200, { buckets });
  } catch (error) {
    metrics.query.aggregateErrors += 1;
    throw error;
  } finally {
    metrics.query.aggregateLatency.record(performance.now() - startedAt);
  }
}
