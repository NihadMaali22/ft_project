import type { IncomingMessage, ServerResponse } from 'node:http';
import type { AppContext } from '../../context.ts';
import { sendJson } from '../respond.ts';
import { snapshotMetrics } from '../../metrics.ts';

/**
 * Additive operational endpoints.
 *
 * These are extras: they add routes without touching the four required ones, so
 * the load generator neither sees nor needs them. They exist because a service
 * whose throughput and backpressure behaviour cannot be observed is a service
 * whose performance claims cannot be checked.
 */

/** GET /metrics - counters, latency percentiles and memory usage as JSON. */
export function handleMetrics(
  request: IncomingMessage,
  response: ServerResponse,
  context: AppContext,
): void {
  context.auth.authorize(request, 'query');
  sendJson(response, 200, snapshotMetrics({ writer: context.writer.stats() }));
}

/** GET /admin/retention - current retention configuration and last sweep. */
export function handleRetentionStatus(
  request: IncomingMessage,
  response: ServerResponse,
  context: AppContext,
): void {
  context.auth.authorize(request, 'query');
  sendJson(response, 200, context.janitor.status());
}

/**
 * POST /admin/retention/run - triggers a sweep immediately.
 * Useful for verifying retention without waiting out the interval.
 */
export async function handleRetentionRun(
  request: IncomingMessage,
  response: ServerResponse,
  context: AppContext,
): Promise<void> {
  context.auth.authorize(request, 'ingest');
  const result = await context.janitor.sweep();
  sendJson(response, 200, result);
}
