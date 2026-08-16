import type { ServerResponse } from 'node:http';
import type { AppContext } from '../../context.ts';
import { sendJson } from '../respond.ts';

/**
 * GET /health
 *
 * Always unauthenticated, regardless of AUTH_ENABLED - the load generator polls
 * it before it has any credentials.
 *
 * Reports 200 only once the database connection is established, migrations have
 * been applied, and the ingest writer is accepting rows. Reporting healthy any
 * earlier would let traffic arrive against a schema that does not exist yet.
 *
 * The status code is a startup readiness gate and deliberately does not flap
 * with live database state. A brief PostgreSQL hiccup should not make an
 * orchestrator tear down a service that is about to recover on its own, and the
 * contract is explicit that this endpoint gates the start of the run. Live
 * dependency state is still reported honestly, in the body, where operators and
 * dashboards can act on it without the required contract changing shape.
 */
export function handleHealth(response: ServerResponse, context: AppContext): void {
  if (!context.ready) {
    sendJson(response, 503, { status: 'starting' });
    return;
  }

  const healthyWriters = context.writer.healthyWriterCount;

  sendJson(response, 200, {
    status: 'ok',
    database: healthyWriters > 0 ? 'connected' : 'unavailable',
    writers: { healthy: healthyWriters, total: context.writer.stats().total_writers },
  });
}
