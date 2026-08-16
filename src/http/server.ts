import http from 'node:http';
import type { IncomingMessage, ServerResponse } from 'node:http';
import type { AppContext } from '../context.ts';
import { notFound } from './errors.ts';
import { sendError, sendJson } from './respond.ts';
import { handleIngest } from './routes/ingest.ts';
import { handleQueryLogs } from './routes/query.ts';
import { handleAggregate } from './routes/aggregate.ts';
import { handleHealth } from './routes/health.ts';
import { handleMetrics, handleRetentionStatus, handleRetentionRun } from './routes/admin.ts';

/**
 * HTTP server built directly on node:http.
 *
 * No framework. Express or Fastify would add per-request middleware dispatch,
 * body parsing and object allocation to a path with a ~33 microsecond CPU
 * budget per log entry. With six routes, a switch over the pathname is both
 * faster and easier to follow than a router abstraction.
 *
 * Routing avoids `new URL()` for the same reason: splitting on the first '?' is
 * a single indexOf, and the query string is only parsed by endpoints that have
 * one, keeping the ingest path free of that allocation entirely.
 */

export function createServer(context: AppContext): http.Server {
  const server = http.createServer((request, response) => {
    void dispatch(request, response, context);
  });

  // Load generators hold connections open; the 5 second default would churn
  // them constantly, paying TCP and TLS-free handshake costs for nothing.
  server.keepAliveTimeout = 72_000;
  server.headersTimeout = 75_000;
  server.requestTimeout = 120_000;
  server.maxRequestsPerSocket = 0;

  server.on('connection', (socket) => {
    // Ingest responses are small and latency-sensitive; waiting for Nagle to
    // coalesce them adds delay with no benefit.
    socket.setNoDelay(true);
  });

  return server;
}

function methodNotAllowed(response: ServerResponse, allow: string): void {
  sendJson(response, 405, { error: `method not allowed; expected ${allow}` }, { allow });
}

async function dispatch(
  request: IncomingMessage,
  response: ServerResponse,
  context: AppContext,
): Promise<void> {
  try {
    const target = request.url ?? '/';
    const queryStart = target.indexOf('?');
    const rawPath = queryStart === -1 ? target : target.slice(0, queryStart);
    const queryString = queryStart === -1 ? '' : target.slice(queryStart + 1);

    // "/logs/" and "/logs" address the same resource.
    const path =
      rawPath.length > 1 && rawPath.charCodeAt(rawPath.length - 1) === 47 /* '/' */
        ? rawPath.slice(0, -1)
        : rawPath;

    const method = request.method;

    switch (path) {
      case '/logs':
        if (method === 'POST') return await handleIngest(request, response, context);
        if (method === 'GET') return await handleQueryLogs(request, response, context, queryString);
        return methodNotAllowed(response, 'GET, POST');

      case '/logs/aggregate':
        if (method === 'GET') return await handleAggregate(request, response, context, queryString);
        return methodNotAllowed(response, 'GET');

      case '/health':
        if (method === 'GET' || method === 'HEAD') return handleHealth(response, context);
        return methodNotAllowed(response, 'GET');

      case '/metrics':
        if (method === 'GET') return handleMetrics(request, response, context);
        return methodNotAllowed(response, 'GET');

      case '/admin/retention':
        if (method === 'GET') return handleRetentionStatus(request, response, context);
        return methodNotAllowed(response, 'GET');

      case '/admin/retention/run':
        if (method === 'POST') return await handleRetentionRun(request, response, context);
        return methodNotAllowed(response, 'POST');

      default:
        throw notFound(`no route for ${method ?? 'UNKNOWN'} ${path}`);
    }
  } catch (error) {
    // sendError decides whether the socket can still carry a response. Testing
    // `request.destroyed` here instead looks like a disconnect check but is not
    // one: Node marks a fully-consumed request destroyed while the client is
    // still waiting, so every failed POST would be answered with a TCP reset
    // rather than an HTTP status.
    sendError(response, error);
  }
}
