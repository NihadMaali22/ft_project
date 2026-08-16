/**
 * Classification of database failures that are the dependency's fault rather
 * than the caller's.
 *
 * These deserve 503 with Retry-After, not 500. A 500 tells a client the request
 * was malformed in some unrecoverable way and should not be retried; a 503 says
 * the service is temporarily unable to serve it, which is the truth when
 * PostgreSQL is restarting, unreachable, or out of connections. Log shippers act
 * on that distinction - they buffer and retry a 503, and typically drop a 500.
 */

/** Connection-level and resource-exhaustion SQLSTATEs. */
const UNAVAILABLE_SQLSTATES = new Set([
  '08000', // connection_exception
  '08001', // sqlclient_unable_to_establish_sqlconnection
  '08003', // connection_does_not_exist
  '08004', // sqlserver_rejected_establishment_of_sqlconnection
  '08006', // connection_failure
  '08007', // transaction_resolution_unknown
  '53100', // disk_full
  '53200', // out_of_memory
  '53300', // too_many_connections
  '57P01', // admin_shutdown
  '57P02', // crash_shutdown
  '57P03', // cannot_connect_now
]);

const UNAVAILABLE_ERRNOS = new Set([
  'ECONNREFUSED',
  'ECONNRESET',
  'ENOTFOUND',
  'EAI_AGAIN',
  'EHOSTUNREACH',
  'ENETUNREACH',
  'EPIPE',
  'ETIMEDOUT',
]);

/** node-postgres reports some failures only as a message. */
const UNAVAILABLE_MESSAGES = [
  'connection terminated',
  'timeout exceeded when trying to connect',
  'client has encountered a connection error',
  'timed out waiting for a database writer',
  'server closed the connection unexpectedly',
  'no space left on device',
];

export function isDatabaseUnavailable(error: unknown): boolean {
  if (error === null || typeof error !== 'object') return false;

  const candidate = error as { code?: unknown; message?: unknown };

  if (typeof candidate.code === 'string') {
    if (UNAVAILABLE_SQLSTATES.has(candidate.code)) return true;
    if (UNAVAILABLE_ERRNOS.has(candidate.code)) return true;
  }

  if (typeof candidate.message === 'string') {
    const message = candidate.message.toLowerCase();
    return UNAVAILABLE_MESSAGES.some((fragment) => message.includes(fragment));
  }

  return false;
}
