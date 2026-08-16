import pg from 'pg';
import type { Config } from '../config.ts';
import { logger } from '../logger.ts';

const { Pool } = pg;

export type QueryPool = pg.Pool;
export type PgClient = pg.PoolClient;

/**
 * Connection pool for read queries and administrative statements.
 *
 * Kept separate from the ingest writer connections on purpose. Writers hold
 * their connections for the lifetime of a COPY, so sharing one pool would let a
 * burst of ingestion starve query traffic of connections -- exactly the
 * "queries stay fast while ingestion is active" requirement, expressed as
 * resource isolation rather than hope.
 */
export function createQueryPool(config: Config): QueryPool {
  const pool = new Pool({
    connectionString: config.databaseUrl,
    max: config.query.poolSize,
    min: 1,
    idleTimeoutMillis: 30_000,
    connectionTimeoutMillis: 10_000,
    application_name: 'logsvc-query',
    // Applied by the server before any statement runs, so a pathological query
    // cannot pin the single available CPU indefinitely.
    statement_timeout: config.query.statementTimeoutMs,
  });

  pool.on('error', (error) => {
    // Emitted for idle clients dropped by the server; the pool replaces them.
    logger.warn('idle query client error', { error });
  });

  return pool;
}

/**
 * Opens a single dedicated connection, used for ingest writers and background
 * jobs that must not contend with the query pool.
 *
 * The 'error' listener is not optional. A pg.Client is an EventEmitter, and an
 * EventEmitter that emits 'error' with no listener throws an uncaught
 * exception. Any database restart or dropped connection would therefore take
 * the whole process down - which is exactly what happened during a saturation
 * test before this listener existed.
 */
export async function createDedicatedClient(
  config: Config,
  applicationName: string,
  settings: Record<string, string> = {},
  onError?: (error: Error) => void,
): Promise<pg.Client> {
  const client = new pg.Client({
    connectionString: config.databaseUrl,
    application_name: applicationName,
  });

  client.on('error', (error: Error) => {
    logger.warn('dedicated client connection error', { applicationName, error });
    onError?.(error);
  });

  await client.connect();

  for (const [key, value] of Object.entries(settings)) {
    // Keys are compile-time constants and values are whitelisted in config.ts;
    // no request data reaches this path.
    await client.query(`SET ${key} = ${value}`);
  }

  return client;
}

/** Waits for PostgreSQL to accept connections, with bounded exponential backoff. */
export async function waitForDatabase(config: Config, timeoutMs = 60_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let delay = 100;

  for (;;) {
    try {
      const client = new pg.Client({
        connectionString: config.databaseUrl,
        application_name: 'logsvc-boot',
        connectionTimeoutMillis: 5000,
      });
      await client.connect();
      await client.query('SELECT 1');
      await client.end();
      return;
    } catch (error) {
      if (Date.now() >= deadline) {
        throw new Error(`Database not reachable within ${timeoutMs}ms: ${String(error)}`);
      }
      logger.info('waiting for database', { retryInMs: delay });
      await new Promise((resolve) => setTimeout(resolve, delay));
      delay = Math.min(delay * 2, 2000);
    }
  }
}
