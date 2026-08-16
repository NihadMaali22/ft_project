import type { Server } from 'node:http';
import { loadConfig } from './config.ts';
import { logger } from './logger.ts';
import { createQueryPool, createDedicatedClient, waitForDatabase } from './db/pool.ts';
import { runMigrations, ensureOptionalIndexes } from './db/migrate.ts';
import { ServiceDictionary } from './db/services.ts';
import { PartitionManager } from './db/partitions.ts';
import { IngestWriter } from './ingest/writer.ts';
import { Authenticator } from './http/auth.ts';
import { RateLimiter } from './http/rate-limit.ts';
import { RetentionJanitor } from './retention/janitor.ts';
import { createServer } from './http/server.ts';
import type { AppContext } from './context.ts';

/**
 * Startup sequence.
 *
 * Ordering here is part of the contract, not just tidiness: the HTTP server
 * begins listening early so /health can be polled, but `ready` is flipped only
 * after migrations, key seeding and the ingest writer are all complete. Until
 * then /health answers 503, so a poller waits rather than sending traffic at a
 * schema that does not exist yet.
 */

async function main(): Promise<void> {
  const config = loadConfig();

  logger.info('starting log service', {
    port: config.port,
    authEnabled: config.auth.enabled,
    retentionDays: config.retention.days,
    synchronousCommit: config.synchronousCommit,
  });

  await waitForDatabase(config);

  const pool = createQueryPool(config);

  // Migrations run on a throwaway connection so that DDL locks and the advisory
  // lock are released deterministically when it closes.
  const migrationClient = await createDedicatedClient(config, 'logsvc-migrate');
  try {
    await runMigrations(migrationClient);
    await ensureOptionalIndexes(migrationClient, config);
  } finally {
    await migrationClient.end();
  }

  const services = new ServiceDictionary(pool);
  await services.load();

  const partitionClient = await createDedicatedClient(config, 'logsvc-partitions');
  const partitions = new PartitionManager(partitionClient);
  await partitions.loadExisting();

  const writer = new IngestWriter(config, pool, services, partitions);
  await writer.start();

  const auth = new Authenticator(config, pool);
  await auth.start();

  const janitor = new RetentionJanitor(config, partitions);

  const context: AppContext = {
    config,
    pool,
    services,
    writer,
    auth,
    rateLimiter: new RateLimiter(config),
    janitor,
    ready: false,
  };

  const server = createServer(context);
  await listen(server, config.port);

  // One sweep before accepting traffic, so the lookahead partitions exist and
  // expired data is already gone by the time the first query runs.
  await janitor.sweep();
  janitor.start();

  context.ready = true;
  logger.info('service ready', { port: config.port });

  installShutdownHandlers(server, context, partitionClient);
}

function listen(server: Server, port: number): Promise<void> {
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, '0.0.0.0', () => {
      server.removeListener('error', reject);
      resolve();
    });
  });
}

function installShutdownHandlers(
  server: Server,
  context: AppContext,
  partitionClient: { end: () => Promise<void> },
): void {
  let shuttingDown = false;

  const shutdown = async (signal: string): Promise<void> => {
    if (shuttingDown) return;
    shuttingDown = true;

    logger.info('shutting down', { signal });
    // Fails readiness first so an orchestrator stops routing here while
    // in-flight batches finish.
    context.ready = false;

    const timeout = setTimeout(() => {
      logger.error('graceful shutdown timed out; exiting');
      process.exit(1);
    }, 15_000);
    timeout.unref();

    try {
      await new Promise<void>((resolve) => server.close(() => resolve()));
      // Flushes whatever is still buffered before the connections go away.
      await context.writer.stop();
      context.janitor.stop();
      await partitionClient.end().catch(() => undefined);
      await context.pool.end();
      logger.info('shutdown complete');
      process.exit(0);
    } catch (error) {
      logger.error('error during shutdown', { error });
      process.exit(1);
    }
  };

  process.on('SIGTERM', () => void shutdown('SIGTERM'));
  process.on('SIGINT', () => void shutdown('SIGINT'));

  process.on('unhandledRejection', (reason) => {
    logger.error('unhandled promise rejection', { error: reason });
  });

  process.on('uncaughtException', (error) => {
    // The process state is unknown after this point; exiting is the only safe
    // response. The container restart policy brings it back.
    logger.error('uncaught exception; exiting', { error });
    process.exit(1);
  });
}

main().catch((error) => {
  logger.error('fatal startup error', { error });
  process.exit(1);
});
