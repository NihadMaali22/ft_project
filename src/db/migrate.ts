import { readdir, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import type pg from 'pg';
import type { Config } from '../config.ts';
import { logger } from '../logger.ts';

/**
 * Forward-only SQL migrations, applied automatically at startup.
 *
 * The service must not report healthy until these have run, so this is awaited
 * during boot rather than kicked off in the background.
 */

/** Arbitrary but fixed key; serialises migration across concurrent instances. */
const MIGRATION_ADVISORY_LOCK = 8_274_113_905_442_001n;

const MIGRATIONS_DIR = join(import.meta.dirname, 'migrations');

async function ensureMigrationsTable(client: pg.ClientBase): Promise<void> {
  await client.query(`
    CREATE TABLE IF NOT EXISTS schema_migrations (
      name       text PRIMARY KEY,
      applied_at timestamptz NOT NULL DEFAULT now()
    )
  `);
}

export async function runMigrations(client: pg.ClientBase): Promise<void> {
  // Held for the whole run so two instances starting together cannot both apply
  // the same migration. Released automatically when the session ends.
  await client.query('SELECT pg_advisory_lock($1)', [MIGRATION_ADVISORY_LOCK.toString()]);

  try {
    await ensureMigrationsTable(client);

    const applied = new Set<string>(
      (await client.query<{ name: string }>('SELECT name FROM schema_migrations')).rows.map(
        (row) => row.name,
      ),
    );

    const files = (await readdir(MIGRATIONS_DIR)).filter((f) => f.endsWith('.sql')).sort();

    for (const file of files) {
      if (applied.has(file)) continue;

      const sql = await readFile(join(MIGRATIONS_DIR, file), 'utf8');
      const started = Date.now();

      // Each migration is atomic: either the DDL and its bookkeeping row both
      // land, or neither does.
      await client.query('BEGIN');
      try {
        await client.query(sql);
        await client.query('INSERT INTO schema_migrations (name) VALUES ($1)', [file]);
        await client.query('COMMIT');
      } catch (error) {
        await client.query('ROLLBACK');
        throw new Error(`Migration ${file} failed: ${String(error)}`);
      }

      logger.info('migration applied', { file, durationMs: Date.now() - started });
    }

    if (files.length === applied.size) {
      logger.info('schema up to date', { migrations: files.length });
    }
  } finally {
    await client.query('SELECT pg_advisory_unlock($1)', [MIGRATION_ADVISORY_LOCK.toString()]);
  }
}

/**
 * Creates the opt-in indexes, if enabled.
 *
 * Kept out of the migration files because they are configuration-dependent
 * rather than schema versions: the same schema version may or may not carry
 * them. Both are off by default; see the README for the measured cost of each.
 *
 * These run non-concurrently and take a lock on the partitioned table, so they
 * are applied at startup before the service reports healthy. PostgreSQL does not
 * support CREATE INDEX CONCURRENTLY directly on a partitioned parent.
 */
export async function ensureOptionalIndexes(client: pg.ClientBase, config: Config): Promise<void> {
  if (config.indexes.attrGin) {
    logger.info('creating optional attributes GIN index (ATTR_GIN_INDEX=true)');
    // jsonb_path_ops rather than the default jsonb_ops: roughly a third of the
    // size and faster to maintain, at the cost of supporting only containment
    // (@>) lookups -- which is all the attribute filter needs.
    await client.query(`
      CREATE INDEX IF NOT EXISTS logs_attributes_gin_idx
        ON logs USING gin (attributes jsonb_path_ops)
    `);
  }

  if (config.indexes.serviceTs) {
    logger.info('creating optional (service_id, ts) index (SERVICE_TS_INDEX=true)');
    await client.query(`
      CREATE INDEX IF NOT EXISTS logs_service_ts_idx
        ON logs (service_id, ts DESC, id DESC)
    `);
  }
}
