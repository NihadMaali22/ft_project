import type { QueryPool } from './pool.ts';
import { logger } from '../logger.ts';

/**
 * In-memory cache of the service-name dictionary.
 *
 * Ingestion needs name -> id on every row, and queries need id -> name on every
 * returned row and every `group_by=service` bucket. Both directions are held in
 * memory, so the steady-state cost of dictionary encoding is a Map lookup and
 * the read path never joins against `services` at all.
 */

/** Cardinality past which the cache is almost certainly being misused. */
const CARDINALITY_WARN_THRESHOLD = 50_000;

export class ServiceDictionary {
  private readonly idByName = new Map<string, number>();
  private readonly nameById = new Map<number, string>();
  private warned = false;

  constructor(private readonly pool: QueryPool) {}

  /** Loads the full dictionary once at startup. */
  async load(): Promise<void> {
    const result = await this.pool.query<{ service_id: number; name: string }>(
      'SELECT service_id, name FROM services',
    );
    for (const row of result.rows) this.remember(row.service_id, row.name);
    logger.info('service dictionary loaded', { services: this.idByName.size });
  }

  private remember(id: number, name: string): void {
    this.idByName.set(name, id);
    this.nameById.set(id, name);
    if (!this.warned && this.idByName.size > CARDINALITY_WARN_THRESHOLD) {
      this.warned = true;
      logger.warn('service cardinality is unexpectedly high', { count: this.idByName.size });
    }
  }

  /** Synchronous lookup for the ingest hot path. */
  lookup(name: string): number | undefined {
    return this.idByName.get(name);
  }

  /** Synchronous reverse lookup for the query path. */
  nameOf(id: number): string | undefined {
    return this.nameById.get(id);
  }

  /**
   * Registers any names not already cached, in one round trip.
   *
   * Called only when a batch contains a service seen for the first time, which
   * after warm-up is effectively never.
   */
  async register(names: string[]): Promise<void> {
    const missing = [...new Set(names.filter((name) => !this.idByName.has(name)))];
    if (missing.length === 0) return;

    // ON CONFLICT DO NOTHING makes this safe against a concurrent instance
    // inserting the same name; the follow-up SELECT then reads whichever row won.
    await this.pool.query('INSERT INTO services (name) SELECT unnest($1::text[]) ON CONFLICT (name) DO NOTHING', [
      missing,
    ]);

    const result = await this.pool.query<{ service_id: number; name: string }>(
      'SELECT service_id, name FROM services WHERE name = ANY($1::text[])',
      [missing],
    );
    for (const row of result.rows) this.remember(row.service_id, row.name);
  }

  /**
   * Resolves a name to an id for the query path, without inserting.
   *
   * Returns null for an unknown service, which callers turn into an empty
   * result set rather than an error: filtering on a service that has never
   * logged is a valid query with no matches.
   */
  lookupForFilter(name: string): number | null {
    return this.idByName.get(name) ?? null;
  }

  /**
   * Fills in any ids missing from the cache, for rows written by another
   * instance since this one started.
   */
  async hydrateIds(ids: number[]): Promise<void> {
    const missing = [...new Set(ids.filter((id) => !this.nameById.has(id)))];
    if (missing.length === 0) return;

    const result = await this.pool.query<{ service_id: number; name: string }>(
      'SELECT service_id, name FROM services WHERE service_id = ANY($1::int[])',
      [missing],
    );
    for (const row of result.rows) this.remember(row.service_id, row.name);
  }
}
