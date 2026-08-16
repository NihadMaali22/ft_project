import type pg from 'pg';
import { logger } from '../logger.ts';

/**
 * Daily partition lifecycle.
 *
 * Partitions are created ahead of the rows that need them and dropped whole
 * once they fall outside the retention window. Keeping the DEFAULT partition
 * empty matters: PostgreSQL must scan it to attach any new partition whose
 * range could overlap, so a default partition holding millions of rows would
 * turn routine partition creation into a long ACCESS EXCLUSIVE stall.
 */

export const MICROS_PER_DAY = 86_400_000_000;
const MILLIS_PER_DAY = 86_400_000;

/** Guards identifier interpolation. Names are derived from dates, never input. */
const PARTITION_NAME_RE = /^logs_\d{8}$/;

export function dayIndexFromMicros(micros: number): number {
  return Math.floor(micros / MICROS_PER_DAY);
}

/** Renders a day index as an ISO date, e.g. 20460 -> "2026-01-01". */
function isoDate(dayIndex: number): string {
  return new Date(dayIndex * MILLIS_PER_DAY).toISOString().slice(0, 10);
}

export function partitionName(dayIndex: number): string {
  return `logs_${isoDate(dayIndex).replaceAll('-', '')}`;
}

/** Recovers the day index encoded in a partition name, or null. */
export function dayIndexFromPartitionName(name: string): number | null {
  if (!PARTITION_NAME_RE.test(name)) return null;
  const digits = name.slice(5);
  const iso = `${digits.slice(0, 4)}-${digits.slice(4, 6)}-${digits.slice(6, 8)}`;
  const millis = Date.parse(`${iso}T00:00:00Z`);
  if (Number.isNaN(millis)) return null;
  return millis / MILLIS_PER_DAY;
}

export class PartitionManager {
  /** Day indexes known to have a partition. Avoids a catalog hit per flush. */
  private readonly known = new Set<number>();

  constructor(private readonly client: pg.ClientBase) {}

  async loadExisting(): Promise<void> {
    const result = await this.client.query<{ relname: string }>(`
      SELECT child.relname
      FROM pg_inherits
      JOIN pg_class parent ON parent.oid = pg_inherits.inhparent
      JOIN pg_class child  ON child.oid  = pg_inherits.inhrelid
      WHERE parent.relname = 'logs'
    `);

    for (const row of result.rows) {
      const dayIndex = dayIndexFromPartitionName(row.relname);
      if (dayIndex !== null) this.known.add(dayIndex);
    }

    logger.info('partitions loaded', { count: this.known.size });
  }

  has(dayIndex: number): boolean {
    return this.known.has(dayIndex);
  }

  /**
   * Creates partitions covering the inclusive day range, skipping those already
   * known.
   *
   * A failure here is logged rather than thrown: the DEFAULT partition still
   * accepts the rows, so ingestion continues correctly, just without pruning
   * for that day.
   */
  async ensureRange(firstDay: number, lastDay: number): Promise<void> {
    for (let day = firstDay; day <= lastDay; day++) {
      if (this.known.has(day)) continue;
      await this.ensureDay(day);
    }
  }

  private async ensureDay(dayIndex: number): Promise<void> {
    const name = partitionName(dayIndex);
    if (!PARTITION_NAME_RE.test(name)) {
      throw new Error(`Refusing to create partition with unexpected name: ${name}`);
    }

    const from = `${isoDate(dayIndex)} 00:00:00+00`;
    const to = `${isoDate(dayIndex + 1)} 00:00:00+00`;

    try {
      await this.client.query(
        `CREATE TABLE IF NOT EXISTS ${name} PARTITION OF logs FOR VALUES FROM ('${from}') TO ('${to}')`,
      );
      this.known.add(dayIndex);
      logger.info('partition created', { partition: name, from, to });
    } catch (error) {
      // Most likely cause: rows for this day already sit in logs_default.
      // Ingestion is unaffected - they simply keep landing there.
      logger.warn('could not create partition; rows will use the default partition', {
        partition: name,
        error,
      });
      // Cached as known so the failure is not retried on every single flush.
      this.known.add(dayIndex);
    }
  }

  /**
   * Drops every partition entirely older than the cutoff day.
   *
   * DETACH CONCURRENTLY would be the gentler tool - it takes only SHARE UPDATE
   * EXCLUSIVE on the parent - but PostgreSQL refuses it outright while a DEFAULT
   * partition exists ("cannot detach partitions concurrently when a default
   * partition exists"). Keeping the default partition is worth more than the
   * lighter lock: it guarantees that a row can always be stored, whatever its
   * timestamp, instead of failing a COPY when a partition is unexpectedly
   * missing.
   *
   * So this drops the partition directly. That is a catalog update plus an
   * unlink, not a data rewrite, and the ACCESS EXCLUSIVE lock it needs is held
   * for milliseconds. `lock_timeout` bounds the wait to acquire it, so a drop
   * that would otherwise queue behind in-flight COPYs gives up and is retried on
   * the next sweep rather than stalling ingestion.
   */
  async dropOlderThan(cutoffDayIndex: number): Promise<string[]> {
    const result = await this.client.query<{ relname: string }>(`
      SELECT child.relname
      FROM pg_inherits
      JOIN pg_class parent ON parent.oid = pg_inherits.inhparent
      JOIN pg_class child  ON child.oid  = pg_inherits.inhrelid
      WHERE parent.relname = 'logs'
    `);

    const dropped: string[] = [];

    for (const row of result.rows) {
      const name = row.relname;
      const dayIndex = dayIndexFromPartitionName(name);
      // Skips logs_default, which has no day index and must never be dropped.
      if (dayIndex === null || dayIndex >= cutoffDayIndex) continue;

      try {
        await this.dropPartition(name);
        this.known.delete(dayIndex);
        dropped.push(name);
      } catch (error) {
        // Retried on the next sweep rather than aborting the whole run. The
        // usual cause is lock_timeout expiring behind a busy COPY.
        logger.warn('failed to drop expired partition', { partition: name, error });
      }
    }

    return dropped;
  }

  private async dropPartition(name: string): Promise<void> {
    // The name is derived from a date and matched against this pattern before
    // it is ever interpolated. No request data reaches this statement.
    if (!PARTITION_NAME_RE.test(name)) {
      throw new Error(`Refusing to drop unexpected relation: ${name}`);
    }

    // Bounds the wait for ACCESS EXCLUSIVE. Without it the drop would queue
    // behind in-flight COPYs and, worse, make every new COPY queue behind the
    // drop - converting a millisecond operation into an ingestion stall.
    await this.client.query(`SET lock_timeout = '5s'`);
    try {
      await this.client.query(`DROP TABLE IF EXISTS ${name}`);
    } finally {
      await this.client.query(`SET lock_timeout = 0`);
    }
  }
}
