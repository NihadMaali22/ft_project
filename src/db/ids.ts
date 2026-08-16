import type { QueryPool } from './pool.ts';
import { logger } from '../logger.ts';

/**
 * Hi/lo row-id allocator.
 *
 * `logs_id_seq` is declared with INCREMENT BY 10000, so a single nextval()
 * reserves a 10,000-id block for this process. Ids are then handed out from
 * local memory, turning what would be one database round trip per row into
 * roughly one per second at target load.
 *
 * Correct across processes and restarts without coordination: no two callers
 * can ever be handed the same block. Gaps are expected and harmless - ids need
 * to be unique and increasing, not dense.
 */
export class IdAllocator {
  private nextId = 0;
  private remaining = 0;
  private blockSize = 0;
  private inFlight: Promise<void> | null = null;

  constructor(private readonly pool: QueryPool) {}

  /** Reads the block size from the sequence so the two cannot drift apart. */
  async init(): Promise<void> {
    const result = await this.pool.query<{ increment_by: string }>(
      `SELECT increment_by FROM pg_sequences WHERE schemaname = current_schema() AND sequencename = 'logs_id_seq'`,
    );
    const row = result.rows[0];
    if (row === undefined) {
      throw new Error('logs_id_seq not found; migrations may not have run');
    }
    this.blockSize = Number(row.increment_by);
    if (!Number.isInteger(this.blockSize) || this.blockSize < 1) {
      throw new Error(`Unexpected logs_id_seq increment: ${row.increment_by}`);
    }
    logger.info('id allocator ready', { blockSize: this.blockSize });
  }

  /** Ids available without touching the database. */
  get available(): number {
    return this.remaining;
  }

  /**
   * Takes one id. Callers must have confirmed `available > 0` first; this is
   * the synchronous inner-loop path and deliberately does no checking.
   */
  take(): number {
    const id = this.nextId;
    this.nextId += 1;
    this.remaining -= 1;
    return id;
  }

  /**
   * Ensures at least one id is available, fetching a fresh block if not.
   *
   * Concurrent callers share a single in-flight fetch rather than each
   * reserving a block, which would waste ids and round trips.
   */
  async ensureAvailable(): Promise<void> {
    if (this.remaining > 0) return;

    if (this.inFlight !== null) {
      await this.inFlight;
      // Another caller's block may already have been consumed; re-check.
      if (this.remaining > 0) return;
    }

    const fetch = this.fetchBlock();
    this.inFlight = fetch;
    try {
      await fetch;
    } finally {
      if (this.inFlight === fetch) this.inFlight = null;
    }
  }

  private async fetchBlock(): Promise<void> {
    const result = await this.pool.query<{ start: string }>(
      `SELECT nextval('logs_id_seq') AS start`,
    );
    const row = result.rows[0];
    if (row === undefined) throw new Error('nextval returned no rows');

    // Any leftover ids from the previous block are abandoned; blocks obtained
    // from separate calls are not guaranteed to be adjacent.
    this.nextId = Number(row.start);
    this.remaining = this.blockSize;
  }
}
