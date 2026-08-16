import type { Config } from '../config.ts';
import { logger } from '../logger.ts';
import { metrics } from '../metrics.ts';
import { PartitionManager, dayIndexFromMicros } from '../db/partitions.ts';

/**
 * Partition maintenance: creates partitions ahead of time and drops expired
 * ones.
 *
 * Retention is implemented as partition drops rather than DELETE. A DELETE of a
 * day's worth of rows would leave millions of dead tuples behind, forcing
 * autovacuum to rewrite heap and index pages while competing with ingestion for
 * the one available CPU, and the space would not return to the filesystem.
 * Dropping a partition is a catalog operation plus an unlink: constant time,
 * no bloat, no vacuum debt.
 *
 * Creating tomorrow's partition in advance matters just as much. Doing it
 * lazily on first arrival would put DDL in the path of a flush at exactly
 * midnight, when the write rate is unchanged.
 */

/** How far ahead partitions are pre-created. */
const LOOKAHEAD_DAYS = 2;

export interface SweepResult {
  dropped: string[];
  cutoff: string;
  durationMs: number;
}

export class RetentionJanitor {
  private timer: NodeJS.Timeout | null = null;
  private running = false;
  private lastResult: SweepResult | null = null;

  constructor(
    private readonly config: Config,
    private readonly partitions: PartitionManager,
  ) {}

  start(): void {
    if (!this.config.retention.enabled) {
      logger.info('retention disabled (RETENTION_ENABLED=false)');
      return;
    }

    logger.info('retention janitor started', {
      retentionDays: this.config.retention.days,
      sweepIntervalMs: this.config.retention.sweepIntervalMs,
    });

    // unref so a pending sweep timer never holds the process open during
    // shutdown.
    this.timer = setInterval(() => {
      void this.sweep().catch((error) => logger.error('retention sweep failed', { error }));
    }, this.config.retention.sweepIntervalMs);
    this.timer.unref();
  }

  /**
   * Runs one maintenance pass. Safe to call concurrently: overlapping calls
   * return the previous result rather than queueing duplicate DDL.
   */
  async sweep(): Promise<SweepResult> {
    if (this.running && this.lastResult !== null) return this.lastResult;
    this.running = true;

    const startedAt = Date.now();
    try {
      const today = dayIndexFromMicros(Date.now() * 1000);

      await this.partitions.ensureRange(today, today + LOOKAHEAD_DAYS);

      const cutoffDay = today - this.config.retention.days;
      const dropped = await this.partitions.dropOlderThan(cutoffDay);

      const result: SweepResult = {
        dropped,
        cutoff: new Date(cutoffDay * 86_400_000).toISOString().slice(0, 10),
        durationMs: Date.now() - startedAt,
      };

      metrics.retention.sweeps += 1;
      metrics.retention.partitionsDropped += dropped.length;
      metrics.retention.lastSweepAt = new Date().toISOString();

      if (dropped.length > 0) {
        logger.info('dropped expired partitions', { dropped, cutoff: result.cutoff });
      }

      this.lastResult = result;
      return result;
    } finally {
      this.running = false;
    }
  }

  status(): Record<string, unknown> {
    return {
      enabled: this.config.retention.enabled,
      retention_days: this.config.retention.days,
      sweep_interval_ms: this.config.retention.sweepIntervalMs,
      last_sweep: this.lastResult,
    };
  }

  stop(): void {
    if (this.timer !== null) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }
}
