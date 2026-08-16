import pg from 'pg';
import copyFrom from 'pg-copy-streams';
import type { Config } from '../config.ts';
import { logger } from '../logger.ts';
import { metrics } from '../metrics.ts';
import { CopyBinaryEncoder } from '../db/copy-encoder.ts';
import { IdAllocator } from '../db/ids.ts';
import type { ServiceDictionary } from '../db/services.ts';
import { PartitionManager, dayIndexFromMicros } from '../db/partitions.ts';
import type { QueryPool } from '../db/pool.ts';
import type { ValidLogRow } from '../domain/types.ts';

/**
 * Group-commit ingestion writer.
 *
 * The contract forbids answering 200 for a batch that is not durably accepted,
 * so a request cannot be acknowledged before its rows commit. Doing one COPY
 * per request would then put a full database round trip on every request and
 * cap throughput at the round-trip rate.
 *
 * Instead requests deposit their rows into a shared open batch and await its
 * commit. A flush is triggered by a short timer, so batch size self-tunes with
 * load: at 15k rows/s a 20 ms window holds roughly 300 rows, at 45k/s roughly
 * 900, while per-request latency stays pinned near the flush interval either
 * way. Several writer connections run in parallel so one flush's round trip
 * does not stall the next.
 */

const COPY_SQL =
  'COPY logs (id, ts, service_id, level, message, attributes) FROM STDIN (FORMAT binary)';

/** Byte ceiling per batch, independent of the row ceiling. */
const MAX_BATCH_BYTES = 8 * 1024 * 1024;

/** Encoders above this size are released rather than retained between flushes. */
const MAX_RETAINED_ENCODER_BYTES = 4 * 1024 * 1024;

/** Grace period before failing batches when no writer connection is healthy. */
const DB_UNAVAILABLE_GRACE_MS = 5000;

/** Signals that the service is shedding load; surfaced as 503 + Retry-After. */
export class BackpressureError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'BackpressureError';
  }
}

interface Batch {
  encoder: CopyBinaryEncoder;
  minDay: number;
  maxDay: number;
  createdAt: number;
  /** When the batch entered the dispatch queue; drives the stall watchdog. */
  queuedAt: number;
  flushed: boolean;
  timer: NodeJS.Timeout | null;
  promise: Promise<void>;
  resolve: () => void;
  reject: (error: Error) => void;
}

interface Writer {
  client: pg.Client;
  busy: boolean;
  healthy: boolean;
  reconnecting: boolean;
}

export class IngestWriter {
  private readonly writers: Writer[] = [];
  private readonly encoderPool: CopyBinaryEncoder[] = [];
  private readonly queue: Batch[] = [];

  private openBatch: Batch | null = null;
  /** Rows accepted but not yet committed; the backpressure signal. */
  private pendingRows = 0;
  private stopped = false;
  private watchdog: NodeJS.Timeout | null = null;

  private readonly ids: IdAllocator;

  constructor(
    private readonly config: Config,
    private readonly pool: QueryPool,
    private readonly services: ServiceDictionary,
    /**
     * Shared with the retention janitor rather than owned here, so that a
     * partition the janitor drops is also removed from the cache this path
     * consults. Two managers would each hold a private view and drift apart.
     *
     * Its DDL runs on a single dedicated connection, and node-pg serialises
     * queries per Client, so two concurrent flushes cannot race to create the
     * same partition -- the second finds IF NOT EXISTS already satisfied.
     */
    private readonly partitions: PartitionManager,
  ) {
    this.ids = new IdAllocator(pool);
  }

  async start(): Promise<void> {
    await this.ids.init();

    for (let i = 0; i < this.config.ingest.writerConnections; i++) {
      this.writers.push(await this.connectWriter(i));
    }

    // Pre-create today and tomorrow so the first flush never blocks on DDL.
    const today = dayIndexFromMicros(Date.now() * 1000);
    await this.partitions.ensureRange(today, today + 1);

    this.watchdog = setInterval(() => this.failStalledBatches(), 1000);
    this.watchdog.unref();

    logger.info('ingest writer ready', {
      connections: this.writers.length,
      flushIntervalMs: this.config.ingest.flushIntervalMs,
      maxBatchRows: this.config.ingest.maxBatchRows,
      synchronousCommit: this.config.synchronousCommit,
    });
  }

  private async connectWriter(index: number): Promise<Writer> {
    const client = new pg.Client({
      connectionString: this.config.databaseUrl,
      application_name: `logsvc-writer-${index}`,
    });
    await client.connect();

    // Value is whitelisted in config.ts; no request data reaches this string.
    await client.query(`SET synchronous_commit = ${this.config.synchronousCommit}`);

    const writer: Writer = { client, busy: false, healthy: true, reconnecting: false };
    this.attachErrorHandler(writer, client, index);

    return writer;
  }

  /**
   * Marks the writer unhealthy and starts recovery.
   *
   * A pg.Client emitting 'error' with no listener is an uncaught exception, so
   * this listener is what keeps a database restart from killing the process.
   * Recovery is started here rather than only after a failed COPY, because an
   * idle connection can die without any batch in flight to notice.
   */
  private attachErrorHandler(writer: Writer, client: pg.Client, index: number): void {
    client.on('error', (error: Error) => {
      logger.error('writer connection error', { index, error });
      writer.healthy = false;
      if (!writer.reconnecting) void this.reconnect(writer);
    });
  }

  /**
   * Queues validated rows and resolves once every one of them has committed.
   *
   * Rows may be split across batches when an id block runs out or a batch fills
   * mid-request; the request then waits on all batches it contributed to.
   */
  async append(rows: ValidLogRow[]): Promise<void> {
    if (this.stopped) throw new Error('writer is shutting down');

    const count = rows.length;
    if (count === 0) return;

    if (this.pendingRows + count > this.config.ingest.maxPendingRows) {
      metrics.ingest.requestsShed += 1;
      throw new BackpressureError(
        `ingest queue is full (${this.pendingRows} rows pending); retry shortly`,
      );
    }

    this.pendingRows += count;
    const startedAt = performance.now();

    try {
      // Register any first-seen service names. After warm-up this is a no-op
      // that costs one Map lookup per row.
      let unknown: string[] | null = null;
      for (let i = 0; i < count; i++) {
        const service = (rows[i] as ValidLogRow).service;
        if (this.services.lookup(service) === undefined) {
          (unknown ??= []).push(service);
        }
      }
      if (unknown !== null) await this.services.register(unknown);

      const touched = new Set<Batch>();
      let written = 0;

      while (written < count) {
        // Awaits only once per id block, i.e. roughly once per 10,000 rows.
        await this.ids.ensureAvailable();

        const batch = this.currentBatch();
        const take = Math.min(
          count - written,
          this.ids.available,
          this.config.ingest.maxBatchRows - batch.encoder.rows,
        );

        // From here to the end of the loop body is synchronous, so the flusher
        // cannot take this batch mid-write.
        for (let i = 0; i < take; i++) {
          const row = rows[written + i] as ValidLogRow;
          const serviceId = this.services.lookup(row.service);
          if (serviceId === undefined) {
            throw new Error(`service '${row.service}' was not registered`);
          }

          const day = dayIndexFromMicros(row.timestampMicros);
          if (day < batch.minDay) batch.minDay = day;
          if (day > batch.maxDay) batch.maxDay = day;

          batch.encoder.writeLogRow(
            this.ids.take(),
            row.timestampMicros,
            serviceId,
            row.levelCode,
            row.message,
            row.attributesJson,
          );
        }

        written += take;
        touched.add(batch);

        if (
          batch.encoder.rows >= this.config.ingest.maxBatchRows ||
          batch.encoder.byteLength >= MAX_BATCH_BYTES
        ) {
          this.flush(batch);
        }
      }

      await Promise.all([...touched].map((batch) => batch.promise));
      metrics.ingest.ingestLatency.record(performance.now() - startedAt);
    } finally {
      this.pendingRows -= count;
    }
  }

  private currentBatch(): Batch {
    const existing = this.openBatch;
    if (existing !== null && !existing.flushed) return existing;

    const encoder = this.encoderPool.pop() ?? new CopyBinaryEncoder();
    encoder.reset();

    let resolve!: () => void;
    let reject!: (error: Error) => void;
    const promise = new Promise<void>((res, rej) => {
      resolve = res;
      reject = rej;
    });

    const batch: Batch = {
      encoder,
      minDay: Number.POSITIVE_INFINITY,
      maxDay: Number.NEGATIVE_INFINITY,
      createdAt: performance.now(),
      queuedAt: 0,
      flushed: false,
      timer: null,
      promise,
      resolve,
      reject,
    };

    batch.timer = setTimeout(() => this.flush(batch), this.config.ingest.flushIntervalMs);

    this.openBatch = batch;
    return batch;
  }

  private flush(batch: Batch): void {
    if (batch.flushed) return;
    batch.flushed = true;

    if (batch.timer !== null) {
      clearTimeout(batch.timer);
      batch.timer = null;
    }
    if (this.openBatch === batch) this.openBatch = null;

    if (batch.encoder.rows === 0) {
      this.releaseEncoder(batch.encoder);
      batch.resolve();
      return;
    }

    batch.queuedAt = performance.now();
    this.queue.push(batch);
    this.dispatch();
  }

  private dispatch(): void {
    while (this.queue.length > 0) {
      const writer = this.writers.find((candidate) => !candidate.busy && candidate.healthy);
      if (writer === undefined) return;

      const batch = this.queue.shift() as Batch;
      writer.busy = true;
      void this.runCopy(writer, batch);
    }
  }

  private async runCopy(writer: Writer, batch: Batch): Promise<void> {
    const startedAt = performance.now();

    try {
      // Ensures the target partitions exist before rows are routed.
      //
      // ensureRange checks the cache per day and issues DDL only for days it
      // has not seen, so the steady-state cost is one Set lookup per distinct
      // day in the batch - normally one - and no query at all. Testing only the
      // endpoints here would be cheaper still, but would skip a gap in the
      // middle of a range that spans several days.
      if (Number.isFinite(batch.minDay)) {
        await this.partitions.ensureRange(batch.minDay, batch.maxDay);
      }

      const payload = batch.encoder.finish();
      await copyBuffer(writer.client, payload);

      const elapsed = performance.now() - startedAt;
      metrics.ingest.batchesFlushed += 1;
      metrics.ingest.rowsCopied += batch.encoder.rows;
      metrics.ingest.flushLatency.record(elapsed);
      metrics.ingest.batchRows.record(batch.encoder.rows);

      batch.resolve();
    } catch (error) {
      metrics.ingest.copyFailures += 1;
      logger.error('COPY failed; rejecting batch', { rows: batch.encoder.rows, error });

      // No automatic retry. COPY is a single statement and therefore atomic, but
      // a connection lost after the server committed is indistinguishable from
      // one lost before. Retrying could duplicate rows, so the failure is
      // surfaced and the client decides.
      batch.reject(error instanceof Error ? error : new Error(String(error)));

      if (!writer.healthy) void this.reconnect(writer);
    } finally {
      this.releaseEncoder(batch.encoder);
      writer.busy = false;
      this.dispatch();
    }
  }

  private releaseEncoder(encoder: CopyBinaryEncoder): void {
    encoder.shrinkIfOversized(MAX_RETAINED_ENCODER_BYTES);
    if (this.encoderPool.length < this.writers.length + 2) {
      this.encoderPool.push(encoder);
    }
  }

  private async reconnect(writer: Writer): Promise<void> {
    if (this.stopped) return;
    writer.reconnecting = true;

    try {
      await writer.client.end().catch(() => undefined);

      const client = new pg.Client({
        connectionString: this.config.databaseUrl,
        application_name: 'logsvc-writer-reconnected',
      });
      // Attached before connect so a failure during handshake is handled too.
      this.attachErrorHandler(writer, client, -1);
      await client.connect();
      await client.query(`SET synchronous_commit = ${this.config.synchronousCommit}`);

      writer.client = client;
      writer.healthy = true;
      writer.reconnecting = false;
      logger.info('writer reconnected');
      this.dispatch();
    } catch (error) {
      logger.error('writer reconnect failed; retrying', { error });
      const retry = setTimeout(() => void this.reconnect(writer), 1000);
      retry.unref();
    }
  }

  /**
   * Fails batches that have sat in the queue too long.
   *
   * Without this, a database outage long enough to leave every writer unhealthy
   * would strand queued batches indefinitely, and the requests awaiting them
   * would never get an answer. Returning an error is worse than returning 200,
   * but far better than hanging - and it keeps the promise that a 200 always
   * means the rows are stored.
   */
  private failStalledBatches(): void {
    if (this.queue.length === 0) return;

    const now = performance.now();

    // With every writer down there is nothing to wait for, so batches fail
    // after a short grace period instead of the full timeout: the client learns
    // the database is unavailable in seconds and can retry, rather than holding
    // a connection open for half a minute to be told the same thing. The grace
    // period still absorbs a brief blip that reconnection resolves on its own.
    const timeout =
      this.healthyWriterCount === 0 ? DB_UNAVAILABLE_GRACE_MS : this.config.ingest.batchTimeoutMs;

    let index = 0;

    while (index < this.queue.length) {
      const batch = this.queue[index] as Batch;
      if (now - batch.queuedAt < timeout) {
        index += 1;
        continue;
      }
      this.queue.splice(index, 1);
      metrics.ingest.copyFailures += 1;
      logger.error('batch timed out waiting for a healthy writer', {
        rows: batch.encoder.rows,
        waitedMs: Math.round(now - batch.queuedAt),
      });
      this.releaseEncoder(batch.encoder);
      batch.reject(new Error('timed out waiting for a database writer'));
    }
  }

  /** Flushes anything pending and closes the writer connections. */
  async stop(): Promise<void> {
    this.stopped = true;
    if (this.watchdog !== null) {
      clearInterval(this.watchdog);
      this.watchdog = null;
    }
    if (this.openBatch !== null) this.flush(this.openBatch);

    const deadline = Date.now() + 10_000;
    while ((this.queue.length > 0 || this.writers.some((w) => w.busy)) && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 20));
    }

    for (const writer of this.writers) {
      await writer.client.end().catch(() => undefined);
    }
  }

  stats(): Record<string, number> {
    return {
      pending_rows: this.pendingRows,
      queued_batches: this.queue.length,
      busy_writers: this.writers.filter((writer) => writer.busy).length,
      healthy_writers: this.healthyWriterCount,
      total_writers: this.writers.length,
      open_batch_rows: this.openBatch?.encoder.rows ?? 0,
    };
  }

  /** Writer connections currently able to accept a COPY. */
  get healthyWriterCount(): number {
    return this.writers.filter((writer) => writer.healthy).length;
  }
}

/** Streams a pre-encoded binary COPY payload and resolves once it commits. */
function copyBuffer(client: pg.Client, payload: Buffer): Promise<void> {
  return new Promise((resolve, reject) => {
    const stream = client.query(copyFrom.from(COPY_SQL));
    stream.on('error', reject);
    stream.on('finish', () => resolve());
    stream.end(payload);
  });
}
