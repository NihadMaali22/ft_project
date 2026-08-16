/**
 * Process-local metrics.
 *
 * Deliberately tiny: no registry library, no label cardinality, no allocation
 * per observation. Counters are plain numbers and latencies land in a
 * fixed-size ring buffer, so recording a sample on the ingest path costs one
 * array store and one increment.
 */

/** Retains the most recent samples for percentile estimation. */
class LatencyRing {
  private readonly samples: Float64Array;
  private index = 0;
  private filled = 0;

  constructor(capacity = 4096) {
    this.samples = new Float64Array(capacity);
  }

  record(value: number): void {
    this.samples[this.index] = value;
    this.index = (this.index + 1) % this.samples.length;
    if (this.filled < this.samples.length) this.filled += 1;
  }

  get count(): number {
    return this.filled;
  }

  /** Nearest-rank percentile over the retained window. */
  percentile(fraction: number): number {
    if (this.filled === 0) return 0;
    const window = this.samples.slice(0, this.filled);
    window.sort();
    const rank = Math.min(window.length - 1, Math.max(0, Math.ceil(fraction * window.length) - 1));
    return window[rank] as number;
  }

  snapshot(): Record<string, number> {
    return {
      count: this.filled,
      p50: round(this.percentile(0.5)),
      p90: round(this.percentile(0.9)),
      p95: round(this.percentile(0.95)),
      p99: round(this.percentile(0.99)),
      max: round(this.percentile(1)),
    };
  }
}

function round(value: number): number {
  return Math.round(value * 1000) / 1000;
}

export const metrics = {
  ingest: {
    entriesReceived: 0,
    entriesAccepted: 0,
    entriesRejected: 0,
    requestsShed: 0,
    batchesFlushed: 0,
    rowsCopied: 0,
    copyFailures: 0,
    flushLatency: new LatencyRing(),
    batchRows: new LatencyRing(),
    ingestLatency: new LatencyRing(),
  },
  query: {
    logsRequests: 0,
    logsErrors: 0,
    logsLatency: new LatencyRing(),
    aggregateRequests: 0,
    aggregateErrors: 0,
    aggregateLatency: new LatencyRing(),
  },
  retention: {
    sweeps: 0,
    partitionsDropped: 0,
    lastSweepAt: null as string | null,
  },
};

export function snapshotMetrics(extra: Record<string, unknown> = {}): Record<string, unknown> {
  const memory = process.memoryUsage();
  return {
    uptime_seconds: round(process.uptime()),
    ingest: {
      entries_received: metrics.ingest.entriesReceived,
      entries_accepted: metrics.ingest.entriesAccepted,
      entries_rejected: metrics.ingest.entriesRejected,
      requests_shed: metrics.ingest.requestsShed,
      batches_flushed: metrics.ingest.batchesFlushed,
      rows_copied: metrics.ingest.rowsCopied,
      copy_failures: metrics.ingest.copyFailures,
      flush_latency_ms: metrics.ingest.flushLatency.snapshot(),
      batch_rows: metrics.ingest.batchRows.snapshot(),
      end_to_end_latency_ms: metrics.ingest.ingestLatency.snapshot(),
    },
    query: {
      logs_requests: metrics.query.logsRequests,
      logs_errors: metrics.query.logsErrors,
      logs_latency_ms: metrics.query.logsLatency.snapshot(),
      aggregate_requests: metrics.query.aggregateRequests,
      aggregate_errors: metrics.query.aggregateErrors,
      aggregate_latency_ms: metrics.query.aggregateLatency.snapshot(),
    },
    retention: {
      sweeps: metrics.retention.sweeps,
      partitions_dropped: metrics.retention.partitionsDropped,
      last_sweep_at: metrics.retention.lastSweepAt,
    },
    memory: {
      rss_mb: round(memory.rss / 1048576),
      heap_used_mb: round(memory.heapUsed / 1048576),
      heap_total_mb: round(memory.heapTotal / 1048576),
      external_mb: round(memory.external / 1048576),
      array_buffers_mb: round(memory.arrayBuffers / 1048576),
    },
    ...extra,
  };
}
