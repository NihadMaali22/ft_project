/**
 * Pre-serialised request payloads.
 *
 * The generator has to sustain 45,000 logs/second without becoming the
 * bottleneck itself. Building and stringifying batches on the fly would spend
 * more CPU in the generator than in the service under test, so batches are
 * serialised once into Buffers and rotated.
 *
 * The pool is rebuilt periodically because timestamps go stale: entries must
 * stay close to "now" for the data to land in the current partition and to
 * reflect realistic ingestion.
 */

const SERVICES = [
  'checkout',
  'auth',
  'payments',
  'inventory',
  'search',
  'notifications',
  'gateway',
  'recommendations',
];

const REGIONS = ['eu-west', 'eu-central', 'us-east', 'us-west', 'ap-south'];

/** Weighted so the mix resembles production: mostly info, few errors. */
const LEVEL_WEIGHTS: Array<[string, number]> = [
  ['info', 60],
  ['debug', 20],
  ['warn', 15],
  ['error', 5],
];

const LEVEL_TABLE: string[] = LEVEL_WEIGHTS.flatMap(([level, weight]) =>
  Array.from({ length: weight }, () => level),
);

const MESSAGES = [
  'payment declined',
  'request completed successfully',
  'cache miss for key',
  'upstream timeout while calling downstream service',
  'user session expired',
  'rate limit threshold approached',
  'database connection acquired',
  'retrying failed operation',
  'inventory reservation confirmed',
  'search query executed',
];

function pick<T>(values: readonly T[]): T {
  return values[(Math.random() * values.length) | 0] as T;
}

export interface PayloadPoolOptions {
  batchSize: number;
  poolSize: number;
  /** Spreads timestamps backwards over this many milliseconds. */
  jitterMs?: number;
}

/**
 * Builds one JSON body containing `batchSize` entries.
 * `timestampBase` is the newest timestamp in the batch.
 */
export function buildBatch(batchSize: number, timestampBase: number, jitterMs: number): Buffer {
  const logs = new Array<unknown>(batchSize);

  for (let i = 0; i < batchSize; i++) {
    logs[i] = {
      timestamp: new Date(timestampBase - Math.random() * jitterMs).toISOString(),
      level: pick(LEVEL_TABLE),
      service: pick(SERVICES),
      message: `${pick(MESSAGES)} #${(Math.random() * 100000) | 0}`,
      attributes: {
        user_id: String((Math.random() * 50000) | 0),
        request_id: `req-${((Math.random() * 1e9) | 0).toString(36)}`,
        region: pick(REGIONS),
        retries: (Math.random() * 4) | 0,
        cached: Math.random() < 0.5,
      },
    };
  }

  return Buffer.from(JSON.stringify({ logs }), 'utf8');
}

export class PayloadPool {
  private buffers: Buffer[] = [];
  private cursor = 0;
  private readonly jitterMs: number;

  constructor(private readonly options: PayloadPoolOptions) {
    this.jitterMs = options.jitterMs ?? 2000;
    this.refresh();
  }

  /** Rebuilds every batch against the current clock. */
  refresh(): void {
    const now = Date.now();
    const next: Buffer[] = new Array(this.options.poolSize);
    for (let i = 0; i < this.options.poolSize; i++) {
      next[i] = buildBatch(this.options.batchSize, now, this.jitterMs);
    }
    this.buffers = next;
  }

  next(): Buffer {
    const buffer = this.buffers[this.cursor] as Buffer;
    this.cursor = (this.cursor + 1) % this.buffers.length;
    return buffer;
  }

  get batchSize(): number {
    return this.options.batchSize;
  }

  /** Average serialised bytes per entry, for reporting. */
  get bytesPerEntry(): number {
    const total = this.buffers.reduce((sum, buffer) => sum + buffer.length, 0);
    return Math.round(total / (this.buffers.length * this.options.batchSize));
  }
}
