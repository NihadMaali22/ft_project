/**
 * Typed, validated configuration read once at startup.
 *
 * Every optional feature defaults to its off state so that `docker compose up`
 * with no environment file yields the plain, unauthenticated core service.
 */

function str(name: string, fallback: string): string {
  const raw = process.env[name];
  return raw === undefined || raw === '' ? fallback : raw;
}

function optionalStr(name: string): string | null {
  const raw = process.env[name];
  return raw === undefined || raw === '' ? null : raw;
}

function int(name: string, fallback: number, min: number, max: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return fallback;
  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed < min || parsed > max) {
    throw new Error(
      `Invalid ${name}: expected an integer in [${min}, ${max}], received ${JSON.stringify(raw)}`,
    );
  }
  return parsed;
}

/** Strict boolean parsing: anything not explicitly truthy is false. */
function bool(name: string, fallback: boolean): boolean {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return fallback;
  const normalised = raw.trim().toLowerCase();
  if (normalised === 'true' || normalised === '1' || normalised === 'yes') return true;
  if (normalised === 'false' || normalised === '0' || normalised === 'no') return false;
  throw new Error(`Invalid ${name}: expected a boolean, received ${JSON.stringify(raw)}`);
}

export interface Config {
  readonly port: number;
  readonly databaseUrl: string;

  /** Per-session synchronous_commit for writer connections: 'on' | 'off' | 'local'. */
  readonly synchronousCommit: string;

  readonly ingest: {
    /** Group-commit window. Bounds ingest latency; batch size self-tunes with load. */
    readonly flushIntervalMs: number;
    /** Hard cap on rows per COPY, so a traffic spike cannot build an unbounded buffer. */
    readonly maxBatchRows: number;
    /** Backpressure threshold. Beyond this the service sheds with 503 + Retry-After. */
    readonly maxPendingRows: number;
    /** Dedicated COPY connections. Parallelism hides per-flush round-trip latency. */
    readonly writerConnections: number;
    /** Largest accepted request body, in bytes. */
    readonly maxBodyBytes: number;
    /** How long a queued batch may wait for a writer before it is failed. */
    readonly batchTimeoutMs: number;
  };

  readonly query: {
    readonly poolSize: number;
    readonly defaultLimit: number;
    readonly maxLimit: number;
    readonly statementTimeoutMs: number;
  };

  readonly retention: {
    readonly days: number;
    readonly sweepIntervalMs: number;
    readonly enabled: boolean;
  };

  readonly indexes: {
    /** Opt-in GIN index on attributes. Off by default: it taxes every insert. */
    readonly attrGin: boolean;
    /** Opt-in (service_id, ts DESC) index for service-filtered queries. */
    readonly serviceTs: boolean;
  };

  readonly auth: {
    readonly enabled: boolean;
    readonly loadgenApiKey: string | null;
  };

  readonly rateLimit: {
    readonly enabled: boolean;
    readonly requestsPerSecond: number;
  };
}

export function loadConfig(): Config {
  return {
    port: int('PORT', 8080, 1, 65535),
    databaseUrl: str('DATABASE_URL', 'postgres://logs:logs@localhost:5432/logs'),

    synchronousCommit: (() => {
      const value = str('PG_SYNCHRONOUS_COMMIT', 'off').trim().toLowerCase();
      // Whitelisted because this value is interpolated into a SET statement.
      if (!['on', 'off', 'local', 'remote_write', 'remote_apply'].includes(value)) {
        throw new Error(`Invalid PG_SYNCHRONOUS_COMMIT: ${JSON.stringify(value)}`);
      }
      return value;
    })(),

    ingest: {
      // Upper bound only. Batches normally leave as soon as a writer is free,
      // so this caps how long one waits when every writer is busy; it is not
      // the latency every request pays.
      flushIntervalMs: int('INGEST_FLUSH_INTERVAL_MS', 5, 1, 5000),
      maxBatchRows: int('INGEST_MAX_BATCH_ROWS', 10_000, 100, 200_000),
      maxPendingRows: int('INGEST_MAX_PENDING_ROWS', 60_000, 1000, 1_000_000),
      // Concurrent COPYs in flight. With flushes released on demand rather than
      // on a timer, this sets how many round trips can overlap, which is the
      // throughput term for a client that waits for each response.
      writerConnections: int('INGEST_WRITER_CONNECTIONS', 8, 1, 32),
      maxBodyBytes: int('INGEST_MAX_BODY_BYTES', 32 * 1024 * 1024, 1024, 256 * 1024 * 1024),
      batchTimeoutMs: int('INGEST_BATCH_TIMEOUT_MS', 30_000, 1000, 600_000),
    },

    query: {
      poolSize: int('QUERY_POOL_SIZE', 4, 1, 32),
      defaultLimit: 100,
      maxLimit: 1000,
      statementTimeoutMs: int('QUERY_STATEMENT_TIMEOUT_MS', 30_000, 100, 600_000),
    },

    retention: {
      days: int('RETENTION_DAYS', 30, 1, 3650),
      sweepIntervalMs: int('RETENTION_SWEEP_INTERVAL_MS', 300_000, 1000, 86_400_000),
      enabled: bool('RETENTION_ENABLED', true),
    },

    indexes: {
      attrGin: bool('ATTR_GIN_INDEX', false),
      serviceTs: bool('SERVICE_TS_INDEX', false),
    },

    auth: {
      enabled: bool('AUTH_ENABLED', false),
      loadgenApiKey: optionalStr('LOADGEN_API_KEY'),
    },

    rateLimit: {
      enabled: bool('RATE_LIMIT_ENABLED', false),
      requestsPerSecond: int('RATE_LIMIT_RPS', 10_000, 1, 1_000_000),
    },
  };
}
