import type { QueryPool } from '../db/pool.ts';
import type { ServiceDictionary } from '../db/services.ts';
import { codeToLevel } from '../domain/levels.ts';
import { badRequest } from '../http/errors.ts';
import type { AggregateBucket } from '../domain/types.ts';
import { ParamList, buildWhereClause, type Filters } from './filters.ts';

/**
 * Time-bucketed aggregation for GET /logs/aggregate.
 *
 * `since` and `until` are mandatory here, which is what makes the endpoint
 * cheap: every query is bounded, so partition pruning limits the scan to the
 * days actually in range instead of the whole table.
 */

export type BucketSize = '1m' | '5m' | '1h' | '1d';
export type GroupBy = 'service' | 'level';

/** Closed set; the value reaching SQL is one of these constants, never input. */
const BUCKET_INTERVALS: Record<BucketSize, string> = {
  '1m': '1 minute',
  '5m': '5 minutes',
  '1h': '1 hour',
  '1d': '1 day',
};

const BUCKET_MICROS: Record<BucketSize, number> = {
  '1m': 60_000_000,
  '5m': 300_000_000,
  '1h': 3_600_000_000,
  '1d': 86_400_000_000,
};

/**
 * Ceiling on buckets per response. A month at 1m granularity is ~43,200, so
 * this is comfortably above any legitimate request while still refusing one
 * that would try to materialise millions of rows.
 */
const MAX_BUCKETS = 100_000;

export function parseBucketSize(raw: string | null): BucketSize {
  if (raw === null) throw badRequest('bucket is required (one of: 1m, 5m, 1h, 1d)');
  if (raw === '1m' || raw === '5m' || raw === '1h' || raw === '1d') return raw;
  throw badRequest(`invalid bucket: '${raw}' (expected one of: 1m, 5m, 1h, 1d)`);
}

export function parseGroupBy(raw: string | null): GroupBy | null {
  if (raw === null) return null;
  if (raw === 'service' || raw === 'level') return raw;
  throw badRequest(`invalid group_by: '${raw}' (expected one of: service, level)`);
}

/**
 * Formats a bucket start without a fractional part.
 *
 * Bucket boundaries are aligned to whole minutes or larger, so milliseconds are
 * always zero; emitting "2026-07-20T14:00:00Z" matches the documented response
 * shape exactly.
 */
function formatBucketStart(bucket: Date): string {
  return bucket.toISOString().replace('.000Z', 'Z');
}

interface AggregateRow {
  bucket: Date;
  group_key: number | null;
  count: string;
}

export interface AggregateQuery {
  filters: Filters;
  bucket: BucketSize;
  groupBy: GroupBy | null;
}

export async function aggregateLogs(
  pool: QueryPool,
  services: ServiceDictionary,
  query: AggregateQuery,
): Promise<AggregateBucket[]> {
  const { sinceMicros, untilMicros } = query.filters;

  if (sinceMicros === null) throw badRequest('since is required');
  if (untilMicros === null) throw badRequest('until is required');

  const bucketCount = Math.ceil((untilMicros - sinceMicros) / BUCKET_MICROS[query.bucket]);
  if (bucketCount > MAX_BUCKETS) {
    throw badRequest(
      `requested range yields ${bucketCount} buckets, above the limit of ${MAX_BUCKETS}; widen bucket or narrow the range`,
    );
  }

  const params = new ParamList();
  const where = buildWhereClause(query.filters, params, services);
  if (where.impossible) return [];

  // Selected from a closed set keyed by an already-validated union type, so
  // this is a constant rather than user input.
  const interval = BUCKET_INTERVALS[query.bucket];

  // Anchoring to the Unix epoch gives buckets that align to natural wall-clock
  // boundaries (top of the minute, hour, UTC midnight) instead of to whatever
  // `since` happened to be.
  const bucketExpression = `date_bin(INTERVAL '${interval}', ts, TIMESTAMPTZ 'epoch')`;

  const groupExpression =
    query.groupBy === 'service' ? 'service_id' : query.groupBy === 'level' ? 'level' : 'NULL::int';

  // The bucket is selected as a bare timestamptz and formatted in the
  // application, rather than converted to epoch microseconds in SQL.
  //
  // The conversion is not free: wrapping the bucket in
  // `(EXTRACT(EPOCH FROM ...) * 1000000)::bigint` makes that arithmetic part of
  // the GROUP BY key, so PostgreSQL evaluates numeric multiplication and a cast
  // once per *input row* rather than once per output bucket. Measured over 1.4M
  // rows that cost 737 ms of a 1164 ms query - 63% of the runtime spent
  // formatting. Grouping on the bare bucket cuts the same query to 427 ms,
  // against a 350 ms floor for the scan itself.
  const sql = `
    SELECT ${bucketExpression} AS bucket,
           ${groupExpression} AS group_key,
           count(*) AS count
    FROM logs
    ${where.sql}
    GROUP BY 1, 2
    ORDER BY 1 ASC, 2 ASC NULLS FIRST
  `;

  const result = await pool.query<AggregateRow>(sql, params.all);

  if (query.groupBy === 'service') {
    await services.hydrateIds(
      result.rows.map((row) => row.group_key).filter((key): key is number => key !== null),
    );
  }

  return result.rows.map((row) => ({
    start: formatBucketStart(row.bucket),
    group: resolveGroup(row.group_key, query.groupBy, services),
    count: Number(row.count),
  }));
}

function resolveGroup(
  key: number | null,
  groupBy: GroupBy | null,
  services: ServiceDictionary,
): string | null {
  if (groupBy === null || key === null) return null;
  if (groupBy === 'level') return codeToLevel(key);
  return services.nameOf(key) ?? `unknown-${key}`;
}
