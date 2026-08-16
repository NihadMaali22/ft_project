import type { QueryPool } from '../db/pool.ts';
import type { ServiceDictionary } from '../db/services.ts';
import { codeToLevel } from '../domain/levels.ts';
import { formatMicrosIso } from '../domain/time.ts';
import { encodeCursor, type Cursor } from '../domain/cursor.ts';
import type { Attributes, LogRecord, QueryPage } from '../domain/types.ts';
import { ParamList, buildWhereClause, timestampFromMicros, type Filters } from './filters.ts';

/**
 * Reads for GET /logs.
 *
 * Timestamps are selected as epoch microseconds rather than as timestamptz.
 * node-postgres would otherwise materialise a JS Date per row, which truncates
 * to milliseconds - silently breaking cursor pagination whenever two rows share
 * a millisecond but differ in microseconds.
 */

interface LogRow {
  id: string;
  ts_us: string;
  service_id: number;
  level: number;
  message: string;
  attributes: Attributes;
}

export interface LogQuery {
  filters: Filters;
  limit: number;
  cursor: Cursor | null;
}

export async function queryLogs(
  pool: QueryPool,
  services: ServiceDictionary,
  query: LogQuery,
): Promise<QueryPage> {
  const params = new ParamList();
  const where = buildWhereClause(query.filters, params, services);

  if (where.impossible) {
    return { logs: [], next_cursor: null };
  }

  const conditions: string[] = [];
  if (where.sql !== '') conditions.push(where.sql.slice('WHERE '.length));

  if (query.cursor !== null) {
    // Row-value comparison, which PostgreSQL can drive directly from the
    // (ts DESC, id DESC) index as a range start rather than as a filter.
    // The id component is what keeps pagination correct across rows sharing a
    // timestamp: without it a tie at a page boundary would drop or repeat rows.
    const tsExpression = timestampFromMicros(params, query.cursor.tsMicros);
    conditions.push(`(ts, id) < (${tsExpression}, ${params.add(query.cursor.id)}::bigint)`);
  }

  const whereSql = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';

  // One row beyond the requested limit, purely to learn whether another page
  // exists. Cheaper and more accurate than a second COUNT query, and it makes
  // next_cursor null exactly when there is nothing more to return.
  const limitPlaceholder = params.add(query.limit + 1);

  const sql = `
    SELECT id,
           (EXTRACT(EPOCH FROM ts) * 1000000)::bigint AS ts_us,
           service_id,
           level,
           message,
           attributes
    FROM logs
    ${whereSql}
    ORDER BY ts DESC, id DESC
    LIMIT ${limitPlaceholder}
  `;

  const result = await pool.query<LogRow>(sql, params.all);

  const hasMore = result.rows.length > query.limit;
  const rows = hasMore ? result.rows.slice(0, query.limit) : result.rows;

  // Covers rows written by another instance since this one cached the
  // dictionary; normally a no-op.
  await services.hydrateIds(rows.map((row) => row.service_id));

  const logs: LogRecord[] = rows.map((row) => ({
    id: row.id,
    timestamp: formatMicrosIso(Number(row.ts_us)),
    level: codeToLevel(row.level),
    service: services.nameOf(row.service_id) ?? `unknown-${row.service_id}`,
    message: row.message,
    attributes: row.attributes ?? {},
  }));

  const last = rows[rows.length - 1];
  const nextCursor =
    hasMore && last !== undefined
      ? encodeCursor({ tsMicros: Number(last.ts_us), id: last.id })
      : null;

  return { logs, next_cursor: nextCursor };
}
