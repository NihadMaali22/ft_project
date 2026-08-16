import { levelToCode, INVALID_LEVEL, LEVELS } from '../domain/levels.ts';
import { parseIsoToMicros, formatMicrosPgLiteral } from '../domain/time.ts';
import { badRequest } from '../http/errors.ts';
import type { ServiceDictionary } from '../db/services.ts';

/**
 * Shared filter parsing and SQL construction for GET /logs and
 * GET /logs/aggregate.
 *
 * Both endpoints accept the same filter vocabulary, so it is defined once here.
 * Handlers deal in HTTP; this module deals in SQL; neither knows about the
 * other's concerns.
 *
 * Every user-supplied value - including attribute *keys* - becomes a bind
 * parameter. No request data is ever concatenated into a statement. The only
 * strings interpolated into SQL anywhere in this file are compile-time
 * constants chosen by a switch over a closed set.
 */

/** Collects bind parameters and hands back their placeholders. */
export class ParamList {
  private readonly values: unknown[] = [];

  add(value: unknown): string {
    this.values.push(value);
    return `$${this.values.length}`;
  }

  get all(): unknown[] {
    return this.values;
  }

  get length(): number {
    return this.values.length;
  }
}

export interface Filters {
  serviceName: string | null;
  levelCode: number | null;
  sinceMicros: number | null;
  untilMicros: number | null;
  attributes: Array<{ key: string; value: string }>;
  q: string | null;
}

/** Ceiling on attribute predicates, to bound planner and execution work. */
const MAX_ATTRIBUTE_FILTERS = 16;

const MAX_Q_LENGTH = 1024;

function parseTimestampParam(raw: string, name: string): number {
  const micros = parseIsoToMicros(raw);
  if (micros === null) {
    throw badRequest(`invalid ${name}: ${JSON.stringify(raw)} is not a valid ISO 8601 timestamp`);
  }
  return micros;
}

/**
 * Parses the filter parameters common to both query endpoints.
 * Throws HttpError(400) on any invalid value.
 */
export function parseFilters(params: URLSearchParams): Filters {
  const filters: Filters = {
    serviceName: null,
    levelCode: null,
    sinceMicros: null,
    untilMicros: null,
    attributes: [],
    q: null,
  };

  const service = params.get('service');
  if (service !== null) {
    if (service.length === 0) throw badRequest('service must be a non-empty string');
    filters.serviceName = service;
  }

  const level = params.get('level');
  if (level !== null) {
    const code = levelToCode(level);
    if (code === INVALID_LEVEL) {
      throw badRequest(`invalid level: '${level}' (expected one of: ${LEVELS.join(', ')})`);
    }
    filters.levelCode = code;
  }

  const since = params.get('since');
  if (since !== null) filters.sinceMicros = parseTimestampParam(since, 'since');

  const until = params.get('until');
  if (until !== null) filters.untilMicros = parseTimestampParam(until, 'until');

  if (
    filters.sinceMicros !== null &&
    filters.untilMicros !== null &&
    filters.untilMicros < filters.sinceMicros
  ) {
    throw badRequest('until must not be earlier than since');
  }

  const q = params.get('q');
  if (q !== null) {
    if (q.length > MAX_Q_LENGTH) throw badRequest(`q exceeds ${MAX_Q_LENGTH} characters`);
    if (q.length > 0) filters.q = q;
  }

  for (const [key, value] of params) {
    if (!key.startsWith('attr.')) continue;
    const attributeKey = key.slice(5);
    if (attributeKey.length === 0) {
      throw badRequest('attribute filter is missing a key (expected attr.<key>=<value>)');
    }
    filters.attributes.push({ key: attributeKey, value });
  }

  if (filters.attributes.length > MAX_ATTRIBUTE_FILTERS) {
    throw badRequest(`at most ${MAX_ATTRIBUTE_FILTERS} attribute filters are supported`);
  }

  return filters;
}

/**
 * Binds epoch microseconds as a timestamptz parameter.
 *
 * Deliberately a plain `$n::timestamptz` over a microsecond-precision literal.
 * Deriving the instant arithmetically instead (epoch plus an interval computed
 * from a bigint parameter) yields a stable rather than immutable expression,
 * which PostgreSQL cannot use for partition pruning - turning every bounded
 * query into a full scan of every partition.
 */
export function timestampFromMicros(params: ParamList, micros: number): string {
  return `${params.add(formatMicrosPgLiteral(micros))}::timestamptz`;
}

/**
 * Escapes LIKE metacharacters so `q` behaves as a literal substring match.
 *
 * Without this, a message containing "%" would silently become a wildcard and
 * `q=100%` would match far more than it should.
 */
export function escapeLikePattern(value: string): string {
  return value.replace(/[\\%_]/g, (character) => `\\${character}`);
}

export interface WhereClause {
  sql: string;
  /**
   * True when a filter can provably match nothing, e.g. a service name that has
   * never been seen. The caller returns an empty result without querying.
   */
  impossible: boolean;
}

/**
 * Renders filters into a WHERE clause.
 *
 * `serviceName` is resolved through the in-memory dictionary rather than joined,
 * so the predicate reduces to an integer comparison.
 */
export function buildWhereClause(
  filters: Filters,
  params: ParamList,
  services: ServiceDictionary,
): WhereClause {
  const conditions: string[] = [];

  if (filters.sinceMicros !== null) {
    conditions.push(`ts >= ${timestampFromMicros(params, filters.sinceMicros)}`);
  }
  if (filters.untilMicros !== null) {
    conditions.push(`ts < ${timestampFromMicros(params, filters.untilMicros)}`);
  }

  if (filters.serviceName !== null) {
    const serviceId = services.lookupForFilter(filters.serviceName);
    if (serviceId === null) {
      // A service that has never logged is a valid query with zero matches,
      // not an error.
      return { sql: '', impossible: true };
    }
    conditions.push(`service_id = ${params.add(serviceId)}`);
  }

  if (filters.levelCode !== null) {
    conditions.push(`level = ${params.add(filters.levelCode)}`);
  }

  for (const attribute of filters.attributes) {
    // Both key and value are bind parameters. `->>` yields the value as text,
    // which is what makes numeric and boolean attributes compare as strings
    // exactly as the contract specifies: {"retries": 3} matches attr.retries=3.
    conditions.push(`attributes ->> ${params.add(attribute.key)} = ${params.add(attribute.value)}`);
  }

  if (filters.q !== null) {
    conditions.push(
      `message ILIKE ${params.add(`%${escapeLikePattern(filters.q)}%`)} ESCAPE '\\'`,
    );
  }

  return {
    sql: conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '',
    impossible: false,
  };
}
