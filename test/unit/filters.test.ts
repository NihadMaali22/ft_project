import test from 'node:test';
import assert from 'node:assert/strict';
import {
  parseFilters,
  buildWhereClause,
  escapeLikePattern,
  ParamList,
} from '../../src/query/filters.ts';
import type { ServiceDictionary } from '../../src/db/services.ts';
import { HttpError } from '../../src/http/errors.ts';

/** Stand-in dictionary: only `checkout` and `auth` have ever logged. */
const services = {
  lookupForFilter: (name: string): number | null =>
    name === 'checkout' ? 10 : name === 'auth' ? 11 : null,
} as unknown as ServiceDictionary;

function params(query: string): URLSearchParams {
  return new URLSearchParams(query);
}

function expectBadRequest(query: string, pattern: RegExp): void {
  assert.throws(
    () => parseFilters(params(query)),
    (error: unknown) => {
      assert.ok(error instanceof HttpError);
      assert.equal(error.status, 400);
      assert.match(error.message, pattern);
      return true;
    },
  );
}

test('parses every documented filter parameter', () => {
  const filters = parseFilters(
    params(
      'service=checkout&level=error&since=2026-07-20T14:00:00Z&until=2026-07-20T15:00:00Z&q=declined&attr.user_id=42&attr.region=eu-west',
    ),
  );

  assert.equal(filters.serviceName, 'checkout');
  assert.equal(filters.levelCode, 3);
  assert.equal(filters.q, 'declined');
  assert.deepEqual(filters.attributes, [
    { key: 'user_id', value: '42' },
    { key: 'region', value: 'eu-west' },
  ]);
  assert.ok(filters.sinceMicros !== null && filters.untilMicros !== null);
  assert.ok(filters.untilMicros > filters.sinceMicros);
});

test('all filters are optional', () => {
  const filters = parseFilters(params(''));
  assert.equal(filters.serviceName, null);
  assert.equal(filters.levelCode, null);
  assert.equal(filters.sinceMicros, null);
  assert.equal(filters.untilMicros, null);
  assert.equal(filters.q, null);
  assert.deepEqual(filters.attributes, []);
});

test('rejects the documented invalid-parameter cases', () => {
  expectBadRequest('since=yesterday', /invalid since/);
  expectBadRequest('until=2026-13-45T00:00:00Z', /invalid until/);
  expectBadRequest('level=critical', /invalid level/);
  expectBadRequest('since=2026-07-20T15:00:00Z&until=2026-07-20T14:00:00Z', /until must not be earlier/);
  expectBadRequest('attr.=42', /missing a key/);
});

test('an equal since and until is a valid empty range', () => {
  const filters = parseFilters(params('since=2026-07-20T14:00:00Z&until=2026-07-20T14:00:00Z'));
  assert.equal(filters.sinceMicros, filters.untilMicros);
});

test('caps the number of attribute filters', () => {
  const many = Array.from({ length: 20 }, (_, i) => `attr.k${i}=v`).join('&');
  expectBadRequest(many, /at most 16 attribute filters/);
});

// --- SQL construction -------------------------------------------------------

test('every user-supplied value becomes a bind parameter', () => {
  const filters = parseFilters(
    params('service=checkout&level=error&since=2026-07-20T14:00:00Z&q=boom&attr.user_id=42'),
  );
  const list = new ParamList();
  const where = buildWhereClause(filters, list, services);

  // No literal from the request may appear in the SQL text itself.
  assert.ok(!where.sql.includes('checkout'));
  assert.ok(!where.sql.includes('boom'));
  assert.ok(!where.sql.includes('user_id'));
  assert.ok(!where.sql.includes('42'));

  assert.deepEqual(list.all, [
    '2026-07-20T14:00:00.000000Z', // since, as a timestamptz literal
    10, // service_id resolved via the dictionary
    3, // level code
    'user_id', // attribute key, parameterised rather than concatenated
    '42', // attribute value
    '%boom%',
  ]);
});

test('time bounds bind as timestamptz so partition pruning works', () => {
  // Regression guard. Deriving the bound arithmetically from a bigint parameter
  // produces a stable expression that PostgreSQL will not prune on, which turned
  // every bounded query into a full scan of all partitions.
  const filters = parseFilters(
    params('since=2026-07-20T14:00:00Z&until=2026-07-20T15:00:00.123456Z'),
  );
  const list = new ParamList();
  const where = buildWhereClause(filters, list, services);

  assert.match(where.sql, /ts >= \$1::timestamptz/);
  assert.match(where.sql, /ts < \$2::timestamptz/);
  assert.ok(!where.sql.includes('INTERVAL'), 'no interval arithmetic in the predicate');
  // Microsecond precision survives the round trip.
  assert.equal(list.all[1], '2026-07-20T15:00:00.123456Z');
});

test('an attribute key containing SQL syntax stays inert', () => {
  const hostile = "x') OR 1=1 --";
  const filters = parseFilters(params(`attr.${encodeURIComponent(hostile)}=1`));
  const list = new ParamList();
  const where = buildWhereClause(filters, list, services);

  assert.ok(!where.sql.includes('OR 1=1'));
  assert.match(where.sql, /attributes ->> \$1 = \$2/);
  assert.deepEqual(list.all, [hostile, '1']);
});

test('a service that has never logged yields no matches rather than an error', () => {
  const filters = parseFilters(params('service=does-not-exist'));
  const where = buildWhereClause(filters, new ParamList(), services);
  assert.equal(where.impossible, true);
});

test('an empty filter set produces no WHERE clause', () => {
  const where = buildWhereClause(parseFilters(params('')), new ParamList(), services);
  assert.equal(where.sql, '');
  assert.equal(where.impossible, false);
});

test('attribute comparison uses ->> so numbers compare as strings', () => {
  // {"retries": 3} must match attr.retries=3, which is what ->> guarantees.
  const filters = parseFilters(params('attr.retries=3'));
  const where = buildWhereClause(filters, new ParamList(), services);
  assert.match(where.sql, /attributes ->>/);
});

// --- LIKE escaping ----------------------------------------------------------

test('escapes LIKE metacharacters so q is a literal substring match', () => {
  assert.equal(escapeLikePattern('100%'), '100\\%');
  assert.equal(escapeLikePattern('a_b'), 'a\\_b');
  assert.equal(escapeLikePattern('back\\slash'), 'back\\\\slash');
  assert.equal(escapeLikePattern('%_\\'), '\\%\\_\\\\');
});

test('leaves ordinary text untouched', () => {
  assert.equal(escapeLikePattern('payment declined'), 'payment declined');
});

test('q is wrapped in wildcards after escaping', () => {
  const filters = parseFilters(params('q=100%25'));
  const list = new ParamList();
  buildWhereClause(filters, list, services);
  assert.deepEqual(list.all, ['%100\\%%']);
});
