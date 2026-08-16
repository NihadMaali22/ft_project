import test, { before, describe } from 'node:test';
import assert from 'node:assert/strict';

/**
 * Integration tests against a running stack (`docker compose up`).
 *
 * These cover the behaviour that cannot be verified without a real PostgreSQL:
 * the binary COPY round trip, keyset pagination across rows sharing a
 * timestamp, partition routing, and aggregation arithmetic.
 */

const BASE_URL = process.env.TEST_BASE_URL ?? 'http://127.0.0.1:8080';
const API_KEY = process.env.LOADGEN_API_KEY;

/** Unique per run, so repeated runs cannot see each other's rows. */
const RUN_ID = `it-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

function headers(): Record<string, string> {
  const result: Record<string, string> = { 'content-type': 'application/json' };
  // Always sent. With AUTH_ENABLED=false the service must ignore it.
  result.authorization = `Bearer ${API_KEY ?? 'unused-token'}`;
  return result;
}

async function post(path: string, body: unknown): Promise<{ status: number; json: any }> {
  const response = await fetch(`${BASE_URL}${path}`, {
    method: 'POST',
    headers: headers(),
    body: typeof body === 'string' ? body : JSON.stringify(body),
  });
  const text = await response.text();
  return { status: response.status, json: text === '' ? null : JSON.parse(text) };
}

async function get(path: string): Promise<{ status: number; json: any }> {
  const response = await fetch(`${BASE_URL}${path}`, { headers: headers() });
  const text = await response.text();
  return { status: response.status, json: text === '' ? null : JSON.parse(text) };
}

before(async () => {
  const deadline = Date.now() + 60_000;
  for (;;) {
    try {
      const response = await fetch(`${BASE_URL}/health`);
      if (response.status === 200) return;
    } catch {
      // not up yet
    }
    if (Date.now() > deadline) throw new Error(`service never became healthy at ${BASE_URL}`);
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
});

describe('GET /health', () => {
  test('reports ready without credentials', async () => {
    // Explicitly unauthenticated: no Authorization header at all.
    const response = await fetch(`${BASE_URL}/health`);
    assert.equal(response.status, 200);
  });
});

describe('POST /logs', () => {
  test('stores a batch and returns it through the query API', async () => {
    const service = `${RUN_ID}-roundtrip`;
    const timestamp = new Date().toISOString();

    const ingest = await post('/logs', {
      logs: [
        {
          timestamp,
          level: 'error',
          service,
          message: 'payment declined for order 991',
          attributes: { user_id: '42', region: 'eu-west', retries: 3, cached: false },
        },
      ],
    });

    assert.equal(ingest.status, 200);
    assert.equal(ingest.json.accepted, 1);
    assert.deepEqual(ingest.json.rejected, []);

    const query = await get(`/logs?service=${encodeURIComponent(service)}`);
    assert.equal(query.status, 200);
    assert.equal(query.json.logs.length, 1);

    const row = query.json.logs[0];
    assert.equal(row.level, 'error');
    assert.equal(row.service, service);
    assert.equal(row.message, 'payment declined for order 991');
    // Attribute types survive the jsonb round trip.
    assert.deepEqual(row.attributes, {
      user_id: '42',
      region: 'eu-west',
      retries: 3,
      cached: false,
    });
    assert.equal(row.timestamp, timestamp);
    assert.equal(typeof row.id, 'string');
  });

  test('accepts valid entries while rejecting invalid ones in the same batch', async () => {
    const service = `${RUN_ID}-partial`;
    const timestamp = new Date().toISOString();

    const response = await post('/logs', {
      logs: [
        { timestamp, level: 'info', service, message: 'ok one' },
        { timestamp, level: 'nonsense', service, message: 'bad level' },
        { timestamp, level: 'warn', service, message: 'ok two' },
        { timestamp: 'not-a-date', level: 'info', service, message: 'bad timestamp' },
      ],
    });

    assert.equal(response.status, 200);
    assert.equal(response.json.accepted, 2);
    assert.equal(response.json.rejected.length, 2);
    assert.deepEqual(
      response.json.rejected.map((entry: { index: number }) => entry.index),
      [1, 3],
    );
    assert.match(response.json.rejected[0].reason, /invalid level/);
  });

  test('returns 400 when every entry is rejected', async () => {
    const response = await post('/logs', {
      logs: [{ timestamp: 'bad', level: 'info', service: 'x', message: 'y' }],
    });
    assert.equal(response.status, 400);
    assert.equal(response.json.accepted, 0);
    assert.equal(response.json.rejected.length, 1);
  });

  test('returns 400 for malformed JSON and wrong top-level shapes', async () => {
    assert.equal((await post('/logs', '{"logs":[')).status, 400);
    assert.equal((await post('/logs', { entries: [] })).status, 400);
    assert.equal((await post('/logs', { logs: {} })).status, 400);
    assert.equal((await post('/logs', [])).status, 400);
  });

  test('accepts a batch of one', async () => {
    const response = await post('/logs', {
      logs: [
        {
          timestamp: new Date().toISOString(),
          level: 'debug',
          service: `${RUN_ID}-single`,
          message: 'single entry batch',
        },
      ],
    });
    assert.equal(response.status, 200);
    assert.equal(response.json.accepted, 1);
  });

  test('rejects a timestamp more than five minutes in the future', async () => {
    const response = await post('/logs', {
      logs: [
        {
          timestamp: new Date(Date.now() + 10 * 60_000).toISOString(),
          level: 'info',
          service: `${RUN_ID}-future`,
          message: 'from the future',
        },
      ],
    });
    assert.equal(response.status, 400);
    assert.match(response.json.rejected[0].reason, /five minutes in the future/);
  });
});

describe('GET /logs pagination', () => {
  /**
   * The hard case for cursor pagination: every row shares one timestamp, so
   * ordering can only be made deterministic by the id tiebreaker. A cursor
   * carrying the timestamp alone would drop or repeat rows at page boundaries.
   */
  test('paginates a set of identically-timestamped rows exactly once each', async () => {
    const service = `${RUN_ID}-paging`;
    const timestamp = new Date().toISOString();
    const total = 55;

    const ingest = await post('/logs', {
      logs: Array.from({ length: total }, (_, index) => ({
        timestamp,
        level: 'info',
        service,
        message: `page entry ${index}`,
        attributes: { seq: index },
      })),
    });
    assert.equal(ingest.json.accepted, total);

    const seen: string[] = [];
    let cursor: string | null = null;
    let pages = 0;

    do {
      const suffix: string = cursor === null ? '' : `&cursor=${encodeURIComponent(cursor)}`;
      const page: { status: number; json: any } = await get(
        `/logs?service=${encodeURIComponent(service)}&limit=10${suffix}`,
      );
      assert.equal(page.status, 200);

      for (const row of page.json.logs) seen.push(row.id);
      cursor = page.json.next_cursor;
      pages += 1;
      assert.ok(pages < 20, 'pagination did not terminate');
    } while (cursor !== null);

    assert.equal(seen.length, total, 'every row returned exactly once');
    assert.equal(new Set(seen).size, total, 'no duplicates across pages');
  });

  test('next_cursor is null on the final page', async () => {
    const service = `${RUN_ID}-lastpage`;
    await post('/logs', {
      logs: [
        {
          timestamp: new Date().toISOString(),
          level: 'info',
          service,
          message: 'only row',
        },
      ],
    });

    const page = await get(`/logs?service=${encodeURIComponent(service)}&limit=100`);
    assert.equal(page.json.logs.length, 1);
    assert.equal(page.json.next_cursor, null);
  });

  test('results are ordered by timestamp descending', async () => {
    const service = `${RUN_ID}-ordering`;
    const base = Date.now() - 60_000;

    await post('/logs', {
      logs: Array.from({ length: 10 }, (_, index) => ({
        timestamp: new Date(base + index * 1000).toISOString(),
        level: 'info',
        service,
        message: `ordered ${index}`,
      })),
    });

    const page = await get(`/logs?service=${encodeURIComponent(service)}&limit=100`);
    const timestamps = page.json.logs.map((row: { timestamp: string }) => row.timestamp);
    const descending = [...timestamps].sort().reverse();
    assert.deepEqual(timestamps, descending);
  });

  test('rejects a malformed cursor with 400', async () => {
    const response = await get('/logs?cursor=%21%21%21not-valid');
    assert.equal(response.status, 400);
    assert.match(response.json.error, /cursor/);
  });
});

describe('GET /logs filters', () => {
  const service = `${RUN_ID}-filters`;
  const timestamp = new Date().toISOString();

  before(async () => {
    await post('/logs', {
      logs: [
        {
          timestamp,
          level: 'error',
          service,
          message: 'connection refused by upstream',
          attributes: { region: 'eu-west', retries: 5, tenant: 'acme' },
        },
        {
          timestamp,
          level: 'info',
          service,
          message: 'request completed',
          attributes: { region: 'us-east', retries: 0, tenant: 'acme' },
        },
        {
          timestamp,
          level: 'warn',
          service,
          message: 'CONNECTION pool nearly exhausted',
          attributes: { region: 'eu-west', retries: 1, tenant: 'globex' },
        },
      ],
    });
  });

  test('filters by level', async () => {
    const response = await get(`/logs?service=${encodeURIComponent(service)}&level=error`);
    assert.equal(response.json.logs.length, 1);
    assert.equal(response.json.logs[0].level, 'error');
  });

  test('matches attributes as strings regardless of stored JSON type', async () => {
    // Stored as the number 5, queried as the string "5".
    const response = await get(`/logs?service=${encodeURIComponent(service)}&attr.retries=5`);
    assert.equal(response.json.logs.length, 1);
    assert.equal(response.json.logs[0].attributes.retries, 5);
  });

  test('combines multiple attribute filters conjunctively', async () => {
    const response = await get(
      `/logs?service=${encodeURIComponent(service)}&attr.region=eu-west&attr.tenant=acme`,
    );
    assert.equal(response.json.logs.length, 1);
    assert.match(response.json.logs[0].message, /connection refused/);
  });

  test('q is a case-insensitive substring match on message', async () => {
    const response = await get(`/logs?service=${encodeURIComponent(service)}&q=connection`);
    assert.equal(response.json.logs.length, 2);
  });

  test('q treats LIKE metacharacters literally', async () => {
    // '%' must not behave as a wildcard, so this matches nothing.
    const response = await get(`/logs?service=${encodeURIComponent(service)}&q=%25`);
    assert.equal(response.json.logs.length, 0);
  });

  test('combines every filter dimension at once', async () => {
    const since = new Date(Date.parse(timestamp) - 60_000).toISOString();
    const until = new Date(Date.parse(timestamp) + 60_000).toISOString();
    const response = await get(
      `/logs?service=${encodeURIComponent(service)}&level=error&since=${since}&until=${until}` +
        `&q=refused&attr.region=eu-west&limit=10`,
    );
    assert.equal(response.status, 200);
    assert.equal(response.json.logs.length, 1);
  });

  test('until is exclusive and since is inclusive', async () => {
    const exact = timestamp;
    const inclusive = await get(
      `/logs?service=${encodeURIComponent(service)}&since=${encodeURIComponent(exact)}`,
    );
    assert.ok(inclusive.json.logs.length >= 3, 'since includes rows at the boundary');

    const exclusive = await get(
      `/logs?service=${encodeURIComponent(service)}&until=${encodeURIComponent(exact)}`,
    );
    assert.equal(exclusive.json.logs.length, 0, 'until excludes rows at the boundary');
  });

  test('an unknown service returns an empty result rather than an error', async () => {
    const response = await get('/logs?service=this-service-has-never-logged');
    assert.equal(response.status, 200);
    assert.deepEqual(response.json.logs, []);
    assert.equal(response.json.next_cursor, null);
  });
});

describe('GET /logs/aggregate', () => {
  const service = `${RUN_ID}-agg`;
  // Aligned to a minute boundary so bucket membership is unambiguous.
  const base = Math.floor((Date.now() - 3_600_000) / 60_000) * 60_000;

  before(async () => {
    const logs: unknown[] = [];
    // 5 entries in minute 0, 3 in minute 1, 2 in minute 2.
    const layout = [5, 3, 2];
    layout.forEach((count, minute) => {
      for (let i = 0; i < count; i++) {
        logs.push({
          timestamp: new Date(base + minute * 60_000 + i * 1000).toISOString(),
          level: i % 2 === 0 ? 'info' : 'error',
          service,
          message: `agg entry m${minute} i${i}`,
        });
      }
    });
    const response = await post('/logs', { logs });
    assert.equal(response.json.accepted, 10);
  });

  test('counts rows into the correct one-minute buckets', async () => {
    const since = new Date(base).toISOString();
    const until = new Date(base + 3 * 60_000).toISOString();
    const response = await get(
      `/logs/aggregate?since=${since}&until=${until}&bucket=1m&service=${encodeURIComponent(service)}`,
    );

    assert.equal(response.status, 200);
    assert.deepEqual(
      response.json.buckets.map((bucket: { count: number }) => bucket.count),
      [5, 3, 2],
    );
    // group is null when group_by is absent.
    for (const bucket of response.json.buckets) assert.equal(bucket.group, null);
  });

  test('bucket starts are ascending and aligned to the bucket size', async () => {
    const since = new Date(base).toISOString();
    const until = new Date(base + 3 * 60_000).toISOString();
    const response = await get(
      `/logs/aggregate?since=${since}&until=${until}&bucket=1m&service=${encodeURIComponent(service)}`,
    );

    const starts = response.json.buckets.map((bucket: { start: string }) => bucket.start);
    assert.deepEqual(starts, [...starts].sort());
    for (const start of starts) {
      assert.equal(Date.parse(start) % 60_000, 0, `${start} is aligned to a minute`);
    }
  });

  test('groups by level', async () => {
    const since = new Date(base).toISOString();
    const until = new Date(base + 3 * 60_000).toISOString();
    const response = await get(
      `/logs/aggregate?since=${since}&until=${until}&bucket=1h&group_by=level` +
        `&service=${encodeURIComponent(service)}`,
    );

    const byLevel = Object.fromEntries(
      response.json.buckets.map((bucket: { group: string; count: number }) => [
        bucket.group,
        bucket.count,
      ]),
    );
    assert.equal(byLevel.info + byLevel.error, 10);
  });

  test('groups by service', async () => {
    const since = new Date(base).toISOString();
    const until = new Date(base + 3 * 60_000).toISOString();
    const response = await get(
      `/logs/aggregate?since=${since}&until=${until}&bucket=1h&group_by=service` +
        `&service=${encodeURIComponent(service)}`,
    );
    assert.equal(response.json.buckets.length, 1);
    assert.equal(response.json.buckets[0].group, service);
    assert.equal(response.json.buckets[0].count, 10);
  });

  test('an empty range returns an empty bucket list', async () => {
    const response = await get(
      '/logs/aggregate?since=1990-01-01T00:00:00Z&until=1990-01-02T00:00:00Z&bucket=1h',
    );
    assert.equal(response.status, 200);
    assert.deepEqual(response.json.buckets, []);
  });

  test('requires since, until and bucket', async () => {
    const since = new Date(base).toISOString();
    const until = new Date(base + 60_000).toISOString();
    assert.equal((await get(`/logs/aggregate?since=${since}&until=${until}`)).status, 400);
    assert.equal((await get(`/logs/aggregate?until=${until}&bucket=1m`)).status, 400);
    assert.equal((await get(`/logs/aggregate?since=${since}&bucket=1m`)).status, 400);
    assert.equal(
      (await get(`/logs/aggregate?since=${since}&until=${until}&bucket=90s`)).status,
      400,
    );
  });
});

describe('retention', () => {
  test('reports configuration and runs a sweep on demand', async () => {
    const status = await get('/admin/retention');
    assert.equal(status.status, 200);
    assert.equal(status.json.enabled, true);
    assert.equal(typeof status.json.retention_days, 'number');

    const sweep = await post('/admin/retention/run', {});
    assert.equal(sweep.status, 200);
    assert.ok(Array.isArray(sweep.json.dropped));
    assert.match(sweep.json.cutoff, /^\d{4}-\d{2}-\d{2}$/);
  });
});

describe('routing', () => {
  test('unknown routes return 404 and wrong methods return 405', async () => {
    assert.equal((await get('/nope')).status, 404);
    const response = await fetch(`${BASE_URL}/logs/aggregate`, {
      method: 'DELETE',
      headers: headers(),
    });
    assert.equal(response.status, 405);
  });

  test('trailing slashes resolve to the same route', async () => {
    assert.equal((await get('/logs/?limit=1')).status, 200);
  });
});
