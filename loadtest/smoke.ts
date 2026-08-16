/**
 * Required-contract smoke test.
 *
 * Verifies the four required endpoints in whichever configuration the service
 * is running, and is the check CI runs in both auth modes. Its job is to catch
 * the failure that matters most: a change that leaves the service working for
 * its own tests but unreachable for the load generator.
 *
 * Environment:
 *   TARGET_HOST / TARGET_PORT  where to probe (default 127.0.0.1:8080)
 *   EXPECT_AUTH=true           also assert that credentials are enforced
 *   LOADGEN_API_KEY            the seeded key, required when EXPECT_AUTH=true
 */

const HOST = process.env.TARGET_HOST ?? '127.0.0.1';
const PORT = Number(process.env.TARGET_PORT ?? 8080);
const BASE = `http://${HOST}:${PORT}`;
const EXPECT_AUTH = process.env.EXPECT_AUTH === 'true';
const API_KEY = process.env.LOADGEN_API_KEY ?? '';

let failures = 0;

function check(name: string, condition: boolean, detail = ''): void {
  if (condition) {
    console.log(`  PASS  ${name}`);
  } else {
    failures += 1;
    console.error(`  FAIL  ${name}${detail === '' ? '' : ` -- ${detail}`}`);
  }
}

/** The load generator always sends a bearer token, in both configurations. */
function authHeaders(): Record<string, string> {
  return {
    'content-type': 'application/json',
    authorization: `Bearer ${API_KEY === '' ? 'unrecognised-token' : API_KEY}`,
  };
}

async function waitForHealth(timeoutMs = 120_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    try {
      // Deliberately unauthenticated: /health must never require credentials.
      const response = await fetch(`${BASE}/health`);
      if (response.status === 200) return;
    } catch {
      // not up yet
    }
    if (Date.now() > deadline) throw new Error(`service never became healthy at ${BASE}`);
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
}

async function main(): Promise<void> {
  console.log(`Smoke test against ${BASE} (EXPECT_AUTH=${EXPECT_AUTH})`);

  await waitForHealth();
  check('GET /health returns 200 without credentials', true);

  const now = new Date().toISOString();
  const service = `smoke-${Date.now()}`;

  // --- POST /logs ---
  const ingest = await fetch(`${BASE}/logs`, {
    method: 'POST',
    headers: authHeaders(),
    body: JSON.stringify({
      logs: [
        { timestamp: now, level: 'error', service, message: 'smoke test entry', attributes: { k: 'v' } },
        { timestamp: now, level: 'bogus', service, message: 'invalid entry' },
      ],
    }),
  });
  const ingestBody = (await ingest.json()) as { accepted: number; rejected: unknown[] };

  check('POST /logs returns 200', ingest.status === 200, `got ${ingest.status}`);
  check('POST /logs accepts the valid entry', ingestBody.accepted === 1, JSON.stringify(ingestBody));
  check(
    'POST /logs reports the invalid entry',
    Array.isArray(ingestBody.rejected) && ingestBody.rejected.length === 1,
    JSON.stringify(ingestBody),
  );

  // --- GET /logs ---
  const query = await fetch(`${BASE}/logs?service=${encodeURIComponent(service)}&limit=10`, {
    headers: authHeaders(),
  });
  const queryBody = (await query.json()) as { logs: unknown[]; next_cursor: string | null };

  check('GET /logs returns 200', query.status === 200, `got ${query.status}`);
  check('GET /logs returns the ingested row', queryBody.logs?.length === 1);
  check('GET /logs includes next_cursor', 'next_cursor' in queryBody);

  // --- GET /logs/aggregate ---
  const until = new Date(Date.now() + 60_000).toISOString();
  const since = new Date(Date.now() - 3_600_000).toISOString();
  const aggregate = await fetch(
    `${BASE}/logs/aggregate?since=${since}&until=${until}&bucket=1m&group_by=service`,
    { headers: authHeaders() },
  );
  const aggregateBody = (await aggregate.json()) as { buckets: unknown[] };

  check('GET /logs/aggregate returns 200', aggregate.status === 200, `got ${aggregate.status}`);
  check('GET /logs/aggregate returns buckets', Array.isArray(aggregateBody.buckets));

  // --- validation still rejects bad input ---
  const badParams = await fetch(`${BASE}/logs?level=nonsense`, { headers: authHeaders() });
  check('GET /logs rejects an invalid level with 400', badParams.status === 400);

  // --- authentication posture ---
  if (EXPECT_AUTH) {
    if (API_KEY === '') throw new Error('EXPECT_AUTH=true requires LOADGEN_API_KEY');

    const noCredentials = await Promise.all([
      fetch(`${BASE}/logs`),
      fetch(`${BASE}/logs/aggregate?since=${since}&until=${until}&bucket=1m`),
      fetch(`${BASE}/logs`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ logs: [] }),
      }),
    ]);

    check(
      'all three data endpoints return 401 without credentials',
      noCredentials.every((response) => response.status === 401),
      noCredentials.map((response) => response.status).join(','),
    );

    const health = await fetch(`${BASE}/health`);
    check('GET /health stays unauthenticated when auth is on', health.status === 200);
  } else {
    // The golden rule: with auth disabled, an unrecognised Authorization header
    // must be ignored rather than rejected.
    const strangeToken = await fetch(`${BASE}/logs`, {
      headers: { authorization: 'Bearer completely-unknown-token' },
    });
    check(
      'unrecognised bearer token is ignored when auth is disabled',
      strangeToken.status === 200,
      `got ${strangeToken.status}`,
    );

    const noCredentials = await fetch(`${BASE}/logs`);
    check('endpoints are reachable with no credentials at all', noCredentials.status === 200);
  }

  console.log(failures === 0 ? '\nSmoke test passed.' : `\nSmoke test FAILED (${failures}).`);
  process.exit(failures === 0 ? 0 : 1);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
