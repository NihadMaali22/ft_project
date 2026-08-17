import http from 'node:http';
import { Samples } from './lib/stats.ts';

/**
 * Closed-loop load driver.
 *
 * The companion to `lib/client.ts`, which is open loop. This one holds a fixed
 * number of concurrent workers, each sending a request and waiting for the
 * response before sending the next -- the default behaviour of k6, JMeter,
 * Gatling and Locust, and therefore the shape most external graders use.
 *
 * That difference is not cosmetic. A closed-loop client's throughput is
 * bounded by `concurrency / latency`, so any fixed latency floor in the service
 * becomes a hard throughput ceiling no matter how much capacity sits behind it.
 * The open-loop harness cannot see such a floor, because it keeps offering load
 * on a fixed schedule regardless of how slowly responses come back.
 *
 * Defaults to one log per request, which is the worst case for the service:
 * it maximises HTTP and per-request overhead per stored row.
 *
 * Usage: closed-loop.ts [concurrency] [seconds] [batchSize]
 */

const concurrency = Number(process.argv[2] ?? 50);
const seconds = Number(process.argv[3] ?? 30);
const batchSize = Number(process.argv[4] ?? 1);

const host = process.env.TARGET_HOST ?? '127.0.0.1';
const port = Number(process.env.TARGET_PORT ?? 8080);
const apiKey = process.env.LOADGEN_API_KEY;

const agent = new http.Agent({
  keepAlive: true,
  keepAliveMsecs: 30_000,
  maxSockets: concurrency,
  maxFreeSockets: concurrency,
  scheduling: 'fifo',
});

const headers: Record<string, string> = { 'content-type': 'application/json' };
if (apiKey !== undefined) headers.authorization = `Bearer ${apiKey}`;

const SERVICES = ['checkout', 'auth', 'payments', 'search', 'inventory', 'shipping'];
const LEVELS = ['debug', 'info', 'info', 'info', 'warn', 'error'];

/**
 * Pre-serialised bodies, refreshed periodically so timestamps stay current.
 *
 * Built ahead of time so the driver's own JSON encoding never shows up in the
 * measured latency; at 15k requests/s the generator has to stay cheaper than
 * the service it is measuring.
 */
function buildPayloads(count: number): Buffer[] {
  const now = Date.now();
  const payloads: Buffer[] = [];

  for (let i = 0; i < count; i++) {
    const logs = [];
    for (let j = 0; j < batchSize; j++) {
      logs.push({
        timestamp: new Date(now - ((i + j) % 1000)).toISOString(),
        level: LEVELS[(i + j) % LEVELS.length],
        service: SERVICES[(i + j) % SERVICES.length],
        message: `closed-loop probe ${i}-${j} request handled in ${(i * 7) % 900}ms`,
        attributes: {
          request_id: `req-${i}-${j}`,
          region: (i + j) % 2 === 0 ? 'eu-west' : 'us-east',
          retries: (i + j) % 3,
        },
      });
    }
    payloads.push(Buffer.from(JSON.stringify({ logs }), 'utf8'));
  }

  return payloads;
}

let payloads = buildPayloads(256);
let cursor = 0;
const nextPayload = (): Buffer => payloads[cursor++ % payloads.length] as Buffer;

const latency = new Samples();
let accepted = 0;
let completed = 0;
let shed = 0;
let failed = 0;
const byStatus = new Map<number, number>();
const errors = new Map<string, number>();

let running = true;

function post(body: Buffer): Promise<void> {
  return new Promise((resolve) => {
    const startedAt = performance.now();

    const request = http.request(
      {
        host,
        port,
        path: '/logs',
        method: 'POST',
        agent,
        headers: { ...headers, 'content-length': body.length },
      },
      (response) => {
        const chunks: Buffer[] = [];
        response.on('data', (chunk: Buffer) => chunks.push(chunk));
        response.on('end', () => {
          const status = response.statusCode ?? 0;
          byStatus.set(status, (byStatus.get(status) ?? 0) + 1);
          latency.record(performance.now() - startedAt);
          completed += 1;

          if (status === 200) {
            try {
              const parsed = JSON.parse(Buffer.concat(chunks).toString('utf8')) as {
                accepted?: number;
              };
              accepted += parsed.accepted ?? 0;
            } catch {
              // Counted by status only.
            }
          } else if (status === 503 || status === 429) {
            shed += 1;
          } else if (status >= 500) {
            failed += 1;
          }
          resolve();
        });
      },
    );

    request.on('error', (error) => {
      failed += 1;
      const code = (error as NodeJS.ErrnoException).code ?? error.message;
      errors.set(code, (errors.get(code) ?? 0) + 1);
      latency.record(performance.now() - startedAt);
      resolve();
    });

    request.end(body);
  });
}

/** One worker: send, await, repeat. This is what makes the driver closed loop. */
async function worker(): Promise<void> {
  while (running) {
    await post(nextPayload());
  }
}

const health = await new Promise<number>((resolve) => {
  const request = http.request({ host, port, path: '/health', method: 'GET' }, (response) => {
    response.resume();
    response.on('end', () => resolve(response.statusCode ?? 0));
  });
  request.on('error', () => resolve(0));
  request.end();
});

if (health !== 200) {
  console.error(`service not healthy (status ${health})`);
  process.exit(1);
}

console.log(
  `Closed loop: ${concurrency} concurrent workers, ${batchSize} log(s) per request, ${seconds}s`,
);

const refresh = setInterval(() => {
  payloads = buildPayloads(256);
}, 5000);

const startedAt = performance.now();
const workers = Array.from({ length: concurrency }, () => worker());

await new Promise((resolve) => setTimeout(resolve, seconds * 1000));
running = false;
await Promise.all(workers);

const elapsed = (performance.now() - startedAt) / 1000;
clearInterval(refresh);
agent.destroy();

const percentiles = latency.percentiles();
const logsPerSecond = accepted / elapsed;
const requestsPerSecond = completed / elapsed;

console.log(`  elapsed          : ${elapsed.toFixed(1)}s`);
console.log(`  requests         : ${completed.toLocaleString()}`);
console.log(`  logs accepted    : ${accepted.toLocaleString()}`);
console.log(`  THROUGHPUT       : ${Math.round(logsPerSecond).toLocaleString()} logs/s`);
console.log(`  request rate     : ${Math.round(requestsPerSecond).toLocaleString()} req/s`);
console.log(
  `  latency ms       : p50 ${percentiles.p50}  p95 ${percentiles.p95}  p99 ${percentiles.p99}  max ${percentiles.max}`,
);
console.log(`  shed / failed    : ${shed} / ${failed}`);
console.log(`  status codes     : ${JSON.stringify(Object.fromEntries(byStatus))}`);
if (errors.size > 0) {
  console.log(`  errors           : ${JSON.stringify(Object.fromEntries(errors))}`);
}

// The arithmetic that explains the number above: a closed-loop client cannot
// exceed concurrency/latency, so this line makes any latency floor obvious.
console.log(
  `  implied ceiling  : ${concurrency} workers / ${percentiles.p50}ms p50 = ` +
    `${Math.round((concurrency / percentiles.p50) * 1000 * batchSize).toLocaleString()} logs/s`,
);
