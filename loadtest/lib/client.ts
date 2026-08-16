import http from 'node:http';
import { Samples } from './stats.ts';
import type { PayloadPool } from './payload.ts';

/**
 * Open-loop load driver.
 *
 * Requests are fired on a fixed schedule derived from the target rate, rather
 * than waiting for the previous response before sending the next. A closed-loop
 * driver would silently reduce its own offered load whenever the service slowed
 * down, reporting healthy latencies for a system that is actually failing to
 * keep up.
 *
 * Latency is measured from the time each request was *scheduled* to be sent,
 * not from when it actually went out. That is the standard correction for
 * coordinated omission: if the driver falls behind, the delay it experienced is
 * attributed to the system, which is where a real client would feel it.
 */

export interface RequestOutcome {
  status: number;
  accepted: number;
  rejected: number;
  /** Scheduled-time latency, i.e. what a client on a fixed cadence observes. */
  latencyMs: number;
  /** Wire latency from actual send to response, for comparison. */
  serviceLatencyMs: number;
  error: string | null;
}

export interface PhaseCollector {
  outcomes: {
    sent: number;
    completed: number;
    accepted: number;
    rejected: number;
    shed: number;
    failed: number;
    byStatus: Map<number, number>;
    errors: Map<string, number>;
  };
  latency: Samples;
  serviceLatency: Samples;
}

export function createCollector(): PhaseCollector {
  return {
    outcomes: {
      sent: 0,
      completed: 0,
      accepted: 0,
      rejected: 0,
      shed: 0,
      failed: 0,
      byStatus: new Map(),
      errors: new Map(),
    },
    latency: new Samples(),
    serviceLatency: new Samples(),
  };
}

function increment<K>(map: Map<K, number>, key: K): void {
  map.set(key, (map.get(key) ?? 0) + 1);
}

export interface ClientOptions {
  host: string;
  port: number;
  apiKey?: string | undefined;
  maxSockets?: number;
}

export class LoadClient {
  private readonly agent: http.Agent;
  private readonly headers: Record<string, string>;

  constructor(private readonly options: ClientOptions) {
    this.agent = new http.Agent({
      keepAlive: true,
      keepAliveMsecs: 30_000,
      maxSockets: options.maxSockets ?? 128,
      maxFreeSockets: options.maxSockets ?? 128,
      scheduling: 'fifo',
    });

    this.headers = { 'content-type': 'application/json' };
    // The load generator always sends a bearer token; with AUTH_ENABLED=false
    // the service must ignore it rather than reject the request.
    if (options.apiKey !== undefined) {
      this.headers.authorization = `Bearer ${options.apiKey}`;
    }
  }

  /** POSTs one pre-serialised batch. Never rejects; failures are reported. */
  postLogs(body: Buffer, scheduledAt: number): Promise<RequestOutcome> {
    return new Promise((resolve) => {
      const sentAt = performance.now();

      const request = http.request(
        {
          host: this.options.host,
          port: this.options.port,
          path: '/logs',
          method: 'POST',
          agent: this.agent,
          headers: { ...this.headers, 'content-length': body.length },
        },
        (response) => {
          const chunks: Buffer[] = [];
          response.on('data', (chunk: Buffer) => chunks.push(chunk));
          response.on('end', () => {
            const finishedAt = performance.now();
            let accepted = 0;
            let rejected = 0;

            try {
              const parsed = JSON.parse(Buffer.concat(chunks).toString('utf8')) as {
                accepted?: number;
                rejected?: unknown[];
              };
              accepted = parsed.accepted ?? 0;
              rejected = Array.isArray(parsed.rejected) ? parsed.rejected.length : 0;
            } catch {
              // Non-JSON body; counted purely by status code.
            }

            resolve({
              status: response.statusCode ?? 0,
              accepted,
              rejected,
              latencyMs: finishedAt - scheduledAt,
              serviceLatencyMs: finishedAt - sentAt,
              error: null,
            });
          });
        },
      );

      request.on('error', (error) => {
        const finishedAt = performance.now();
        resolve({
          status: 0,
          accepted: 0,
          rejected: 0,
          latencyMs: finishedAt - scheduledAt,
          serviceLatencyMs: finishedAt - sentAt,
          error: (error as NodeJS.ErrnoException).code ?? error.message,
        });
      });

      request.end(body);
    });
  }

  /** Issues an arbitrary GET and returns status plus latency. */
  get(path: string): Promise<{ status: number; latencyMs: number; body: string; error: string | null }> {
    return new Promise((resolve) => {
      const startedAt = performance.now();
      const request = http.request(
        {
          host: this.options.host,
          port: this.options.port,
          path,
          method: 'GET',
          agent: this.agent,
          headers: this.headers,
        },
        (response) => {
          const chunks: Buffer[] = [];
          response.on('data', (chunk: Buffer) => chunks.push(chunk));
          response.on('end', () =>
            resolve({
              status: response.statusCode ?? 0,
              latencyMs: performance.now() - startedAt,
              body: Buffer.concat(chunks).toString('utf8'),
              error: null,
            }),
          );
        },
      );
      request.on('error', (error) =>
        resolve({
          status: 0,
          latencyMs: performance.now() - startedAt,
          body: '',
          error: error.message,
        }),
      );
      request.end();
    });
  }

  destroy(): void {
    this.agent.destroy();
  }
}

/**
 * Drives one constant-rate phase to completion.
 *
 * Resolves once the phase duration has elapsed *and* every in-flight request
 * has settled, so a phase never leaks requests into the next one.
 */
export async function runPhase(
  client: LoadClient,
  pool: PayloadPool,
  targetLogsPerSecond: number,
  durationMs: number,
  collector: PhaseCollector,
): Promise<void> {
  const requestsPerSecond = targetLogsPerSecond / pool.batchSize;
  const intervalMs = 1000 / requestsPerSecond;

  const startedAt = performance.now();
  const endsAt = startedAt + durationMs;
  let scheduledAt = startedAt;

  const inFlight = new Set<Promise<void>>();

  // Keeps timestamps in the payload pool close to the current clock.
  const refreshTimer = setInterval(() => pool.refresh(), 5000);

  await new Promise<void>((resolve) => {
    const timer = setInterval(() => {
      const now = performance.now();

      // Fire every request whose scheduled slot has arrived. Batching the
      // catch-up here is what keeps the offered rate accurate despite the
      // ~1 ms floor on Node timer resolution.
      while (scheduledAt <= now && scheduledAt < endsAt) {
        const slot = scheduledAt;
        scheduledAt += intervalMs;

        collector.outcomes.sent += 1;

        const promise = client
          .postLogs(pool.next(), slot)
          .then((outcome) => {
            collector.outcomes.completed += 1;
            collector.outcomes.accepted += outcome.accepted;
            collector.outcomes.rejected += outcome.rejected;
            increment(collector.outcomes.byStatus, outcome.status);
            collector.latency.record(outcome.latencyMs);
            collector.serviceLatency.record(outcome.serviceLatencyMs);

            if (outcome.error !== null) {
              collector.outcomes.failed += 1;
              increment(collector.outcomes.errors, outcome.error);
            } else if (outcome.status === 503 || outcome.status === 429) {
              collector.outcomes.shed += 1;
            } else if (outcome.status >= 500) {
              collector.outcomes.failed += 1;
              increment(collector.outcomes.errors, `HTTP ${outcome.status}`);
            }
          })
          .finally(() => {
            inFlight.delete(promise);
          });

        inFlight.add(promise);
      }

      if (now >= endsAt) {
        clearInterval(timer);
        clearInterval(refreshTimer);
        resolve();
      }
    }, 1);
  });

  await Promise.all([...inFlight]);
}
