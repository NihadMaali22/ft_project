import type { LoadClient } from './client.ts';
import { Samples } from './stats.ts';

/**
 * Issues read queries at a steady rate while ingestion runs.
 *
 * The target is one aggregation per second sustained during the ingestion test,
 * with p95 under a second. Running the probe concurrently with the write load
 * is the only way to measure that honestly - query latency measured on an idle
 * system says nothing about behaviour under concurrent load.
 */

export interface ProbeDefinition {
  name: string;
  /** Built fresh per call so the time range slides with the clock. */
  path: () => string;
  /** Firing interval. The contract asks for one aggregation per second. */
  intervalMs: number;
  /**
   * Ceiling on outstanding requests for this probe.
   *
   * Without it, a probe that fires on a fixed timer while the service is slow
   * accumulates an unbounded backlog, and the queue it creates - not the
   * service - becomes what the latency numbers measure. An early run of this
   * harness fired four queries a second with no cap and reported an 8 second
   * p50 for a query that takes well under a second, purely from self-inflicted
   * queueing. Real dashboards do not stack requests this way.
   */
  maxOutstanding: number;
}

export interface ProbeResult {
  name: string;
  intervalMs: number;
  requests: number;
  /** Firings suppressed because the previous requests had not yet returned. */
  skipped: number;
  errors: number;
  latency: Record<string, number>;
  statuses: Record<string, number>;
  lastBody: string;
}

/**
 * The dashboard-style query treated as the primary aggregation target, fired at
 * exactly the one request per second the contract calls for.
 */
export const PRIMARY_AGGREGATE: ProbeDefinition = {
  name: 'aggregate_1h_1m_by_service',
  path: () => {
    const until = new Date();
    const since = new Date(until.getTime() - 60 * 60 * 1000);
    return `/logs/aggregate?since=${since.toISOString()}&until=${until.toISOString()}&bucket=1m&group_by=service`;
  },
  intervalMs: 1000,
  // One at a time, which is what a dashboard polling once a second actually
  // does: it does not launch a second copy of a query whose first copy has not
  // returned. Allowing overlap multiplies the query load precisely when the
  // system is slowest, so the measurement stops describing a real client.
  maxOutstanding: 1,
};

export const PROBES: ProbeDefinition[] = [
  PRIMARY_AGGREGATE,
  {
    // A wide-window aggregation, sampled rather than sustained: it exists to
    // show the endpoint stays correct and bounded, not to add a second query
    // per second on top of the one the contract specifies.
    name: 'aggregate_24h_1h_by_level',
    path: () => {
      const until = new Date();
      const since = new Date(until.getTime() - 24 * 60 * 60 * 1000);
      return `/logs/aggregate?since=${since.toISOString()}&until=${until.toISOString()}&bucket=1h&group_by=level`;
    },
    intervalMs: 10_000,
    maxOutstanding: 1,
  },
  {
    name: 'logs_recent_page',
    path: () => '/logs?limit=100',
    intervalMs: 1000,
    maxOutstanding: 2,
  },
  {
    name: 'logs_filtered',
    path: () => '/logs?service=checkout&level=error&limit=100',
    intervalMs: 5000,
    maxOutstanding: 1,
  },
];

export class QueryProbe {
  private readonly latency = new Map<string, Samples>();
  private readonly statuses = new Map<string, Map<number, number>>();
  private readonly requests = new Map<string, number>();
  private readonly errors = new Map<string, number>();
  private readonly skipped = new Map<string, number>();
  private readonly outstanding = new Map<string, number>();
  private readonly lastBody = new Map<string, string>();
  private timers: NodeJS.Timeout[] = [];

  constructor(
    private readonly client: LoadClient,
    private readonly probes: ProbeDefinition[] = PROBES,
  ) {
    for (const probe of probes) {
      this.latency.set(probe.name, new Samples());
      this.statuses.set(probe.name, new Map());
      this.requests.set(probe.name, 0);
      this.errors.set(probe.name, 0);
      this.skipped.set(probe.name, 0);
      this.outstanding.set(probe.name, 0);
    }
  }

  /** Runs each probe on its own declared interval. */
  start(): void {
    for (const probe of this.probes) {
      const timer = setInterval(() => {
        void this.fire(probe);
      }, probe.intervalMs);
      this.timers.push(timer);
    }
  }

  private async fire(probe: ProbeDefinition): Promise<void> {
    const inFlight = this.outstanding.get(probe.name) ?? 0;
    if (inFlight >= probe.maxOutstanding) {
      // Recorded rather than silently dropped: a rising skip count is itself a
      // finding about query latency under load.
      this.skipped.set(probe.name, (this.skipped.get(probe.name) ?? 0) + 1);
      return;
    }

    this.outstanding.set(probe.name, inFlight + 1);
    this.requests.set(probe.name, (this.requests.get(probe.name) ?? 0) + 1);

    const response = await this.client.get(probe.path()).finally(() => {
      this.outstanding.set(probe.name, (this.outstanding.get(probe.name) ?? 1) - 1);
    });

    this.latency.get(probe.name)?.record(response.latencyMs);

    const statuses = this.statuses.get(probe.name) as Map<number, number>;
    statuses.set(response.status, (statuses.get(response.status) ?? 0) + 1);

    if (response.status !== 200 || response.error !== null) {
      this.errors.set(probe.name, (this.errors.get(probe.name) ?? 0) + 1);
    }
    this.lastBody.set(probe.name, response.body.slice(0, 400));
  }

  stop(): ProbeResult[] {
    for (const timer of this.timers) clearInterval(timer);
    this.timers = [];

    return this.probes.map((probe) => ({
      name: probe.name,
      intervalMs: probe.intervalMs,
      requests: this.requests.get(probe.name) ?? 0,
      skipped: this.skipped.get(probe.name) ?? 0,
      errors: this.errors.get(probe.name) ?? 0,
      latency: this.latency.get(probe.name)?.percentiles() ?? {},
      statuses: Object.fromEntries(
        [...(this.statuses.get(probe.name) ?? new Map())].map(([status, count]) => [
          String(status),
          count,
        ]),
      ),
      lastBody: this.lastBody.get(probe.name) ?? '',
    }));
  }
}
