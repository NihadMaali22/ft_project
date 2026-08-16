import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { writeFile, mkdir } from 'node:fs/promises';
import { join } from 'node:path';
import { LoadClient, createCollector, runPhase, type PhaseCollector } from './lib/client.ts';
import { PayloadPool } from './lib/payload.ts';
import { ResourceMonitor, type ContainerSummary } from './lib/monitor.ts';
import { QueryProbe, type ProbeResult } from './lib/probe.ts';
import { round, formatNumber } from './lib/stats.ts';

const execFileAsync = promisify(execFile);

/**
 * Performance test harness.
 *
 * Runs the four required scenarios, recording achieved throughput against
 * target, latency distribution, error and shed rates, container resource usage,
 * and the row counts the database actually holds afterwards.
 *
 * Every number reported here is measured. Accepted counts come from the
 * service's own responses and are then reconciled against a COUNT(*) on the
 * table, so a run that claimed acceptance without storing rows would show up as
 * a mismatch rather than as a good result.
 */

const HOST = process.env.TARGET_HOST ?? '127.0.0.1';
const PORT = Number(process.env.TARGET_PORT ?? 8080);
const BATCH_SIZE = Number(process.env.LOAD_BATCH ?? 500);
const APP_CONTAINER = process.env.APP_CONTAINER ?? 'logsvc-app';
const PG_CONTAINER = process.env.PG_CONTAINER ?? 'logsvc-postgres';
const RESULTS_DIR = process.env.RESULTS_DIR ?? join(process.cwd(), 'results');

/** Seconds allowed for in-flight batches to commit after a phase ends. */
const DRAIN_SECONDS = 10;

interface Phase {
  targetLogsPerSecond: number;
  durationSeconds: number;
}

interface Scenario {
  key: string;
  name: string;
  purpose: string;
  phases: Phase[];
}

const SCENARIOS: Scenario[] = [
  {
    key: 'load',
    name: 'Load Test',
    purpose: 'Compare achieved performance against the target metrics.',
    phases: [{ targetLogsPerSecond: 15_000, durationSeconds: 120 }],
  },
  {
    key: 'stress',
    name: 'Stress Test',
    purpose: 'Verify stability and performance as load progressively increases.',
    phases: [
      { targetLogsPerSecond: 15_000, durationSeconds: 30 },
      { targetLogsPerSecond: 22_500, durationSeconds: 60 },
      { targetLogsPerSecond: 30_000, durationSeconds: 60 },
    ],
  },
  {
    key: 'spike',
    name: 'Spike Test',
    purpose: 'Verify the system absorbs a sudden spike and recovers afterwards.',
    phases: [
      { targetLogsPerSecond: 7_500, durationSeconds: 30 },
      { targetLogsPerSecond: 30_000, durationSeconds: 10 },
      { targetLogsPerSecond: 7_500, durationSeconds: 60 },
    ],
  },
  {
    key: 'breakpoint',
    name: 'Breakpoint Test',
    purpose: 'Identify the breaking point and the maximum sustainable throughput.',
    phases: [
      { targetLogsPerSecond: 15_000, durationSeconds: 30 },
      { targetLogsPerSecond: 22_500, durationSeconds: 30 },
      { targetLogsPerSecond: 30_000, durationSeconds: 30 },
      { targetLogsPerSecond: 45_000, durationSeconds: 30 },
    ],
  },
];

async function countRows(): Promise<number> {
  try {
    const { stdout } = await execFileAsync('docker', [
      'exec',
      PG_CONTAINER,
      'psql',
      '-U',
      'logs',
      '-d',
      'logs',
      '-tAc',
      'SELECT count(*) FROM logs',
    ]);
    return Number(stdout.trim());
  } catch {
    return -1;
  }
}

async function fetchServiceMetrics(client: LoadClient): Promise<unknown> {
  const response = await client.get('/metrics');
  if (response.status !== 200) return null;
  try {
    return JSON.parse(response.body);
  } catch {
    return null;
  }
}

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Waits for PostgreSQL to go quiet before measuring.
 *
 * A bulk load leaves autovacuum working through every partition it touched and
 * a checkpoint flushing the dirty buffers behind it. Both compete for the
 * single available core, and benchmarking through that measures the recovery,
 * not the steady state - an early run of this harness reported 6,000 logs/s for
 * exactly that reason, against 15,000 once the system had settled.
 *
 * The wait is bounded: if the database never goes quiet, that is itself the
 * result and the run proceeds so the number gets reported rather than hidden.
 */
async function waitForQuiescence(maxWaitMs = 180_000): Promise<number> {
  const startedAt = performance.now();
  const deadline = startedAt + maxWaitMs;
  let consecutiveQuiet = 0;

  while (performance.now() < deadline) {
    try {
      const { stdout } = await execFileAsync('docker', [
        'exec',
        PG_CONTAINER,
        'psql',
        '-U',
        'logs',
        '-d',
        'logs',
        '-tAc',
        `SELECT (SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'autovacuum worker')
              + (SELECT count(*) FROM pg_stat_activity
                 WHERE backend_type = 'client backend' AND state = 'active'
                   AND query NOT LIKE '%pg_stat_activity%')`,
      ]);

      // Three consecutive quiet samples, so a momentary gap does not count.
      if (Number(stdout.trim()) === 0) {
        consecutiveQuiet += 1;
        if (consecutiveQuiet >= 3) break;
      } else {
        consecutiveQuiet = 0;
      }
    } catch {
      break;
    }
    await sleep(2000);
  }

  // A checkpoint can still be draining even with no visible backend activity.
  await execFileAsync('docker', [
    'exec',
    PG_CONTAINER,
    'psql',
    '-U',
    'logs',
    '-d',
    'logs',
    '-tAc',
    'CHECKPOINT',
  ]).catch(() => undefined);

  return round((performance.now() - startedAt) / 1000);
}

/**
 * Measures how long newly ingested data takes to become queryable.
 *
 * Posts an entry under a service name unique to this probe, then polls
 * GET /logs until it comes back. Directly tests the 20 second freshness
 * requirement rather than inferring it from the design.
 *
 * The poll filters by `service`, not by `q`. A substring match on `message` is
 * an unindexed scan of every row in range, so polling with it measures how long
 * that scan takes rather than how long the row took to become visible - an
 * earlier version of this probe did exactly that and reported 14 seconds for a
 * row that was already queryable.
 */
async function measureFreshness(client: LoadClient): Promise<number | null> {
  const marker = `freshness-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  const body = Buffer.from(
    JSON.stringify({
      logs: [
        {
          timestamp: new Date().toISOString(),
          level: 'info',
          service: marker,
          message: 'freshness probe',
        },
      ],
    }),
    'utf8',
  );

  const startedAt = performance.now();
  const post = await client.postLogs(body, startedAt);
  if (post.status !== 200) return null;

  const deadline = startedAt + 25_000;
  for (;;) {
    const response = await client.get(`/logs?service=${encodeURIComponent(marker)}&limit=1`);
    if (response.status === 200 && response.body.includes(marker)) {
      return round(performance.now() - startedAt);
    }
    if (performance.now() > deadline) return null;
    await sleep(50);
  }
}

interface PhaseReport {
  target_logs_per_second: number;
  duration_seconds: number;
  requests_sent: number;
  requests_completed: number;
  logs_accepted: number;
  logs_rejected_validation: number;
  logs_shed: number;
  requests_shed: number;
  requests_failed: number;
  achieved_logs_per_second: number;
  target_attainment_percent: number;
  error_rate_percent: number;
  latency_ms_scheduled: Record<string, number>;
  latency_ms_wire: Record<string, number>;
  status_codes: Record<string, number>;
  errors: Record<string, number>;
}

function summarisePhase(
  phase: Phase,
  collector: PhaseCollector,
  elapsedSeconds: number,
): PhaseReport {
  const accepted = collector.outcomes.accepted;
  const achieved = accepted / elapsedSeconds;
  const shedLogs = collector.outcomes.shed * BATCH_SIZE;

  const failedOrShed = collector.outcomes.failed + collector.outcomes.shed;
  const errorRate =
    collector.outcomes.sent === 0 ? 0 : (failedOrShed / collector.outcomes.sent) * 100;

  return {
    target_logs_per_second: phase.targetLogsPerSecond,
    duration_seconds: round(elapsedSeconds),
    requests_sent: collector.outcomes.sent,
    requests_completed: collector.outcomes.completed,
    logs_accepted: accepted,
    logs_rejected_validation: collector.outcomes.rejected,
    logs_shed: shedLogs,
    requests_shed: collector.outcomes.shed,
    requests_failed: collector.outcomes.failed,
    achieved_logs_per_second: round(achieved),
    target_attainment_percent: round((achieved / phase.targetLogsPerSecond) * 100),
    error_rate_percent: round(errorRate, 3),
    latency_ms_scheduled: collector.latency.percentiles(),
    latency_ms_wire: collector.serviceLatency.percentiles(),
    status_codes: Object.fromEntries(
      [...collector.outcomes.byStatus].map(([status, count]) => [String(status), count]),
    ),
    errors: Object.fromEntries(collector.outcomes.errors),
  };
}

interface ScenarioReport {
  key: string;
  name: string;
  purpose: string;
  batch_size: number;
  bytes_per_entry: number;
  started_at: string;
  settle_seconds: number;
  phases: PhaseReport[];
  totals: {
    logs_accepted: number;
    logs_shed: number;
    requests_failed: number;
    rows_before: number;
    rows_after: number;
    rows_inserted: number;
    accounting_delta: number;
  };
  freshness_ms: number | null;
  queries: ProbeResult[];
  resources: Record<string, ContainerSummary>;
  service_metrics: unknown;
}

async function runScenario(scenario: Scenario, client: LoadClient): Promise<ScenarioReport> {
  console.log(`\n${'='.repeat(78)}`);
  console.log(`${scenario.name}  --  ${scenario.purpose}`);
  console.log('='.repeat(78));

  const pool = new PayloadPool({ batchSize: BATCH_SIZE, poolSize: 24 });
  const monitor = new ResourceMonitor([APP_CONTAINER, PG_CONTAINER], 2000);
  const probe = new QueryProbe(client);

  process.stdout.write('  waiting for database to settle... ');
  const settleSeconds = await waitForQuiescence();
  console.log(`${settleSeconds}s`);

  // Short unmeasured warm-up: fills connection pools, lets V8 reach steady
  // state, and pages the current partition's index into shared_buffers.
  console.log('  warming up for 10s (not measured)');
  await runPhase(client, pool, 5000, 10_000, createCollector());

  const rowsBefore = await countRows();
  const startedAt = new Date().toISOString();

  monitor.start();
  probe.start();

  const phaseReports: PhaseReport[] = [];

  for (const phase of scenario.phases) {
    console.log(
      `\n  phase: ${formatNumber(phase.targetLogsPerSecond)} logs/s for ${phase.durationSeconds}s ` +
        `(${round(phase.targetLogsPerSecond / BATCH_SIZE)} req/s at batch ${BATCH_SIZE})`,
    );

    const collector = createCollector();
    const phaseStart = performance.now();

    await runPhase(
      client,
      pool,
      phase.targetLogsPerSecond,
      phase.durationSeconds * 1000,
      collector,
    );

    const elapsedSeconds = (performance.now() - phaseStart) / 1000;
    const report = summarisePhase(phase, collector, elapsedSeconds);
    phaseReports.push(report);

    console.log(
      `    achieved ${formatNumber(Math.round(report.achieved_logs_per_second))} logs/s ` +
        `(${report.target_attainment_percent}% of target) | ` +
        `p95 ${report.latency_ms_scheduled.p95}ms | ` +
        `errors ${report.error_rate_percent}% | shed ${formatNumber(report.logs_shed)}`,
    );
  }

  // Lets in-flight batches commit before the row count is taken.
  console.log(`\n  draining for ${DRAIN_SECONDS}s...`);
  await sleep(DRAIN_SECONDS * 1000);

  const freshness = await measureFreshness(client);
  const queries = probe.stop();
  const resources = monitor.stop();
  const rowsAfter = await countRows();
  const serviceMetrics = await fetchServiceMetrics(client);

  const totalAccepted = phaseReports.reduce((sum, phase) => sum + phase.logs_accepted, 0);
  const rowsInserted = rowsAfter - rowsBefore;

  const report: ScenarioReport = {
    key: scenario.key,
    name: scenario.name,
    purpose: scenario.purpose,
    batch_size: BATCH_SIZE,
    bytes_per_entry: pool.bytesPerEntry,
    started_at: startedAt,
    settle_seconds: settleSeconds,
    phases: phaseReports,
    totals: {
      logs_accepted: totalAccepted,
      logs_shed: phaseReports.reduce((sum, phase) => sum + phase.logs_shed, 0),
      requests_failed: phaseReports.reduce((sum, phase) => sum + phase.requests_failed, 0),
      rows_before: rowsBefore,
      rows_after: rowsAfter,
      rows_inserted: rowsInserted,
      // Rows actually stored minus rows the service claimed to accept. Any
      // meaningful negative value would mean a 200 was returned for data that
      // never landed.
      accounting_delta: rowsInserted - totalAccepted,
    },
    freshness_ms: freshness,
    queries,
    resources,
    service_metrics: serviceMetrics,
  };

  printScenarioSummary(report);
  return report;
}

function printScenarioSummary(report: ScenarioReport): void {
  const primary = report.queries.find((query) => query.name.startsWith('aggregate_1h'));

  console.log(`\n  --- ${report.name} summary ---`);
  console.log(
    `  rows stored: ${formatNumber(report.totals.rows_inserted)} ` +
      `(service reported ${formatNumber(report.totals.logs_accepted)} accepted, ` +
      `delta ${report.totals.accounting_delta})`,
  );
  if (primary !== undefined) {
    console.log(
      `  primary aggregation (1/s): ${primary.requests} requests, ${primary.errors} errors, ` +
        `${primary.skipped} skipped | p50 ${primary.latency.p50}ms p95 ${primary.latency.p95}ms ` +
        `max ${primary.latency.max}ms`,
    );
  }
  console.log(`  freshness (ingest -> queryable): ${report.freshness_ms ?? 'not observed'}ms`);
  for (const [container, usage] of Object.entries(report.resources)) {
    console.log(
      `  ${container}: cpu mean ${usage.cpuPercentMean}% max ${usage.cpuPercentMax}% | ` +
        `mem mean ${usage.memoryMiBMean}MiB max ${usage.memoryMiBMax}MiB / ${usage.memoryLimitMiB}MiB`,
    );
  }
}

async function main(): Promise<void> {
  const requested = process.argv
    .filter((argument) => argument.startsWith('--scenario='))
    .map((argument) => argument.slice('--scenario='.length));

  const selected =
    requested.length > 0 ? SCENARIOS.filter((s) => requested.includes(s.key)) : SCENARIOS;

  if (selected.length === 0) {
    console.error(`No scenario matched. Available: ${SCENARIOS.map((s) => s.key).join(', ')}`);
    process.exit(1);
  }

  const client = new LoadClient({
    host: HOST,
    port: PORT,
    apiKey: process.env.LOADGEN_API_KEY ?? 'loadgen-key-ignored-when-auth-disabled',
    maxSockets: 256,
  });

  const health = await client.get('/health');
  if (health.status !== 200) {
    throw new Error(`service is not healthy at ${HOST}:${PORT} (status ${health.status})`);
  }
  console.log(`Target ${HOST}:${PORT} is healthy. Batch size ${BATCH_SIZE}.`);

  const reports: ScenarioReport[] = [];
  for (const scenario of selected) {
    reports.push(await runScenario(scenario, client));
    // Lets the system settle so one scenario does not colour the next.
    await sleep(5000);
  }

  await mkdir(RESULTS_DIR, { recursive: true });
  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const path = join(RESULTS_DIR, `results-${stamp}.json`);
  await writeFile(path, JSON.stringify({ generatedAt: stamp, batchSize: BATCH_SIZE, reports }, null, 2));

  console.log(`\n\nResults written to ${path}`);
  client.destroy();
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
