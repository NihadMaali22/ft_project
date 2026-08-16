import { LoadClient, createCollector, runPhase } from './lib/client.ts';
import { PayloadPool } from './lib/payload.ts';

/**
 * Throughput regression guard for CI.
 *
 * Usage: ci-guard.ts <targetLogsPerSecond> <seconds> <minimumAcceptedPerSecond>
 *
 * Exits non-zero if achieved throughput falls below the floor or if any request
 * fails. The floor is set well under the measured ceiling because CI runners are
 * slower and noisier than a benchmark host, and a flaky guard is a guard that
 * gets ignored.
 */

const target = Number(process.argv[2] ?? 10_000);
const seconds = Number(process.argv[3] ?? 30);
const floor = Number(process.argv[4] ?? 8_000);

const client = new LoadClient({
  host: process.env.TARGET_HOST ?? '127.0.0.1',
  port: Number(process.env.TARGET_PORT ?? 8080),
  apiKey: process.env.LOADGEN_API_KEY,
  maxSockets: 128,
});

const health = await client.get('/health');
if (health.status !== 200) {
  console.error(`service not healthy (status ${health.status})`);
  process.exit(1);
}

const pool = new PayloadPool({ batchSize: 500, poolSize: 16 });
const collector = createCollector();

console.log(`Driving ${target.toLocaleString()} logs/s for ${seconds}s (floor ${floor.toLocaleString()}/s)`);

const startedAt = performance.now();
await runPhase(client, pool, target, seconds * 1000, collector);
const elapsed = (performance.now() - startedAt) / 1000;

const achieved = collector.outcomes.accepted / elapsed;
const latency = collector.latency.percentiles();

console.log(`  accepted        : ${collector.outcomes.accepted.toLocaleString()}`);
console.log(`  achieved        : ${Math.round(achieved).toLocaleString()} logs/s`);
console.log(`  latency p50/p95 : ${latency.p50}ms / ${latency.p95}ms`);
console.log(`  shed / failed   : ${collector.outcomes.shed} / ${collector.outcomes.failed}`);
console.log(`  status codes    : ${JSON.stringify(Object.fromEntries(collector.outcomes.byStatus))}`);

client.destroy();

const problems: string[] = [];
if (achieved < floor) {
  problems.push(`throughput ${Math.round(achieved).toLocaleString()}/s is below the floor of ${floor.toLocaleString()}/s`);
}
if (collector.outcomes.failed > 0) {
  problems.push(`${collector.outcomes.failed} requests failed`);
}

if (problems.length > 0) {
  console.error(`\nFAILED: ${problems.join('; ')}`);
  process.exit(1);
}

console.log('\nPerformance guard passed.');
