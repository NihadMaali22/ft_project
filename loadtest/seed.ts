import { LoadClient } from './lib/client.ts';
import { buildBatch } from './lib/payload.ts';
import { formatNumber } from './lib/stats.ts';

/**
 * Seeds the dataset the performance targets assume: roughly one million rows
 * spread over about a month.
 *
 * Loaded through the public API rather than by direct COPY, so the seed
 * exercises the same validation, partition-routing and write path as live
 * traffic. A month-wide spread also forces ~30 daily partitions into existence,
 * which is what makes the later aggregation measurements meaningful - querying
 * one partition holding everything would flatter the results.
 */

const TOTAL_ROWS = Number(process.env.SEED_ROWS ?? 1_000_000);
const SPREAD_DAYS = Number(process.env.SEED_DAYS ?? 30);
const BATCH_SIZE = Number(process.env.SEED_BATCH ?? 1000);
const CONCURRENCY = Number(process.env.SEED_CONCURRENCY ?? 8);

const HOST = process.env.TARGET_HOST ?? '127.0.0.1';
const PORT = Number(process.env.TARGET_PORT ?? 8080);

async function main(): Promise<void> {
  const client = new LoadClient({
    host: HOST,
    port: PORT,
    apiKey: process.env.LOADGEN_API_KEY,
    maxSockets: CONCURRENCY * 2,
  });

  const health = await client.get('/health');
  if (health.status !== 200) {
    throw new Error(`service is not healthy (status ${health.status})`);
  }

  const totalBatches = Math.ceil(TOTAL_ROWS / BATCH_SIZE);
  const spreadMs = SPREAD_DAYS * 24 * 60 * 60 * 1000;
  const now = Date.now();

  console.log(
    `Seeding ${formatNumber(TOTAL_ROWS)} rows across ${SPREAD_DAYS} days ` +
      `(${formatNumber(totalBatches)} batches of ${BATCH_SIZE}, concurrency ${CONCURRENCY})`,
  );

  const startedAt = performance.now();
  let completed = 0;
  let accepted = 0;
  let failed = 0;
  let nextBatch = 0;

  async function worker(): Promise<void> {
    for (;;) {
      const index = nextBatch++;
      if (index >= totalBatches) return;

      // Walks backwards from now so the newest data sits at the end of the
      // range, matching how a real month of logs accumulates.
      const age = (index / totalBatches) * spreadMs;
      const timestampBase = now - age;

      const body = buildBatch(BATCH_SIZE, timestampBase, 60_000);
      const outcome = await client.postLogs(body, performance.now());

      completed += 1;
      accepted += outcome.accepted;
      if (outcome.status !== 200) failed += 1;

      if (completed % 100 === 0) {
        const elapsed = (performance.now() - startedAt) / 1000;
        const rate = accepted / elapsed;
        const remaining = (totalBatches - completed) / (completed / elapsed);
        process.stdout.write(
          `\r  ${formatNumber(accepted)} rows | ${Math.round(rate).toLocaleString()} rows/s | ` +
            `${completed}/${totalBatches} batches | eta ${Math.round(remaining)}s   `,
        );
      }
    }
  }

  await Promise.all(Array.from({ length: CONCURRENCY }, () => worker()));

  const elapsedSeconds = (performance.now() - startedAt) / 1000;
  process.stdout.write('\n');
  console.log(
    `Seeded ${formatNumber(accepted)} rows in ${elapsedSeconds.toFixed(1)}s ` +
      `(${Math.round(accepted / elapsedSeconds).toLocaleString()} rows/s, ${failed} failed batches)`,
  );

  client.destroy();
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
