import { LoadClient, createCollector, runPhase } from './lib/client.ts';
import { PayloadPool } from './lib/payload.ts';

const rate = Number(process.argv[2] ?? 15000);
const seconds = Number(process.argv[3] ?? 10);
const client = new LoadClient({ host: '127.0.0.1', port: 8080, maxSockets: 128 });
const pool = new PayloadPool({ batchSize: 500, poolSize: 8 });
console.log('bytes/entry:', pool.bytesPerEntry);
const c = createCollector();
const t0 = performance.now();
await runPhase(client, pool, rate, seconds * 1000, c);
const el = (performance.now()-t0)/1000;
console.log('sent', c.outcomes.sent, 'completed', c.outcomes.completed);
console.log('accepted', c.outcomes.accepted, '=>', Math.round(c.outcomes.accepted/el), 'logs/s (target', rate + ')');
console.log('statuses', [...c.outcomes.byStatus]);
console.log('errors', [...c.outcomes.errors]);
console.log('latency(scheduled)', c.latency.percentiles());
console.log('latency(wire)', c.serviceLatency.percentiles());
client.destroy();
