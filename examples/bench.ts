/** Throughput benchmark: enqueue N no-op jobs, then drain them with W workers. */
import { rmSync } from 'node:fs';
import { JobStore, Queue, Worker } from '../src/index.js';

const N = Number(process.env.N ?? 20_000);
const CONCURRENCY = Number(process.env.CONCURRENCY ?? 16);
const path = 'bench.db';
for (const f of [path, `${path}-wal`, `${path}-shm`]) rmSync(f, { force: true });

const store = new JobStore({ path });
const q = new Queue<number>('bench', store);

let t = performance.now();
q.addBulk(Array.from({ length: N }, (_, i) => ({ name: 'noop', payload: i })));
const enqueueMs = performance.now() - t;

t = performance.now();
const worker = new Worker('bench', store, () => null, { concurrency: CONCURRENCY, pollIntervalMs: 1 });
await new Promise<void>((resolve) => {
  worker.on('drained', () => q.stats().completed === N && resolve());
  worker.start();
});
const drainMs = performance.now() - t;
await worker.stop();
store.close();
for (const f of [path, `${path}-wal`, `${path}-shm`]) rmSync(f, { force: true });

const fmt = (n: number) => Math.round(n).toLocaleString('en-US');
console.log(`jobs:        ${fmt(N)}`);
console.log(`enqueue:     ${fmt(enqueueMs)} ms  (${fmt(N / (enqueueMs / 1000))} jobs/s, batched)`);
console.log(`process:     ${fmt(drainMs)} ms  (${fmt(N / (drainMs / 1000))} jobs/s, concurrency ${CONCURRENCY}, durable on disk)`);
