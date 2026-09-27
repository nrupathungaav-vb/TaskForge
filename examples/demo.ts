/**
 * Live demo: starts the API + dashboard, a producer that keeps enqueueing work,
 * and workers whose handlers fail randomly so you can watch retries and dead-lettering.
 *
 *   npm run demo  →  open http://localhost:3000
 */
import { JobStore, Queue, Worker, createServer } from '../src/index.js';

const PORT = Number(process.env.PORT ?? 3000);
const store = new JobStore({ path: process.env.DB_PATH ?? 'taskforge-demo.db' });
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

const emails = new Queue<{ to: string }>('emails', store);
const images = new Queue<{ file: string; width: number }>('image-resize', store);

const workers = [
  new Worker<{ to: string }>('emails', store, async (job) => {
    await sleep(200 + Math.random() * 600);
    if (Math.random() < 0.25) throw new Error(`SMTP 421: ${job.payload.to} temporarily unavailable`);
    return { messageId: `msg-${job.id}` };
  }, { concurrency: 3, pollIntervalMs: 100 }),

  new Worker<{ file: string; width: number }>('image-resize', store, async (job, ctx) => {
    for (let step = 0; step < 4; step++) {
      if (ctx.signal.aborted) return;
      await sleep(250 + Math.random() * 500);
    }
    if (job.payload.file.endsWith('.bmp')) throw new Error('unsupported format: bmp');
    return { output: job.payload.file.replace(/(\.\w+)$/, `@${job.payload.width}w$1`) };
  }, { concurrency: 2, pollIntervalMs: 100, timeoutMs: 5000 }),
];

for (const w of workers) {
  w.on('failed', (job, err, outcome) => console.log(`✗ ${job.queue}#${job.id} attempt ${job.attempts}/${job.maxAttempts} → ${outcome}: ${err.message}`));
  w.on('recovered', (info) => console.log(`↺ recovered ${info.requeued.length} stalled job(s)`));
  w.start();
}

const users = ['ada', 'grace', 'linus', 'margaret', 'ken', 'barbara'];
const producer = setInterval(() => {
  const user = users[Math.floor(Math.random() * users.length)];
  emails.add('welcome', { to: `${user}@example.com` }, {
    priority: Math.random() < 0.2 ? 10 : 0,
    backoff: { type: 'exponential', delayMs: 500, maxDelayMs: 8000, jitter: true },
  });
  if (Math.random() < 0.5) {
    const ext = Math.random() < 0.1 ? 'bmp' : 'jpg';
    images.add('thumbnail', { file: `${user}-avatar.${ext}`, width: 256 }, {
      delayMs: Math.random() < 0.3 ? 3000 : 0,
      maxAttempts: 2,
      backoff: { type: 'fixed', delayMs: 1000 },
    });
  }
}, 400);

const server = createServer(store).listen(PORT, () => {
  console.log(`TaskForge dashboard → http://localhost:${PORT}`);
});

async function shutdown() {
  console.log('\nshutting down gracefully (finishing in-flight jobs)...');
  clearInterval(producer);
  server.close();
  await Promise.all(workers.map((w) => w.stop()));
  store.close();
  process.exit(0);
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
