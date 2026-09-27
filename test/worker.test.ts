import { describe, it, expect, afterEach } from 'vitest';
import { JobStore } from '../src/store.js';
import { Queue } from '../src/queue.js';
import { Worker } from '../src/worker.js';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const fast = { pollIntervalMs: 5, reapIntervalMs: 20 };
const workers: Worker[] = [];

function track<W extends Worker>(w: W): W {
  workers.push(w);
  return w;
}

async function until(cond: () => boolean, timeoutMs = 3000) {
  const start = Date.now();
  while (!cond()) {
    if (Date.now() - start > timeoutMs) throw new Error('timed out waiting for condition');
    await sleep(5);
  }
}

afterEach(async () => {
  await Promise.all(workers.splice(0).map((w) => w.stop({ force: true })));
});

describe('Worker', () => {
  it('processes jobs and stores results', async () => {
    const store = new JobStore();
    const q = new Queue<{ n: number }>('math', store);
    const jobs = [1, 2, 3].map((n) => q.add('square', { n }));
    track(new Worker('math', store, (job) => job.payload.n ** 2, fast)).start();

    await until(() => q.stats().completed === 3);
    expect(jobs.map((j) => q.get(j.id)!.result)).toEqual([1, 4, 9]);
  });

  it('never exceeds its concurrency limit', async () => {
    const store = new JobStore();
    const q = new Queue('q', store);
    for (let i = 0; i < 20; i++) q.add('x', i);
    let running = 0;
    let peak = 0;
    track(
      new Worker(
        'q',
        store,
        async () => {
          peak = Math.max(peak, ++running);
          await sleep(10);
          running--;
        },
        { ...fast, concurrency: 4 },
      ),
    ).start();

    await until(() => q.stats().completed === 20);
    expect(peak).toBe(4);
  });

  it('processes every job exactly once across competing workers', async () => {
    const store = new JobStore();
    const q = new Queue<number>('q', store);
    for (let i = 0; i < 100; i++) q.add('x', i);
    const seen: number[] = [];
    const handler = async (job: { payload: number }) => {
      seen.push(job.payload);
      await sleep(1);
    };
    for (let i = 0; i < 3; i++) track(new Worker('q', store, handler, { ...fast, concurrency: 5 })).start();

    await until(() => q.stats().completed === 100);
    expect(seen.sort((a, b) => a - b)).toEqual([...Array(100).keys()]);
  });

  it('retries a failing job and eventually succeeds', async () => {
    const store = new JobStore();
    const q = new Queue('q', store);
    const job = q.add('flaky', null, { maxAttempts: 3, backoff: { type: 'fixed', delayMs: 5 } });
    let calls = 0;
    const outcomes: string[] = [];
    const w = track(
      new Worker(
        'q',
        store,
        () => {
          if (++calls < 3) throw new Error(`fail ${calls}`);
          return 'ok';
        },
        fast,
      ),
    );
    w.on('failed', (_j, _e, outcome) => outcomes.push(outcome));
    w.start();

    await until(() => q.get(job.id)!.status === 'completed');
    expect(outcomes).toEqual(['retry', 'retry']);
    expect(q.get(job.id)).toMatchObject({ attempts: 3, result: 'ok' });
  });

  it('dead-letters a job that keeps failing', async () => {
    const store = new JobStore();
    const q = new Queue('q', store);
    const job = q.add('broken', null, { maxAttempts: 2, backoff: { type: 'fixed', delayMs: 0 } });
    track(
      new Worker(
        'q',
        store,
        () => {
          throw new Error('always broken');
        },
        fast,
      ),
    ).start();

    await until(() => q.get(job.id)!.status === 'dead');
    expect(q.get(job.id)!.lastError).toContain('always broken');
  });

  it('times out a hung job and aborts its signal', async () => {
    const store = new JobStore();
    const q = new Queue('q', store);
    const job = q.add('hang', null, { maxAttempts: 1 });
    let aborted = false;
    track(
      new Worker(
        'q',
        store,
        (_job, ctx) =>
          new Promise(() => {
            ctx.signal.addEventListener('abort', () => (aborted = true));
          }),
        { ...fast, timeoutMs: 30 },
      ),
    ).start();

    await until(() => q.get(job.id)!.status === 'dead');
    expect(aborted).toBe(true);
    expect(q.get(job.id)!.lastError).toContain('timed out');
  });

  it('recovers jobs abandoned by a crashed worker', async () => {
    const store = new JobStore();
    const q = new Queue('q', store);
    const job = q.add('x', null);
    // Simulate a worker that claimed the job and then died without heartbeating.
    store.claim('q', 'dead-worker', 20);

    const w = track(new Worker('q', store, () => 'recovered', fast));
    const recovered: number[] = [];
    w.on('recovered', (info) => recovered.push(...info.requeued));
    w.start();

    await until(() => q.get(job.id)!.status === 'completed');
    expect(recovered).toContain(job.id);
    expect(q.get(job.id)).toMatchObject({ attempts: 2, result: 'recovered' });
  });

  it('keeps long jobs alive with heartbeats', async () => {
    const store = new JobStore();
    const q = new Queue('q', store);
    const job = q.add('long', null);
    track(
      new Worker(
        'q',
        store,
        async () => {
          await sleep(150);
          return 'done';
        },
        { ...fast, leaseMs: 40 },
      ),
    ).start();

    await until(() => q.get(job.id)!.status === 'completed');
    expect(q.get(job.id)!.attempts).toBe(1);
  });

  it('stops gracefully, letting in-flight jobs finish', async () => {
    const store = new JobStore();
    const q = new Queue('q', store);
    for (let i = 0; i < 5; i++) q.add('x', i);
    const w = new Worker(
      'q',
      store,
      async () => {
        await sleep(30);
      },
      { ...fast, concurrency: 2 },
    ).start();

    await until(() => w.activeCount === 2);
    await w.stop();
    expect(q.stats()).toMatchObject({ completed: 2, active: 0, waiting: 3 });
  });
});
