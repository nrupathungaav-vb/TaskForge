import { describe, it, expect, beforeEach } from 'vitest';
import { JobStore } from '../src/store.js';
import { computeBackoff } from '../src/backoff.js';

let clock: number;
let store: JobStore;
const noJitter = { type: 'exponential' as const, delayMs: 100, maxDelayMs: 1000 };

beforeEach(() => {
  clock = 1_000_000;
  store = new JobStore({ now: () => clock });
});

describe('JobStore', () => {
  it('adds and reads back a job with its payload', () => {
    const job = store.add('email', 'welcome', { to: 'a@b.c' });
    expect(job).toMatchObject({ queue: 'email', name: 'welcome', status: 'waiting', attempts: 0, maxAttempts: 3 });
    expect(store.get(job.id)!.payload).toEqual({ to: 'a@b.c' });
  });

  it('claims highest priority first, then FIFO', () => {
    const low = store.add('q', 'low', null);
    const high = store.add('q', 'high', null, { priority: 10 });
    const low2 = store.add('q', 'low2', null);
    expect([1, 2, 3].map(() => store.claim('q', 'w', 1000)!.id)).toEqual([high.id, low.id, low2.id]);
    expect(store.claim('q', 'w', 1000)).toBeUndefined();
  });

  it('isolates queues from each other', () => {
    store.add('a', 'x', null);
    expect(store.claim('b', 'w', 1000)).toBeUndefined();
    expect(store.claim('a', 'w', 1000)).toBeDefined();
  });

  it('does not hand out delayed jobs before their run time', () => {
    const job = store.add('q', 'later', null, { delayMs: 5000 });
    expect(store.stats('q')).toMatchObject({ waiting: 0, delayed: 1 });
    expect(store.claim('q', 'w', 1000)).toBeUndefined();
    clock += 5000;
    expect(store.claim('q', 'w', 1000)!.id).toBe(job.id);
  });

  it('never gives the same job to two workers', () => {
    store.add('q', 'only', null);
    expect(store.claim('q', 'w1', 1000)).toBeDefined();
    expect(store.claim('q', 'w2', 1000)).toBeUndefined();
  });

  it('retries with exponential backoff and then dead-letters', () => {
    store.add('q', 'flaky', null, { maxAttempts: 3, backoff: noJitter });

    let job = store.claim('q', 'w', 1000)!;
    expect(store.fail(job, 'w', 'boom 1')).toBe('retry');
    expect(store.get(job.id)!.runAt).toBe(clock + 100);
    expect(store.claim('q', 'w', 1000)).toBeUndefined();

    clock += 100;
    job = store.claim('q', 'w', 1000)!;
    expect(job.attempts).toBe(2);
    expect(store.fail(job, 'w', 'boom 2')).toBe('retry');
    expect(store.get(job.id)!.runAt).toBe(clock + 200);

    clock += 200;
    job = store.claim('q', 'w', 1000)!;
    expect(store.fail(job, 'w', 'boom 3')).toBe('dead');
    expect(store.get(job.id)).toMatchObject({ status: 'dead', attempts: 3, lastError: 'boom 3' });
  });

  it('recovers jobs whose worker crashed (lease expired)', () => {
    const job = store.add('q', 'x', null);
    store.claim('q', 'crashed-worker', 1000);
    expect(store.reapExpired().requeued).toEqual([]);
    clock += 1001;
    expect(store.reapExpired().requeued).toEqual([job.id]);
    expect(store.claim('q', 'w2', 1000)!.attempts).toBe(2);
  });

  it('dead-letters a crashed job that has used its last attempt', () => {
    const job = store.add('q', 'x', null, { maxAttempts: 1 });
    store.claim('q', 'w', 1000);
    clock += 1001;
    expect(store.reapExpired().dead).toEqual([job.id]);
    expect(store.get(job.id)!.status).toBe('dead');
  });

  it('fences out a stale worker after its lease was taken over', () => {
    const job = store.add('q', 'x', null);
    const stale = store.claim('q', 'slow', 1000)!;
    clock += 1001;
    store.reapExpired();
    store.claim('q', 'fresh', 1000);
    // The slow worker finally finishes, but it no longer owns the job.
    expect(store.complete(job.id, 'slow', 'late result')).toBe(false);
    expect(store.fail(stale, 'slow', 'late error')).toBe('lost');
    expect(store.heartbeat(job.id, 'slow', 1000)).toBe(false);
    expect(store.complete(job.id, 'fresh', 'ok')).toBe(true);
    expect(store.get(job.id)).toMatchObject({ status: 'completed', result: 'ok' });
  });

  it('heartbeat extends the lease so a long job is not reaped', () => {
    const job = store.add('q', 'long', null);
    store.claim('q', 'w', 1000);
    clock += 900;
    expect(store.heartbeat(job.id, 'w', 1000)).toBe(true);
    clock += 900;
    expect(store.reapExpired().requeued).toEqual([]);
  });

  it('dedupes jobs by key within a queue', () => {
    const a = store.add('q', 'x', { n: 1 }, { dedupeKey: 'order-42' });
    const b = store.add('q', 'x', { n: 2 }, { dedupeKey: 'order-42' });
    const c = store.add('other', 'x', { n: 3 }, { dedupeKey: 'order-42' });
    expect(b.id).toBe(a.id);
    expect(b.payload).toEqual({ n: 1 });
    expect(c.id).not.toBe(a.id);
  });

  it('requeues a dead job with a fresh attempt budget', () => {
    const job = store.add('q', 'x', null, { maxAttempts: 1 });
    store.fail(store.claim('q', 'w', 1000)!, 'w', 'nope');
    expect(store.requeueDead(job.id)).toBe(true);
    expect(store.get(job.id)).toMatchObject({ status: 'waiting', attempts: 0, lastError: null });
    expect(store.requeueDead(job.id)).toBe(false);
  });

  it('cleans old finished jobs only', () => {
    const a = store.add('q', 'a', null);
    store.complete(store.claim('q', 'w', 1000)!.id, 'w', null);
    clock += 10_000;
    store.add('q', 'b', null);
    store.complete(store.claim('q', 'w', 1000)!.id, 'w', null);
    expect(store.clean('q', 'completed', 5_000)).toBe(1);
    expect(store.get(a.id)).toBeUndefined();
    expect(store.stats('q').completed).toBe(1);
  });

  it('rejects invalid maxAttempts', () => {
    expect(() => store.add('q', 'x', null, { maxAttempts: 0 })).toThrow(RangeError);
  });
});

describe('computeBackoff', () => {
  it('doubles and caps', () => {
    expect([1, 2, 3, 4, 5].map((n) => computeBackoff(noJitter, n))).toEqual([100, 200, 400, 800, 1000]);
  });
  it('applies full jitter within [0, cap]', () => {
    expect(computeBackoff({ ...noJitter, jitter: true }, 3, () => 0.5)).toBe(200);
  });
  it('supports fixed delays', () => {
    expect(computeBackoff({ type: 'fixed', delayMs: 250 }, 7)).toBe(250);
  });
});
