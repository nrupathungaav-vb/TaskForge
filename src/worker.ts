import { EventEmitter } from 'node:events';
import { randomUUID } from 'node:crypto';
import type { JobStore } from './store.js';
import type { Job, JobHandler } from './types.js';

export interface WorkerOptions {
  /** Max jobs processed at the same time. Default 1. */
  concurrency?: number;
  /** How long a claimed job is leased before another worker may take it over. Default 30s. */
  leaseMs?: number;
  /** How often to poll when the queue is empty. Default 250ms. */
  pollIntervalMs?: number;
  /** Abort and fail a job that runs longer than this. Default: no timeout. */
  timeoutMs?: number;
  /** How often to sweep for expired leases from crashed workers. Default 5s. */
  reapIntervalMs?: number;
  workerId?: string;
}

export interface WorkerEvents {
  active: [job: Job];
  completed: [job: Job, result: unknown];
  failed: [job: Job, error: Error, outcome: 'retry' | 'dead'];
  /** The job's lease was taken over by another worker; its result was discarded. */
  lost: [job: Job];
  recovered: [info: { requeued: number[]; dead: number[] }];
  log: [job: Job, message: string];
  error: [error: Error];
  drained: [];
}

class TimeoutError extends Error {
  constructor(ms: number) {
    super(`job timed out after ${ms}ms`);
    this.name = 'TimeoutError';
  }
}

/**
 * Consumer that pulls jobs from one queue and runs them with bounded concurrency.
 * Delivery is at-least-once: a job is only marked complete after the handler resolves,
 * so a crash mid-job means the job is retried once its lease expires.
 */
export class Worker<T = any> extends EventEmitter<WorkerEvents> {
  readonly id: string;
  private readonly opts: Required<Omit<WorkerOptions, 'timeoutMs' | 'workerId'>> & { timeoutMs?: number };
  private readonly inFlight = new Map<number, { controller: AbortController; promise: Promise<void> }>();
  private running = false;
  private pollTimer?: NodeJS.Timeout;
  private reapTimer?: NodeJS.Timeout;
  private wake?: () => void;
  private loop?: Promise<void>;

  constructor(
    readonly queue: string,
    private readonly store: JobStore,
    private readonly handler: JobHandler<T>,
    opts: WorkerOptions = {},
  ) {
    super();
    this.id = opts.workerId ?? `worker-${randomUUID().slice(0, 8)}`;
    this.opts = {
      concurrency: opts.concurrency ?? 1,
      leaseMs: opts.leaseMs ?? 30_000,
      pollIntervalMs: opts.pollIntervalMs ?? 250,
      reapIntervalMs: opts.reapIntervalMs ?? 5_000,
      timeoutMs: opts.timeoutMs,
    };
    if (this.opts.concurrency < 1) throw new RangeError('concurrency must be >= 1');
  }

  get isRunning(): boolean {
    return this.running;
  }

  get activeCount(): number {
    return this.inFlight.size;
  }

  start(): this {
    if (this.running) return this;
    this.running = true;
    this.reap();
    this.reapTimer = setInterval(() => this.reap(), this.opts.reapIntervalMs);
    this.reapTimer.unref();
    this.loop = this.run();
    return this;
  }

  /**
   * Graceful shutdown: stop claiming new jobs and wait for in-flight jobs to finish.
   * With `force`, in-flight handlers are aborted and their jobs are recorded as failed (and retried per backoff).
   */
  async stop({ force = false }: { force?: boolean } = {}): Promise<void> {
    if (!this.running) return;
    this.running = false;
    clearInterval(this.reapTimer);
    clearTimeout(this.pollTimer);
    this.wake?.();
    if (force) for (const { controller } of this.inFlight.values()) controller.abort(new Error('worker stopped'));
    await this.loop;
    await Promise.allSettled([...this.inFlight.values()].map((f) => f.promise));
  }

  private reap(): void {
    try {
      const info = this.store.reapExpired();
      if (info.requeued.length || info.dead.length) this.emit('recovered', info);
    } catch (err) {
      this.emit('error', err as Error);
    }
  }

  private async run(): Promise<void> {
    while (this.running) {
      // Fill every free slot before sleeping.
      let claimed = false;
      while (this.running && this.inFlight.size < this.opts.concurrency) {
        let job: Job | undefined;
        try {
          job = this.store.claim(this.queue, this.id, this.opts.leaseMs);
        } catch (err) {
          this.emit('error', err as Error);
        }
        if (!job) break;
        claimed = true;
        this.dispatch(job);
      }
      if (!this.running) break;
      if (!claimed && this.inFlight.size === 0) this.emit('drained');
      // Sleep until the poll interval elapses or a slot frees up.
      await new Promise<void>((resolve) => {
        this.wake = resolve;
        this.pollTimer = setTimeout(resolve, this.opts.pollIntervalMs);
      });
      this.wake = undefined;
    }
  }

  private dispatch(job: Job): void {
    const controller = new AbortController();
    const promise = this.execute(job, controller).finally(() => {
      this.inFlight.delete(job.id);
      this.wake?.();
    });
    this.inFlight.set(job.id, { controller, promise });
  }

  private async execute(job: Job, controller: AbortController): Promise<void> {
    const { leaseMs, timeoutMs } = this.opts;
    const heartbeat = () => this.store.heartbeat(job.id, this.id, leaseMs);
    // Renew the lease at a third of its length so a healthy long job is never reaped.
    const hb = setInterval(() => {
      if (!heartbeat()) controller.abort(new Error('lease lost'));
    }, Math.max(10, Math.floor(leaseMs / 3)));
    let timeout: NodeJS.Timeout | undefined;

    this.emit('active', job);
    try {
      const aborted = new Promise<never>((_, reject) => {
        const onAbort = () => reject(controller.signal.reason);
        if (controller.signal.aborted) onAbort();
        else controller.signal.addEventListener('abort', onAbort, { once: true });
      });
      if (timeoutMs) timeout = setTimeout(() => controller.abort(new TimeoutError(timeoutMs)), timeoutMs);

      const ctx = { heartbeat, signal: controller.signal, log: (m: string) => this.emit('log', job, m) };
      const result = await Promise.race([Promise.resolve().then(() => this.handler(job as Job<T>, ctx)), aborted]);

      if (this.store.complete(job.id, this.id, result)) this.emit('completed', job, result);
      else this.emit('lost', job);
    } catch (e) {
      const error = e instanceof Error ? e : new Error(String(e));
      const outcome = this.store.fail(job, this.id, error.stack ?? error.message);
      if (outcome === 'lost') this.emit('lost', job);
      else this.emit('failed', job, error, outcome);
    } finally {
      clearInterval(hb);
      clearTimeout(timeout);
    }
  }
}
