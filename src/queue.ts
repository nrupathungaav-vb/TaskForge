import { JobStore } from './store.js';
import type { Job, JobOptions, JobStatus, QueueStats } from './types.js';

/** Producer-side handle for one named queue. */
export class Queue<T = unknown> {
  constructor(
    readonly name: string,
    readonly store: JobStore,
  ) {
    if (!name) throw new Error('queue name is required');
  }

  add(jobName: string, payload: T, opts?: JobOptions): Job<T> {
    return this.store.add(this.name, jobName, payload, opts);
  }

  addBulk(jobs: { name: string; payload: T; opts?: JobOptions }[]): Job<T>[] {
    const db = this.store.db;
    db.exec('BEGIN');
    try {
      const out = jobs.map((j) => this.add(j.name, j.payload, j.opts));
      db.exec('COMMIT');
      return out;
    } catch (err) {
      db.exec('ROLLBACK');
      throw err;
    }
  }

  get(id: number): Job<T> | undefined {
    const job = this.store.get<T>(id);
    return job?.queue === this.name ? job : undefined;
  }

  list(status?: JobStatus, limit?: number, offset?: number): Job<T>[] {
    return this.store.list(this.name, status, limit, offset) as Job<T>[];
  }

  stats(): QueueStats {
    return this.store.stats(this.name);
  }

  retryDead(id: number): boolean {
    return this.get(id) !== undefined && this.store.requeueDead(id);
  }

  clean(status: 'completed' | 'dead', olderThanMs: number): number {
    return this.store.clean(this.name, status, olderThanMs);
  }
}
