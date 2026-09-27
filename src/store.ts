import { DatabaseSync, type StatementSync } from 'node:sqlite';
import { DEFAULT_BACKOFF, computeBackoff } from './backoff.js';
import type { Job, JobOptions, JobStatus, QueueStats } from './types.js';

export interface StoreOptions {
  /** SQLite file path, or ':memory:'. */
  path?: string;
  /** Injectable clock, used by tests to control time. */
  now?: () => number;
}

interface Row {
  id: number;
  queue: string;
  name: string;
  payload: string;
  status: JobStatus;
  priority: number;
  attempts: number;
  max_attempts: number;
  backoff: string;
  run_at: number;
  locked_until: number | null;
  worker_id: string | null;
  dedupe_key: string | null;
  result: string | null;
  last_error: string | null;
  created_at: number;
  updated_at: number;
  finished_at: number | null;
}

const SCHEMA = `
CREATE TABLE IF NOT EXISTS jobs (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  queue        TEXT    NOT NULL,
  name         TEXT    NOT NULL,
  payload      TEXT    NOT NULL,
  status       TEXT    NOT NULL CHECK (status IN ('waiting','active','completed','dead')),
  priority     INTEGER NOT NULL DEFAULT 0,
  attempts     INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 3,
  backoff      TEXT    NOT NULL,
  run_at       INTEGER NOT NULL,
  locked_until INTEGER,
  worker_id    TEXT,
  dedupe_key   TEXT,
  result       TEXT,
  last_error   TEXT,
  created_at   INTEGER NOT NULL,
  updated_at   INTEGER NOT NULL,
  finished_at  INTEGER
);
-- Covers the hot path: "next runnable job in this queue".
CREATE INDEX IF NOT EXISTS idx_jobs_claim ON jobs (queue, status, priority DESC, run_at, id);
-- Covers the reaper: "active jobs whose lease expired".
CREATE INDEX IF NOT EXISTS idx_jobs_lease ON jobs (status, locked_until);
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_dedupe ON jobs (queue, dedupe_key) WHERE dedupe_key IS NOT NULL;
`;

function toJob<T>(r: Row): Job<T> {
  return {
    id: r.id,
    queue: r.queue,
    name: r.name,
    payload: JSON.parse(r.payload),
    status: r.status,
    priority: r.priority,
    attempts: r.attempts,
    maxAttempts: r.max_attempts,
    backoff: JSON.parse(r.backoff),
    runAt: r.run_at,
    lockedUntil: r.locked_until,
    workerId: r.worker_id,
    dedupeKey: r.dedupe_key,
    result: r.result == null ? null : JSON.parse(r.result),
    lastError: r.last_error,
    createdAt: r.created_at,
    updatedAt: r.updated_at,
    finishedAt: r.finished_at,
  };
}

/**
 * Persistence layer. Every state transition is a single SQL statement, so each one is atomic
 * without explicit transactions, and completions are fenced on worker_id so a worker that lost
 * its lease can never overwrite the outcome of the worker that took the job over.
 */
export class JobStore {
  readonly db: DatabaseSync;
  readonly now: () => number;
  private stmts: Record<string, StatementSync>;

  constructor(opts: StoreOptions = {}) {
    this.now = opts.now ?? Date.now;
    this.db = new DatabaseSync(opts.path ?? ':memory:');
    this.db.exec('PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL; PRAGMA busy_timeout = 5000;');
    this.db.exec(SCHEMA);
    this.stmts = {
      insert: this.db.prepare(`
        INSERT INTO jobs (queue, name, payload, status, priority, max_attempts, backoff, run_at, dedupe_key, created_at, updated_at)
        VALUES (?, ?, ?, 'waiting', ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (queue, dedupe_key) WHERE dedupe_key IS NOT NULL DO NOTHING
        RETURNING *`),
      byDedupe: this.db.prepare(`SELECT * FROM jobs WHERE queue = ? AND dedupe_key = ?`),
      byId: this.db.prepare(`SELECT * FROM jobs WHERE id = ?`),
      // Atomic claim: pick the best runnable job and lease it in one statement.
      claim: this.db.prepare(`
        UPDATE jobs
           SET status = 'active', attempts = attempts + 1, worker_id = ?, locked_until = ?, updated_at = ?
         WHERE id = (
           SELECT id FROM jobs
            WHERE queue = ? AND status = 'waiting' AND run_at <= ?
            ORDER BY priority DESC, run_at, id
            LIMIT 1)
        RETURNING *`),
      heartbeat: this.db.prepare(`
        UPDATE jobs SET locked_until = ?, updated_at = ?
         WHERE id = ? AND worker_id = ? AND status = 'active'`),
      complete: this.db.prepare(`
        UPDATE jobs
           SET status = 'completed', result = ?, locked_until = NULL, updated_at = ?, finished_at = ?
         WHERE id = ? AND worker_id = ? AND status = 'active'`),
      retry: this.db.prepare(`
        UPDATE jobs
           SET status = 'waiting', run_at = ?, last_error = ?, locked_until = NULL, worker_id = NULL, updated_at = ?
         WHERE id = ? AND worker_id = ? AND status = 'active'`),
      bury: this.db.prepare(`
        UPDATE jobs
           SET status = 'dead', last_error = ?, locked_until = NULL, updated_at = ?, finished_at = ?
         WHERE id = ? AND worker_id = ? AND status = 'active'`),
      // Crash recovery: leases that expired belong to workers that died or hung.
      reap: this.db.prepare(`
        UPDATE jobs
           SET status      = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'waiting' END,
               finished_at = CASE WHEN attempts >= max_attempts THEN ? ELSE NULL END,
               last_error  = 'lease expired (worker crashed or stalled)',
               worker_id = NULL, locked_until = NULL, run_at = ?, updated_at = ?
         WHERE status = 'active' AND locked_until < ?
        RETURNING id, status`),
      requeue: this.db.prepare(`
        UPDATE jobs
           SET status = 'waiting', attempts = 0, run_at = ?, last_error = NULL, finished_at = NULL,
               worker_id = NULL, updated_at = ?
         WHERE id = ? AND status = 'dead'`),
      stats: this.db.prepare(`
        SELECT
          SUM(status = 'waiting' AND run_at <= ?) AS waiting,
          SUM(status = 'waiting' AND run_at >  ?) AS delayed,
          SUM(status = 'active')    AS active,
          SUM(status = 'completed') AS completed,
          SUM(status = 'dead')      AS dead
        FROM jobs WHERE queue = ?`),
      queues: this.db.prepare(`SELECT DISTINCT queue FROM jobs ORDER BY queue`),
      clean: this.db.prepare(`DELETE FROM jobs WHERE queue = ? AND status = ? AND finished_at < ?`),
    };
  }

  add<T>(queue: string, name: string, payload: T, opts: JobOptions = {}): Job<T> {
    const now = this.now();
    const maxAttempts = opts.maxAttempts ?? 3;
    if (!Number.isInteger(maxAttempts) || maxAttempts < 1) throw new RangeError('maxAttempts must be an integer >= 1');
    const row = this.stmts.insert.get(
      queue,
      name,
      JSON.stringify(payload ?? null),
      opts.priority ?? 0,
      maxAttempts,
      JSON.stringify(opts.backoff ?? DEFAULT_BACKOFF),
      now + (opts.delayMs ?? 0),
      opts.dedupeKey ?? null,
      now,
      now,
    ) as Row | undefined;
    if (row) return toJob<T>(row);
    // Dedupe hit: return the job that already owns this key.
    return toJob<T>(this.stmts.byDedupe.get(queue, opts.dedupeKey!) as unknown as Row);
  }

  get<T = unknown>(id: number): Job<T> | undefined {
    const row = this.stmts.byId.get(id) as Row | undefined;
    return row && toJob<T>(row);
  }

  claim(queue: string, workerId: string, leaseMs: number): Job | undefined {
    const now = this.now();
    const row = this.stmts.claim.get(workerId, now + leaseMs, now, queue, now) as Row | undefined;
    return row && toJob(row);
  }

  heartbeat(id: number, workerId: string, leaseMs: number): boolean {
    const now = this.now();
    return this.stmts.heartbeat.run(now + leaseMs, now, id, workerId).changes === 1;
  }

  complete(id: number, workerId: string, result: unknown): boolean {
    const now = this.now();
    return this.stmts.complete.run(JSON.stringify(result ?? null), now, now, id, workerId).changes === 1;
  }

  /** Record a failure: schedule a retry with backoff, or move to the dead-letter state. */
  fail(job: Job, workerId: string, error: string): 'retry' | 'dead' | 'lost' {
    const now = this.now();
    if (job.attempts >= job.maxAttempts) {
      return this.stmts.bury.run(error, now, now, job.id, workerId).changes === 1 ? 'dead' : 'lost';
    }
    const runAt = now + computeBackoff(job.backoff, job.attempts);
    return this.stmts.retry.run(runAt, error, now, job.id, workerId).changes === 1 ? 'retry' : 'lost';
  }

  /** Release jobs whose lease expired. Returns ids that were requeued and ids that were buried. */
  reapExpired(): { requeued: number[]; dead: number[] } {
    const now = this.now();
    const rows = this.stmts.reap.all(now, now, now, now) as { id: number; status: JobStatus }[];
    return {
      requeued: rows.filter((r) => r.status === 'waiting').map((r) => r.id),
      dead: rows.filter((r) => r.status === 'dead').map((r) => r.id),
    };
  }

  /** Move a dead job back to the queue with a fresh attempt budget. */
  requeueDead(id: number): boolean {
    const now = this.now();
    return this.stmts.requeue.run(now, now, id).changes === 1;
  }

  stats(queue: string): QueueStats {
    const now = this.now();
    const r = this.stmts.stats.get(now, now, queue) as Record<keyof QueueStats, number | null>;
    return {
      waiting: r.waiting ?? 0,
      delayed: r.delayed ?? 0,
      active: r.active ?? 0,
      completed: r.completed ?? 0,
      dead: r.dead ?? 0,
    };
  }

  queues(): string[] {
    return (this.stmts.queues.all() as { queue: string }[]).map((r) => r.queue);
  }

  list(queue: string, status?: JobStatus, limit = 50, offset = 0): Job[] {
    const sql = status
      ? `SELECT * FROM jobs WHERE queue = ? AND status = ? ORDER BY updated_at DESC, id DESC LIMIT ? OFFSET ?`
      : `SELECT * FROM jobs WHERE queue = ? ORDER BY updated_at DESC, id DESC LIMIT ? OFFSET ?`;
    const params = status ? [queue, status, limit, offset] : [queue, limit, offset];
    return (this.db.prepare(sql).all(...params) as unknown as Row[]).map((r) => toJob(r));
  }

  /** Delete finished jobs older than `olderThanMs`. Returns the number removed. */
  clean(queue: string, status: 'completed' | 'dead', olderThanMs: number): number {
    return Number(this.stmts.clean.run(queue, status, this.now() - olderThanMs).changes);
  }

  close(): void {
    this.db.close();
  }
}
