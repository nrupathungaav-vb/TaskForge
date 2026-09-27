export type JobStatus = 'waiting' | 'active' | 'completed' | 'dead';

export type BackoffStrategy =
  | { type: 'fixed'; delayMs: number }
  | { type: 'exponential'; delayMs: number; maxDelayMs?: number; jitter?: boolean };

export interface JobOptions {
  /** Higher numbers run first. Default 0. */
  priority?: number;
  /** Delay before the job becomes eligible to run. */
  delayMs?: number;
  /** Total attempts including the first run. Default 3. */
  maxAttempts?: number;
  backoff?: BackoffStrategy;
  /** If set, adding a second job with the same key to the same queue returns the existing job. */
  dedupeKey?: string;
}

export interface Job<T = unknown> {
  id: number;
  queue: string;
  name: string;
  payload: T;
  status: JobStatus;
  priority: number;
  attempts: number;
  maxAttempts: number;
  backoff: BackoffStrategy;
  runAt: number;
  lockedUntil: number | null;
  workerId: string | null;
  dedupeKey: string | null;
  result: unknown;
  lastError: string | null;
  createdAt: number;
  updatedAt: number;
  finishedAt: number | null;
}

export interface QueueStats {
  waiting: number;
  delayed: number;
  active: number;
  completed: number;
  dead: number;
}

export interface JobContext {
  /** Extend this job's lease; call during long-running work. Done automatically by the worker. */
  heartbeat(): boolean;
  /** Aborted when the job times out or the worker is force-stopped. */
  signal: AbortSignal;
  log(message: string): void;
}

export type JobHandler<T = any, R = unknown> = (job: Job<T>, ctx: JobContext) => Promise<R> | R;
