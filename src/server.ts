import express, { type NextFunction, type Request, type Response } from 'express';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import type { JobStore } from './store.js';
import type { JobOptions, JobStatus } from './types.js';

const STATUSES: JobStatus[] = ['waiting', 'active', 'completed', 'dead'];
const QUEUE_NAME = /^[A-Za-z0-9_.:-]{1,64}$/;

class HttpError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}

function parseJobOptions(body: any): JobOptions {
  const opts: JobOptions = {};
  const int = (v: unknown, field: string, min: number) => {
    if (v === undefined) return undefined;
    if (!Number.isInteger(v) || (v as number) < min) throw new HttpError(400, `${field} must be an integer >= ${min}`);
    return v as number;
  };
  opts.priority = int(body.priority, 'priority', -1_000_000);
  opts.delayMs = int(body.delayMs, 'delayMs', 0);
  opts.maxAttempts = int(body.maxAttempts, 'maxAttempts', 1);
  if (body.dedupeKey !== undefined) {
    if (typeof body.dedupeKey !== 'string' || !body.dedupeKey) throw new HttpError(400, 'dedupeKey must be a non-empty string');
    opts.dedupeKey = body.dedupeKey;
  }
  if (body.backoff !== undefined) {
    const b = body.backoff;
    if (!b || !['fixed', 'exponential'].includes(b.type) || !Number.isInteger(b.delayMs) || b.delayMs < 0) {
      throw new HttpError(400, 'backoff must be { type: "fixed" | "exponential", delayMs: integer >= 0 }');
    }
    opts.backoff = b;
  }
  return opts;
}

/** REST API plus a static dashboard for inspecting and managing queues. */
export function createServer(store: JobStore) {
  const app = express();
  app.use(express.json({ limit: '1mb' }));
  const publicDir = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'public');
  app.use(express.static(publicDir));

  app.param('queue', (_req, _res, next, value: string) => {
    next(QUEUE_NAME.test(value) ? undefined : new HttpError(400, 'invalid queue name'));
  });

  const jobId = (req: Request) => {
    const id = Number(req.params.id);
    if (!Number.isInteger(id) || id < 1) throw new HttpError(400, 'invalid job id');
    return id;
  };

  app.get('/api/health', (_req, res) => {
    res.json({ ok: true });
  });

  app.get('/api/queues', (_req, res) => {
    res.json(store.queues().map((name) => ({ name, ...store.stats(name) })));
  });

  app.post('/api/queues/:queue/jobs', (req, res) => {
    const { name, payload } = req.body ?? {};
    if (typeof name !== 'string' || !name) throw new HttpError(400, 'name is required');
    const job = store.add(req.params.queue as string, name, payload ?? null, parseJobOptions(req.body));
    res.status(201).json(job);
  });

  app.get('/api/queues/:queue/jobs', (req, res) => {
    const status = req.query.status as JobStatus | undefined;
    if (status && !STATUSES.includes(status)) throw new HttpError(400, `status must be one of ${STATUSES.join(', ')}`);
    const limit = Math.min(Number(req.query.limit ?? 50) || 50, 500);
    const offset = Math.max(Number(req.query.offset ?? 0) || 0, 0);
    res.json(store.list(req.params.queue as string, status, limit, offset));
  });

  app.get('/api/queues/:queue/stats', (req, res) => {
    res.json(store.stats(req.params.queue as string));
  });

  app.get('/api/jobs/:id', (req, res) => {
    const job = store.get(jobId(req));
    if (!job) throw new HttpError(404, 'job not found');
    res.json(job);
  });

  app.post('/api/jobs/:id/retry', (req, res) => {
    const id = jobId(req);
    if (!store.get(id)) throw new HttpError(404, 'job not found');
    if (!store.requeueDead(id)) throw new HttpError(409, 'only dead jobs can be retried');
    res.json(store.get(id));
  });

  app.use('/api', (_req, _res, next) => next(new HttpError(404, 'not found')));

  app.use((err: any, _req: Request, res: Response, _next: NextFunction) => {
    const status = err instanceof HttpError ? err.status : err.status ?? 500;
    res.status(status).json({ error: status >= 500 ? 'internal error' : err.message });
  });

  return app;
}
