import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { JobStore } from '../src/store.js';
import { createServer } from '../src/server.js';

let server: Server;
let base: string;
const store = new JobStore();

beforeAll(async () => {
  server = createServer(store).listen(0);
  await new Promise((r) => server.once('listening', r));
  base = `http://127.0.0.1:${(server.address() as AddressInfo).port}/api`;
});

afterAll(() => server.close());

const post = (path: string, body?: unknown) =>
  fetch(base + path, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body ?? {}) });

describe('REST API', () => {
  it('enqueues and fetches a job', async () => {
    const res = await post('/queues/emails/jobs', { name: 'welcome', payload: { to: 'x@y.z' }, priority: 5 });
    expect(res.status).toBe(201);
    const job = await res.json();
    expect(job).toMatchObject({ queue: 'emails', name: 'welcome', priority: 5, status: 'waiting' });

    const fetched = await (await fetch(`${base}/jobs/${job.id}`)).json();
    expect(fetched.payload).toEqual({ to: 'x@y.z' });
  });

  it('lists queues with stats', async () => {
    const queues = await (await fetch(`${base}/queues`)).json();
    expect(queues).toContainEqual(expect.objectContaining({ name: 'emails', waiting: 1 }));
  });

  it('filters jobs by status', async () => {
    const res = await fetch(`${base}/queues/emails/jobs?status=waiting`);
    expect((await res.json()).length).toBe(1);
    expect((await fetch(`${base}/queues/emails/jobs?status=bogus`)).status).toBe(400);
  });

  it('validates input', async () => {
    expect((await post('/queues/emails/jobs', { payload: 1 })).status).toBe(400);
    expect((await post('/queues/emails/jobs', { name: 'x', maxAttempts: 0 })).status).toBe(400);
    expect((await post('/queues/bad%20name/jobs', { name: 'x' })).status).toBe(400);
    expect((await fetch(`${base}/jobs/abc`)).status).toBe(400);
    expect((await fetch(`${base}/jobs/99999`)).status).toBe(404);
  });

  it('retries only dead jobs', async () => {
    const job = store.add('bounces', 'bounce', null, { maxAttempts: 1 });
    expect((await post(`/jobs/${job.id}/retry`)).status).toBe(409);
    expect(store.fail(store.claim('bounces', 'w', 1000)!, 'w', 'smtp down')).toBe('dead');
    const res = await post(`/jobs/${job.id}/retry`);
    expect(res.status).toBe(200);
    expect((await res.json()).status).toBe('waiting');
  });
});
