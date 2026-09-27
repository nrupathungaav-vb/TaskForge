import type { BackoffStrategy } from './types.js';

export const DEFAULT_BACKOFF: BackoffStrategy = { type: 'exponential', delayMs: 1000, maxDelayMs: 60_000, jitter: true };

/**
 * Delay before retry number `attempt` (1 = the first retry).
 * Exponential backoff uses "full jitter" (random in [0, cap]) to avoid thundering herds
 * when many jobs fail at the same moment.
 */
export function computeBackoff(strategy: BackoffStrategy, attempt: number, random: () => number = Math.random): number {
  if (strategy.type === 'fixed') return strategy.delayMs;
  const cap = Math.min(strategy.delayMs * 2 ** (attempt - 1), strategy.maxDelayMs ?? Number.POSITIVE_INFINITY);
  return strategy.jitter ? Math.floor(random() * cap) : cap;
}
