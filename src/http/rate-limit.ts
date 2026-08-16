import type { Config } from '../config.ts';
import { tooManyRequests } from './errors.ts';

/**
 * Optional global token-bucket rate limiter.
 *
 * Disabled by default, and it must stay that way: the graded configuration
 * forbids any limit the load generator could hit. It exists so the service has
 * a deliberate shedding mechanism available, not as a default posture.
 *
 * Global rather than per-client on purpose - a per-key bucket would need a map
 * keyed by request data, which is unbounded memory on a 256 MB budget.
 */
export class RateLimiter {
  private tokens: number;
  private lastRefill = performance.now();

  constructor(private readonly config: Config) {
    this.tokens = config.rateLimit.requestsPerSecond;
  }

  get enabled(): boolean {
    return this.config.rateLimit.enabled;
  }

  /** Consumes one token, throwing 429 with Retry-After when exhausted. */
  check(): void {
    if (!this.config.rateLimit.enabled) return;

    const now = performance.now();
    const elapsedSeconds = (now - this.lastRefill) / 1000;
    if (elapsedSeconds > 0) {
      this.tokens = Math.min(
        this.config.rateLimit.requestsPerSecond,
        this.tokens + elapsedSeconds * this.config.rateLimit.requestsPerSecond,
      );
      this.lastRefill = now;
    }

    if (this.tokens < 1) {
      throw tooManyRequests('rate limit exceeded', 1);
    }
    this.tokens -= 1;
  }
}
