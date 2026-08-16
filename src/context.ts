import type { Config } from './config.ts';
import type { QueryPool } from './db/pool.ts';
import type { ServiceDictionary } from './db/services.ts';
import type { IngestWriter } from './ingest/writer.ts';
import type { Authenticator } from './http/auth.ts';
import type { RateLimiter } from './http/rate-limit.ts';
import type { RetentionJanitor } from './retention/janitor.ts';

/**
 * Dependencies shared by request handlers.
 *
 * Passed explicitly rather than reached for through module-level singletons, so
 * handlers can be exercised in tests against fakes.
 */
export interface AppContext {
  readonly config: Config;
  readonly pool: QueryPool;
  readonly services: ServiceDictionary;
  readonly writer: IngestWriter;
  readonly auth: Authenticator;
  readonly rateLimiter: RateLimiter;
  readonly janitor: RetentionJanitor;
  /** Flipped true only after migrations, seeding and writer startup complete. */
  ready: boolean;
}
