import { createHash } from 'node:crypto';
import type { IncomingMessage } from 'node:http';
import type { Config } from '../config.ts';
import type { QueryPool } from '../db/pool.ts';
import { logger } from '../logger.ts';
import { unauthorized, forbidden } from './errors.ts';

/**
 * Optional API-key authentication.
 *
 * Off by default. The load generator is not told whether a build has auth, and
 * always sends `Authorization: Bearer <key>` on the three data endpoints, so
 * with AUTH_ENABLED=false an unrecognised Authorization header must be ignored
 * rather than rejected. That is the single most important behaviour here: a
 * build that 401s an unknown bearer token while auth is disabled would fail the
 * core contract even though its own tests pass.
 *
 * GET /health is never routed through this class at all.
 */

export type Scope = 'ingest' | 'query';

function hashKey(key: string): string {
  return createHash('sha256').update(key, 'utf8').digest('hex');
}

export class Authenticator {
  private readonly scopesByHash = new Map<string, Set<Scope>>();

  constructor(
    private readonly config: Config,
    private readonly pool: QueryPool,
  ) {}

  get enabled(): boolean {
    return this.config.auth.enabled;
  }

  /**
   * Seeds the configured key and loads the key set.
   *
   * Runs during startup, before the service reports healthy, so the load
   * generator can never observe a window where its key is not yet valid. The
   * upsert makes it idempotent across restarts.
   */
  async start(): Promise<void> {
    if (!this.config.auth.enabled) {
      logger.info('authentication disabled (AUTH_ENABLED=false)');
      return;
    }

    const seedKey = this.config.auth.loadgenApiKey;
    if (seedKey !== null) {
      await this.pool.query(
        `INSERT INTO api_keys (key_hash, name, scopes)
         VALUES ($1, $2, ARRAY['ingest','query'])
         ON CONFLICT (key_hash) DO UPDATE SET scopes = EXCLUDED.scopes, name = EXCLUDED.name`,
        [hashKey(seedKey), 'loadgen'],
      );
      logger.info('seeded LOADGEN_API_KEY with ingest and query scopes');
    } else {
      // Explicitly supported: the service starts and stays healthy, it simply
      // has no seeded key.
      logger.warn('AUTH_ENABLED=true but LOADGEN_API_KEY is unset; no key seeded');
    }

    const result = await this.pool.query<{ key_hash: string; scopes: string[] }>(
      'SELECT key_hash, scopes FROM api_keys',
    );
    for (const row of result.rows) {
      this.scopesByHash.set(row.key_hash, new Set(row.scopes as Scope[]));
    }

    logger.info('authentication enabled', { keys: this.scopesByHash.size });
  }

  /**
   * Authorises a request for the given scope.
   * No-op when authentication is disabled.
   */
  authorize(request: IncomingMessage, scope: Scope): void {
    if (!this.config.auth.enabled) return;

    const credential = extractCredential(request);
    if (credential === null) {
      throw unauthorized('missing or malformed credential; expected Authorization: Bearer <key>');
    }

    const scopes = this.scopesByHash.get(hashKey(credential));
    if (scopes === undefined) {
      throw unauthorized('invalid API key');
    }
    if (!scopes.has(scope)) {
      throw forbidden(`API key lacks the '${scope}' scope`);
    }
  }
}

/**
 * Reads the credential from the Authorization header, falling back to
 * X-API-Key. Credentials are never accepted from the query string or body.
 */
function extractCredential(request: IncomingMessage): string | null {
  const authorization = request.headers.authorization;
  if (typeof authorization === 'string') {
    const separator = authorization.indexOf(' ');
    if (separator > 0) {
      const scheme = authorization.slice(0, separator);
      const value = authorization.slice(separator + 1).trim();
      if (scheme.toLowerCase() === 'bearer' && value.length > 0) return value;
    }
    return null;
  }

  const apiKey = request.headers['x-api-key'];
  if (typeof apiKey === 'string' && apiKey.length > 0) return apiKey;

  return null;
}
