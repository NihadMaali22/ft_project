/**
 * Minimal structured logger.
 *
 * Deliberately not a library: nothing on the ingest hot path logs per request,
 * so this only needs to be correct and cheap at startup and on rare events.
 */

type Level = 'debug' | 'info' | 'warn' | 'error';

const LEVEL_ORDER: Record<Level, number> = { debug: 10, info: 20, warn: 30, error: 40 };

const threshold = LEVEL_ORDER[(process.env.LOG_LEVEL as Level) ?? 'info'] ?? LEVEL_ORDER.info;

function emit(level: Level, message: string, fields?: Record<string, unknown>): void {
  if (LEVEL_ORDER[level] < threshold) return;
  const record = {
    ts: new Date().toISOString(),
    level,
    message,
    ...fields,
  };
  const line = JSON.stringify(record, (_key, value) =>
    value instanceof Error ? { name: value.name, message: value.message, stack: value.stack } : value,
  );
  if (level === 'error' || level === 'warn') process.stderr.write(line + '\n');
  else process.stdout.write(line + '\n');
}

export const logger = {
  debug: (message: string, fields?: Record<string, unknown>) => emit('debug', message, fields),
  info: (message: string, fields?: Record<string, unknown>) => emit('info', message, fields),
  warn: (message: string, fields?: Record<string, unknown>) => emit('warn', message, fields),
  error: (message: string, fields?: Record<string, unknown>) => emit('error', message, fields),
};
