/**
 * Log levels are stored as smallint rather than text.
 *
 * Four values never justify 5-9 bytes per row plus text comparison on every
 * filter and GROUP BY. Encoding happens once at ingest and decoding once per
 * returned row, both via switch statements that V8 compiles to jump tables.
 */

export const LEVELS = ['debug', 'info', 'warn', 'error'] as const;

export type Level = (typeof LEVELS)[number];

/** Sentinel for "not a recognised level", avoiding an allocation on the hot path. */
export const INVALID_LEVEL = -1;

export function levelToCode(value: string): number {
  switch (value) {
    case 'debug':
      return 0;
    case 'info':
      return 1;
    case 'warn':
      return 2;
    case 'error':
      return 3;
    default:
      return INVALID_LEVEL;
  }
}

export function codeToLevel(code: number): Level {
  switch (code) {
    case 0:
      return 'debug';
    case 1:
      return 'info';
    case 2:
      return 'warn';
    case 3:
      return 'error';
    default:
      // Unreachable for rows this service wrote; treated as data corruption.
      throw new Error(`Unknown level code: ${code}`);
  }
}

export function isLevel(value: string): value is Level {
  return levelToCode(value) !== INVALID_LEVEL;
}
