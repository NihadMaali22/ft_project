/**
 * Opaque keyset pagination cursor.
 *
 * Keyset (not OFFSET) pagination: OFFSET makes PostgreSQL walk and discard every
 * skipped row, so page 10,000 costs 10,000x page 1. A cursor carrying the last
 * row's (timestamp, id) turns every page into the same bounded index range scan
 * against the (ts DESC, id DESC) index.
 *
 * The pair is what makes the ordering deterministic when timestamps collide:
 * timestamp alone is not unique, so a tie at a page boundary would otherwise
 * drop or duplicate rows.
 */

export interface Cursor {
  /** Microseconds since the Unix epoch. */
  tsMicros: number;
  /** Row id, carried as a string so bigint precision never depends on float64. */
  id: string;
}

const CURSOR_VERSION = 1;

/** Bounds work done on hostile input before anything is parsed. */
const MAX_CURSOR_LENGTH = 512;

const BASE64URL_RE = /^[A-Za-z0-9_-]+$/;
const DIGITS_RE = /^\d{1,19}$/;

interface CursorPayload {
  v: number;
  t: number;
  i: string;
}

export function encodeCursor(cursor: Cursor): string {
  const payload: CursorPayload = { v: CURSOR_VERSION, t: cursor.tsMicros, i: cursor.id };
  return Buffer.from(JSON.stringify(payload), 'utf8').toString('base64url');
}

/**
 * Decodes a cursor, returning null for anything malformed.
 *
 * Node's base64url decoder silently skips characters outside the alphabet, so
 * the charset is checked explicitly first; otherwise "!!!!" would decode to an
 * empty buffer rather than being recognised as invalid.
 */
export function decodeCursor(raw: string): Cursor | null {
  if (raw.length === 0 || raw.length > MAX_CURSOR_LENGTH) return null;
  if (!BASE64URL_RE.test(raw)) return null;

  let decoded: string;
  try {
    decoded = Buffer.from(raw, 'base64url').toString('utf8');
  } catch {
    return null;
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(decoded);
  } catch {
    return null;
  }

  if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) return null;

  const payload = parsed as Partial<CursorPayload>;
  if (payload.v !== CURSOR_VERSION) return null;
  if (typeof payload.t !== 'number' || !Number.isSafeInteger(payload.t)) return null;
  if (typeof payload.i !== 'string' || !DIGITS_RE.test(payload.i)) return null;

  return { tsMicros: payload.t, id: payload.i };
}
