/**
 * ISO 8601 parsing and formatting.
 *
 * Timestamps are carried through the system as integer microseconds since the
 * Unix epoch. Microseconds (not milliseconds) because PostgreSQL's timestamptz
 * stores microseconds, and truncating here would make two distinct rows collide
 * in the pagination cursor. Integers (not Date objects, not floats) because
 * cursor round-trips and ORDER BY tie-breaking must be exact.
 */

/**
 * Shape check for ISO 8601. `Date.parse` alone is far too permissive - it
 * happily accepts "December 17, 1995" and other non-ISO forms - so the regex
 * gates the format and `Date.parse` then validates the actual calendar values
 * (rejecting e.g. 2026-02-30).
 *
 * Group 1: fractional digits. Group 2: timezone designator.
 */
const ISO_RE =
  /^\d{4}-\d{2}-\d{2}[Tt ]\d{2}:\d{2}(?::\d{2}(?:\.(\d{1,9}))?)?([Zz]|[+-]\d{2}:?\d{2})?$/;

const MICROS_PER_MS = 1000;

const DAYS_IN_MONTH = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

function isLeapYear(year: number): boolean {
  return (year % 4 === 0 && year % 100 !== 0) || year % 400 === 0;
}

/**
 * Validates the day-of-month, which `Date.parse` does not.
 *
 * V8 rejects an out-of-range month or hour, but silently *rolls over* a
 * day-of-month overflow: "2026-02-30" parses as 2026-03-02, and "2026-02-29"
 * in a non-leap year becomes 2026-03-01. Left unchecked, an invalid timestamp
 * would be accepted and the entry silently filed under a different day - wrong
 * partition, wrong aggregation bucket, no error anywhere.
 *
 * Digits are read directly by character code rather than via capture groups or
 * `slice`, so this costs no allocation on the ingest path.
 */
function hasValidCalendarDate(raw: string): boolean {
  const year =
    (raw.charCodeAt(0) - 48) * 1000 +
    (raw.charCodeAt(1) - 48) * 100 +
    (raw.charCodeAt(2) - 48) * 10 +
    (raw.charCodeAt(3) - 48);
  const month = (raw.charCodeAt(5) - 48) * 10 + (raw.charCodeAt(6) - 48);
  const day = (raw.charCodeAt(8) - 48) * 10 + (raw.charCodeAt(9) - 48);

  if (month < 1 || month > 12) return false;

  const limit = month === 2 && isLeapYear(year) ? 29 : (DAYS_IN_MONTH[month - 1] as number);
  return day >= 1 && day <= limit;
}

/**
 * Parses an ISO 8601 timestamp into microseconds since the Unix epoch.
 * Returns null when the input is not a valid ISO 8601 instant.
 *
 * A missing timezone designator is interpreted as UTC. ECMAScript would
 * otherwise resolve it against the host timezone, making ingestion results
 * depend on container configuration.
 */
export function parseIsoToMicros(raw: string): number | null {
  const match = ISO_RE.exec(raw);
  if (match === null) return null;

  // The regex guarantees the date occupies positions 0-9, so this can read
  // fixed offsets.
  if (!hasValidCalendarDate(raw)) return null;

  const fraction = match[1];
  const timezone = match[2];

  let normalised = raw;
  let extraMicros = 0;

  // Date.parse resolves only to milliseconds, and V8's handling of fractions
  // that are not exactly three digits is not guaranteed. Normalise to exactly
  // three digits and carry the remainder as microseconds ourselves.
  if (fraction !== undefined && fraction.length !== 3) {
    const dot = raw.indexOf('.');
    const millis = (fraction + '000').slice(0, 3);
    if (fraction.length > 3) {
      extraMicros = Number((fraction + '000000').slice(3, 6));
    }
    normalised = raw.slice(0, dot + 1) + millis + raw.slice(dot + 1 + fraction.length);
  }

  if (timezone === undefined) normalised += 'Z';

  // Accept the space separator permitted by ISO 8601 / RFC 3339 section 5.6.
  if (normalised.charCodeAt(10) === 32 /* ' ' */) {
    normalised = normalised.slice(0, 10) + 'T' + normalised.slice(11);
  }

  const millis = Date.parse(normalised);
  if (Number.isNaN(millis)) return null;

  return millis * MICROS_PER_MS + extraMicros;
}

/**
 * Formats epoch microseconds as the millisecond-precision ISO 8601 string used
 * in API responses, e.g. "2026-07-20T14:32:01.123Z".
 *
 * Responses are deliberately fixed at millisecond precision to match the
 * documented contract. Sub-millisecond precision is preserved in storage and in
 * pagination cursors, where it actually matters.
 */
export function formatMicrosIso(micros: number): string {
  return new Date(Math.floor(micros / MICROS_PER_MS)).toISOString();
}

/**
 * Formats epoch microseconds as a PostgreSQL timestamptz literal, preserving
 * full microsecond precision, e.g. "2026-07-20T14:32:01.123456Z".
 *
 * Query predicates bind this as a text parameter cast to timestamptz, rather
 * than reconstructing the instant with interval arithmetic. The difference is
 * not cosmetic: `TIMESTAMPTZ 'epoch' + $1::bigint * INTERVAL '1 microsecond'`
 * is a *stable* expression, and PostgreSQL will not prune partitions on one.
 * That single detail made every time-bounded query scan all 34 partitions
 * instead of the one or two in range - measured at 1368 ms versus 2 ms.
 */
export function formatMicrosPgLiteral(micros: number): string {
  const millis = Math.floor(micros / MICROS_PER_MS);
  const subMillisMicros = micros - millis * MICROS_PER_MS;
  const base = new Date(millis).toISOString(); // ...sss'Z'
  return `${base.slice(0, -1)}${String(subMillisMicros).padStart(3, '0')}Z`;
}

/** PostgreSQL's timestamp epoch is 2000-01-01, offset from the Unix epoch. */
export const PG_EPOCH_OFFSET_MICROS = 946_684_800_000_000;

export function unixMicrosToPgMicros(micros: number): number {
  return micros - PG_EPOCH_OFFSET_MICROS;
}
