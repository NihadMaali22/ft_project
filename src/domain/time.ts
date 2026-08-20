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
  const len = raw.length;
  // Fast-path for common ISO UTC formats:
  // "YYYY-MM-DDTHH:mm:ss.sssZ" (24) or "YYYY-MM-DDTHH:mm:ssZ" (20) or "YYYY-MM-DDTHH:mm:ss.uuuuuuZ" (27)
  if (
    (len === 24 || len === 20 || len === 27) &&
    raw.charCodeAt(4) === 45 && // '-'
    raw.charCodeAt(7) === 45 && // '-'
    raw.charCodeAt(13) === 58 && // ':'
    raw.charCodeAt(16) === 58 // ':'
  ) {
    const sep = raw.charCodeAt(10);
    const tz = raw.charCodeAt(len - 1);
    if ((sep === 84 || sep === 116 || sep === 32) && (tz === 90 || tz === 122)) {
      const c0 = raw.charCodeAt(0) - 48;
      const c1 = raw.charCodeAt(1) - 48;
      const c2 = raw.charCodeAt(2) - 48;
      const c3 = raw.charCodeAt(3) - 48;
      const c5 = raw.charCodeAt(5) - 48;
      const c6 = raw.charCodeAt(6) - 48;
      const c8 = raw.charCodeAt(8) - 48;
      const c9 = raw.charCodeAt(9) - 48;
      const c11 = raw.charCodeAt(11) - 48;
      const c12 = raw.charCodeAt(12) - 48;
      const c14 = raw.charCodeAt(14) - 48;
      const c15 = raw.charCodeAt(15) - 48;
      const c17 = raw.charCodeAt(17) - 48;
      const c18 = raw.charCodeAt(18) - 48;

      if (
        c0 >= 0 && c0 <= 9 && c1 >= 0 && c1 <= 9 && c2 >= 0 && c2 <= 9 && c3 >= 0 && c3 <= 9 &&
        c5 >= 0 && c5 <= 9 && c6 >= 0 && c6 <= 9 && c8 >= 0 && c8 <= 9 && c9 >= 0 && c9 <= 9 &&
        c11 >= 0 && c11 <= 9 && c12 >= 0 && c12 <= 9 && c14 >= 0 && c14 <= 9 && c15 >= 0 && c15 <= 9 &&
        c17 >= 0 && c17 <= 9 && c18 >= 0 && c18 <= 9
      ) {
        const year = c0 * 1000 + c1 * 100 + c2 * 10 + c3;
        const month = c5 * 10 + c6;
        const day = c8 * 10 + c9;
        const hour = c11 * 10 + c12;
        const minute = c14 * 10 + c15;
        const second = c17 * 10 + c18;

        if (month >= 1 && month <= 12 && hour <= 23 && minute <= 59 && second <= 59) {
          const maxDay = month === 2 && isLeapYear(year) ? 29 : (DAYS_IN_MONTH[month - 1] as number);
          if (day >= 1 && day <= maxDay) {
            let millis = 0;
            let extraMicros = 0;

            if (len === 20) {
              return Date.UTC(year, month - 1, day, hour, minute, second, 0) * MICROS_PER_MS;
            }

            if (raw.charCodeAt(19) === 46) { // '.'
              const m0 = raw.charCodeAt(20) - 48;
              const m1 = raw.charCodeAt(21) - 48;
              const m2 = raw.charCodeAt(22) - 48;
              if (m0 >= 0 && m0 <= 9 && m1 >= 0 && m1 <= 9 && m2 >= 0 && m2 <= 9) {
                millis = m0 * 100 + m1 * 10 + m2;
                if (len === 24) {
                  return Date.UTC(year, month - 1, day, hour, minute, second, millis) * MICROS_PER_MS;
                }
                if (len === 27) {
                  const u0 = raw.charCodeAt(23) - 48;
                  const u1 = raw.charCodeAt(24) - 48;
                  const u2 = raw.charCodeAt(25) - 48;
                  if (u0 >= 0 && u0 <= 9 && u1 >= 0 && u1 <= 9 && u2 >= 0 && u2 <= 9) {
                    extraMicros = u0 * 100 + u1 * 10 + u2;
                    return Date.UTC(year, month - 1, day, hour, minute, second, millis) * MICROS_PER_MS + extraMicros;
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  // General fallback for timezone offsets, variable-length sub-seconds, etc.
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
