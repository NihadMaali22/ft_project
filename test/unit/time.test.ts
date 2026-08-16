import test from 'node:test';
import assert from 'node:assert/strict';
import { parseIsoToMicros, formatMicrosIso } from '../../src/domain/time.ts';

test('parses the documented timestamp format', () => {
  const micros = parseIsoToMicros('2026-07-20T14:32:01.123Z');
  assert.equal(micros, Date.parse('2026-07-20T14:32:01.123Z') * 1000);
});

test('preserves microsecond precision beyond what Date.parse resolves', () => {
  const micros = parseIsoToMicros('2026-07-20T14:32:01.123456Z');
  assert.equal(micros, Date.parse('2026-07-20T14:32:01.123Z') * 1000 + 456);
});

test('truncates rather than rounds sub-microsecond digits', () => {
  const micros = parseIsoToMicros('2026-07-20T14:32:01.123456789Z');
  assert.equal(micros, Date.parse('2026-07-20T14:32:01.123Z') * 1000 + 456);
});

test('normalises fractions shorter than three digits', () => {
  assert.equal(
    parseIsoToMicros('2026-07-20T14:32:01.1Z'),
    Date.parse('2026-07-20T14:32:01.100Z') * 1000,
  );
  assert.equal(
    parseIsoToMicros('2026-07-20T14:32:01.12Z'),
    Date.parse('2026-07-20T14:32:01.120Z') * 1000,
  );
});

test('honours explicit UTC offsets', () => {
  assert.equal(
    parseIsoToMicros('2026-07-20T16:32:01+02:00'),
    parseIsoToMicros('2026-07-20T14:32:01Z'),
  );
  assert.equal(
    parseIsoToMicros('2026-07-20T16:32:01+0200'),
    parseIsoToMicros('2026-07-20T14:32:01Z'),
  );
});

test('treats a missing timezone as UTC rather than host-local time', () => {
  // Guards against ingestion results depending on container TZ configuration.
  assert.equal(
    parseIsoToMicros('2026-07-20T14:32:01'),
    parseIsoToMicros('2026-07-20T14:32:01Z'),
  );
});

test('accepts the space separator permitted by RFC 3339', () => {
  assert.equal(
    parseIsoToMicros('2026-07-20 14:32:01Z'),
    parseIsoToMicros('2026-07-20T14:32:01Z'),
  );
});

test('accepts a timestamp without seconds', () => {
  assert.equal(
    parseIsoToMicros('2026-07-20T14:32Z'),
    Date.parse('2026-07-20T14:32:00Z') * 1000,
  );
});

test('rejects non-ISO formats that Date.parse would otherwise accept', () => {
  // The whole reason the regex exists: Date.parse is far too permissive.
  assert.equal(parseIsoToMicros('December 17, 1995 03:24:00'), null);
  assert.equal(parseIsoToMicros('Mon, 20 Jul 2026 14:32:01 GMT'), null);
  assert.equal(parseIsoToMicros('2026/07/20 14:32:01'), null);
});

test('rejects impossible calendar dates', () => {
  assert.equal(parseIsoToMicros('2026-02-30T00:00:00Z'), null);
  assert.equal(parseIsoToMicros('2026-13-01T00:00:00Z'), null);
  assert.equal(parseIsoToMicros('2026-00-10T00:00:00Z'), null);
  assert.equal(parseIsoToMicros('2026-07-20T25:00:00Z'), null);
});

test('accepts a genuine leap day and rejects a non-leap one', () => {
  assert.notEqual(parseIsoToMicros('2028-02-29T00:00:00Z'), null);
  assert.equal(parseIsoToMicros('2026-02-29T00:00:00Z'), null);
});

test('rejects empty and malformed input', () => {
  assert.equal(parseIsoToMicros(''), null);
  assert.equal(parseIsoToMicros('not-a-timestamp'), null);
  assert.equal(parseIsoToMicros('2026-07-20'), null);
});

test('formats back to the documented response shape', () => {
  const iso = '2026-07-20T14:32:01.123Z';
  assert.equal(formatMicrosIso(parseIsoToMicros(iso) as number), iso);
});

test('round-trips timestamps before the Unix epoch', () => {
  const iso = '1969-07-20T20:17:40.000Z';
  const micros = parseIsoToMicros(iso) as number;
  assert.ok(micros < 0);
  assert.equal(formatMicrosIso(micros), iso);
});
