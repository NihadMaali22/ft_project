import test from 'node:test';
import assert from 'node:assert/strict';
import { validateEntry, isRejection } from '../../src/domain/validate.ts';
import type { ValidLogRow } from '../../src/domain/types.ts';

const NOW = Date.parse('2026-07-20T14:32:01.000Z');

function valid(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    timestamp: '2026-07-20T14:32:01.123Z',
    level: 'error',
    service: 'checkout',
    message: 'payment declined',
    attributes: { user_id: '42', region: 'eu-west', retries: 3 },
    ...overrides,
  };
}

function accept(entry: unknown): ValidLogRow {
  const result = validateEntry(entry, NOW);
  assert.ok(!isRejection(result), `expected acceptance, got: ${String(result)}`);
  return result;
}

function reject(entry: unknown): string {
  const result = validateEntry(entry, NOW);
  assert.ok(isRejection(result), 'expected rejection');
  return result;
}

test('accepts the documented example entry', () => {
  const row = accept(valid());
  assert.equal(row.levelCode, 3);
  assert.equal(row.service, 'checkout');
  assert.equal(row.message, 'payment declined');
  assert.deepEqual(JSON.parse(row.attributesJson), {
    user_id: '42',
    region: 'eu-west',
    retries: 3,
  });
});

test('accepts an entry with no attributes', () => {
  const row = accept(valid({ attributes: undefined }));
  assert.equal(row.attributesJson, '{}');
});

test('accepts all four levels and encodes them in order', () => {
  assert.equal(accept(valid({ level: 'debug' })).levelCode, 0);
  assert.equal(accept(valid({ level: 'info' })).levelCode, 1);
  assert.equal(accept(valid({ level: 'warn' })).levelCode, 2);
  assert.equal(accept(valid({ level: 'error' })).levelCode, 3);
});

test('rejects an unsupported level with the documented reason format', () => {
  assert.equal(reject(valid({ level: 'critical' })), "invalid level: 'critical'");
});

test('level matching is case sensitive', () => {
  assert.match(reject(valid({ level: 'ERROR' })), /invalid level/);
});

test('rejects missing required fields', () => {
  assert.match(reject(valid({ timestamp: undefined })), /timestamp is required/);
  assert.match(reject(valid({ level: undefined })), /level is required/);
  assert.match(reject(valid({ service: undefined })), /service is required/);
  assert.match(reject(valid({ message: undefined })), /message is required/);
});

test('rejects empty service and message', () => {
  assert.match(reject(valid({ service: '' })), /service must be a non-empty string/);
  assert.match(reject(valid({ message: '' })), /message must be a non-empty string/);
});

test('rejects wrong types for required fields', () => {
  assert.match(reject(valid({ service: 42 })), /service must be a non-empty string/);
  assert.match(reject(valid({ message: { text: 'hi' } })), /message must be a non-empty string/);
  assert.match(reject(valid({ timestamp: 1_700_000_000 })), /timestamp must be an ISO 8601 string/);
});

test('rejects a timestamp more than five minutes in the future', () => {
  const future = new Date(NOW + 5 * 60_000 + 1000).toISOString();
  assert.match(reject(valid({ timestamp: future })), /five minutes in the future/);
});

test('accepts a timestamp just inside the five minute future allowance', () => {
  const nearFuture = new Date(NOW + 4 * 60_000).toISOString();
  accept(valid({ timestamp: nearFuture }));
});

test('accepts arbitrarily old timestamps', () => {
  accept(valid({ timestamp: '2001-01-01T00:00:00Z' }));
});

test('accepts scalar attribute values of every permitted type', () => {
  const row = accept(
    valid({ attributes: { text: 'a', number: 3.5, negative: -1, flag: true, off: false } }),
  );
  assert.deepEqual(JSON.parse(row.attributesJson), {
    text: 'a',
    number: 3.5,
    negative: -1,
    flag: true,
    off: false,
  });
});

test('rejects nested objects and arrays in attributes', () => {
  assert.match(reject(valid({ attributes: { nested: { a: 1 } } })), /nested objects and arrays/);
  assert.match(reject(valid({ attributes: { list: [1, 2] } })), /nested objects and arrays/);
});

test('rejects null attribute values', () => {
  // null is not among the permitted scalar types.
  assert.match(reject(valid({ attributes: { missing: null } })), /must be a string, number/);
});

test('rejects attributes that are not a flat object', () => {
  assert.match(reject(valid({ attributes: [1, 2, 3] })), /attributes must be a flat object/);
  assert.match(reject(valid({ attributes: 'user_id=42' })), /attributes must be a flat object/);
});

test('rejects non-object entries', () => {
  assert.match(reject(null), /entry must be a JSON object/);
  assert.match(reject('a log line'), /entry must be a JSON object/);
  assert.match(reject([valid()]), /entry must be a JSON object/);
});

test('reports the first failing rule only, so reasons stay actionable', () => {
  const reason = reject({ timestamp: 'nope', level: 'nope', service: '', message: '' });
  assert.match(reason, /invalid timestamp/);
});

test('preserves attribute value types for storage while remaining string-comparable', () => {
  // The contract compares attributes as strings but the stored value keeps its
  // JSON type, so responses round-trip faithfully.
  const row = accept(valid({ attributes: { retries: 3 } }));
  assert.equal(row.attributesJson, '{"retries":3}');
});
