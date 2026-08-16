import test from 'node:test';
import assert from 'node:assert/strict';
import { encodeCursor, decodeCursor } from '../../src/domain/cursor.ts';

test('round-trips a cursor', () => {
  const cursor = { tsMicros: 1_784_305_921_123_456, id: '9007199254740991' };
  assert.deepEqual(decodeCursor(encodeCursor(cursor)), cursor);
});

test('produces a URL-safe token', () => {
  const encoded = encodeCursor({ tsMicros: 1_784_305_921_123_456, id: '12345' });
  assert.equal(encoded, encodeURIComponent(encoded));
});

test('round-trips through a query string unchanged', () => {
  const cursor = { tsMicros: -1_000_000, id: '1' };
  const encoded = encodeCursor(cursor);
  const parsed = new URLSearchParams(`cursor=${encoded}`).get('cursor');
  assert.deepEqual(decodeCursor(parsed as string), cursor);
});

test('rejects malformed cursors rather than misreading them', () => {
  assert.equal(decodeCursor(''), null);
  assert.equal(decodeCursor('!!!!'), null);
  assert.equal(decodeCursor('not-base64-$$$'), null);
  assert.equal(decodeCursor('a'.repeat(600)), null);
});

test('rejects a token whose payload is not the expected shape', () => {
  const encode = (value: unknown): string =>
    Buffer.from(JSON.stringify(value), 'utf8').toString('base64url');

  assert.equal(decodeCursor(encode({ v: 1, t: 'not-a-number', i: '1' })), null);
  assert.equal(decodeCursor(encode({ v: 1, t: 1000, i: 5 })), null);
  assert.equal(decodeCursor(encode({ v: 1, t: 1000, i: 'abc' })), null);
  assert.equal(decodeCursor(encode({ v: 1, t: 1.5, i: '1' })), null);
  assert.equal(decodeCursor(encode({ t: 1000, i: '1' })), null);
  assert.equal(decodeCursor(encode([1, 2, 3])), null);
  assert.equal(decodeCursor(encode('a string')), null);
});

test('rejects a cursor from an incompatible version', () => {
  const encoded = Buffer.from(JSON.stringify({ v: 99, t: 1000, i: '1' }), 'utf8').toString(
    'base64url',
  );
  assert.equal(decodeCursor(encoded), null);
});

test('rejects base64 that does not decode to JSON', () => {
  assert.equal(decodeCursor(Buffer.from('not json', 'utf8').toString('base64url')), null);
});
