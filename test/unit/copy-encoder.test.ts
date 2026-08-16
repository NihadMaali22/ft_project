import test from 'node:test';
import assert from 'node:assert/strict';
import { CopyBinaryEncoder } from '../../src/db/copy-encoder.ts';
import { PG_EPOCH_OFFSET_MICROS } from '../../src/domain/time.ts';

/**
 * The binary COPY layout is verified byte by byte here.
 *
 * A framing bug in this encoder surfaces as an opaque "insufficient data left
 * in message" from the server with no indication of which field is wrong, so it
 * is worth pinning the exact offsets rather than only testing round trips.
 */

const HEADER_LENGTH = 19;

test('writes the PGCOPY header signature', () => {
  const encoder = new CopyBinaryEncoder();
  const payload = encoder.finish();

  assert.equal(payload.subarray(0, 11).toString('latin1'), 'PGCOPY\n\xff\r\n\0');
  // Flags and header extension length, both zero.
  assert.equal(payload.readInt32BE(11), 0);
  assert.equal(payload.readInt32BE(15), 0);
});

test('an empty payload is just the header and trailer', () => {
  const payload = new CopyBinaryEncoder().finish();
  assert.equal(payload.length, HEADER_LENGTH + 2);
  assert.equal(payload.readInt16BE(HEADER_LENGTH), -1);
});

test('encodes one row with the exact expected field framing', () => {
  const encoder = new CopyBinaryEncoder();
  // Unix micros chosen so the PostgreSQL-epoch value is exactly zero.
  encoder.writeLogRow(1, PG_EPOCH_OFFSET_MICROS, 7, 3, 'hi', '{"a":1}');
  const payload = encoder.finish();

  let offset = HEADER_LENGTH;

  assert.equal(payload.readInt16BE(offset), 6, 'field count');
  offset += 2;

  assert.equal(payload.readInt32BE(offset), 8, 'id length');
  assert.equal(payload.readBigInt64BE(offset + 4), 1n, 'id value');
  offset += 12;

  assert.equal(payload.readInt32BE(offset), 8, 'ts length');
  assert.equal(payload.readBigInt64BE(offset + 4), 0n, 'ts at the PostgreSQL epoch');
  offset += 12;

  assert.equal(payload.readInt32BE(offset), 4, 'service_id length');
  assert.equal(payload.readInt32BE(offset + 4), 7, 'service_id value');
  offset += 8;

  assert.equal(payload.readInt32BE(offset), 2, 'level length');
  assert.equal(payload.readInt16BE(offset + 4), 3, 'level value');
  offset += 6;

  assert.equal(payload.readInt32BE(offset), 2, 'message length');
  assert.equal(payload.subarray(offset + 4, offset + 6).toString('utf8'), 'hi');
  offset += 6;

  // jsonb carries a one-byte version prefix, so the declared length is the
  // JSON text length plus one.
  assert.equal(payload.readInt32BE(offset), 8, 'attributes length');
  assert.equal(payload[offset + 4], 0x01, 'jsonb version byte');
  assert.equal(payload.subarray(offset + 5, offset + 12).toString('utf8'), '{"a":1}');
  offset += 12;

  assert.equal(payload.readInt16BE(offset), -1, 'trailer');
  assert.equal(offset + 2, payload.length);
});

test('encodes timestamps before the PostgreSQL epoch as negative values', () => {
  const encoder = new CopyBinaryEncoder();
  // 1970-01-01, i.e. 30 years before the PostgreSQL epoch.
  encoder.writeLogRow(1, 0, 1, 0, 'x', '{}');
  const payload = encoder.finish();

  assert.equal(payload.readBigInt64BE(HEADER_LENGTH + 2 + 12 + 4), BigInt(-PG_EPOCH_OFFSET_MICROS));
});

test('encodes ids beyond the 32-bit range', () => {
  const encoder = new CopyBinaryEncoder();
  const largeId = 9_007_199_254_740_991; // Number.MAX_SAFE_INTEGER
  encoder.writeLogRow(largeId, PG_EPOCH_OFFSET_MICROS, 1, 0, 'x', '{}');
  const payload = encoder.finish();

  assert.equal(payload.readBigInt64BE(HEADER_LENGTH + 2 + 4), BigInt(largeId));
});

test('declares string lengths in bytes, not characters', () => {
  const encoder = new CopyBinaryEncoder();
  // 4 characters, 9 UTF-8 bytes: a 3-byte CJK char, a 4-byte emoji as a
  // surrogate pair, and one ASCII char.
  const message = '你\u{1F600}!';
  encoder.writeLogRow(1, PG_EPOCH_OFFSET_MICROS, 1, 0, message, '{}');
  const payload = encoder.finish();

  const messageLengthOffset = HEADER_LENGTH + 2 + 12 + 12 + 8 + 6;
  const declared = payload.readInt32BE(messageLengthOffset);

  assert.equal(declared, Buffer.byteLength(message, 'utf8'));
  assert.equal(
    payload.subarray(messageLengthOffset + 4, messageLengthOffset + 4 + declared).toString('utf8'),
    message,
  );
});

test('grows beyond its initial capacity without corrupting earlier rows', () => {
  // Starts far too small so the growth path is exercised repeatedly.
  const encoder = new CopyBinaryEncoder(64);
  const rowCount = 500;

  for (let i = 0; i < rowCount; i++) {
    encoder.writeLogRow(i, PG_EPOCH_OFFSET_MICROS + i, 1, i % 4, `message ${i}`, '{"k":"v"}');
  }

  assert.equal(encoder.rows, rowCount);
  const payload = encoder.finish();

  // Walk the whole payload and confirm every row still frames correctly.
  let offset = HEADER_LENGTH;
  for (let i = 0; i < rowCount; i++) {
    assert.equal(payload.readInt16BE(offset), 6, `row ${i} field count`);
    offset += 2;
    assert.equal(payload.readBigInt64BE(offset + 4), BigInt(i), `row ${i} id`);
    offset += 12 + 12 + 8 + 6;
    const messageLength = payload.readInt32BE(offset);
    assert.equal(
      payload.subarray(offset + 4, offset + 4 + messageLength).toString('utf8'),
      `message ${i}`,
    );
    offset += 4 + messageLength;
    const attributesLength = payload.readInt32BE(offset);
    offset += 4 + attributesLength;
  }
  assert.equal(payload.readInt16BE(offset), -1);
});

test('reset clears rows and rewrites the header', () => {
  const encoder = new CopyBinaryEncoder();
  encoder.writeLogRow(1, PG_EPOCH_OFFSET_MICROS, 1, 0, 'first', '{}');
  encoder.reset();
  encoder.writeLogRow(2, PG_EPOCH_OFFSET_MICROS, 1, 0, 'second', '{}');

  assert.equal(encoder.rows, 1);
  const payload = encoder.finish();
  assert.equal(payload.readBigInt64BE(HEADER_LENGTH + 2 + 4), 2n);
  assert.ok(!payload.toString('utf8').includes('first'));
});

test('releases capacity grown by an oversized batch', () => {
  const encoder = new CopyBinaryEncoder(1024);
  const large = 'x'.repeat(100_000);
  for (let i = 0; i < 20; i++) {
    encoder.writeLogRow(i, PG_EPOCH_OFFSET_MICROS, 1, 0, large, '{}');
  }
  encoder.finish();
  assert.ok(encoder.byteLength > 1_000_000);

  encoder.shrinkIfOversized(64 * 1024);

  // Back to an empty header-only buffer at the original capacity.
  assert.equal(encoder.rows, 0);
  assert.equal(encoder.byteLength, HEADER_LENGTH);
});
