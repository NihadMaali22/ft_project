import { PG_EPOCH_OFFSET_MICROS } from '../domain/time.ts';

/**
 * Encoder for PostgreSQL's binary COPY format.
 *
 * Binary rather than text COPY for two reasons. On the client side, text COPY
 * requires escaping backslashes, tabs and newlines, which is a per-character
 * scan in JavaScript; binary writes go straight through Buffer's native paths.
 * On the server side, binary skips input parsing for every fixed-width column,
 * which matters when PostgreSQL has a single core to spend.
 *
 * Rows are encoded as they arrive rather than buffered as objects and encoded
 * at flush time. That keeps thousands of short-lived JS objects out of the
 * young generation, spreads encoding cost across request handling instead of
 * spiking it at the flush boundary, and - most usefully - means the exact
 * on-wire byte size of the pending batch is known at all times, so backpressure
 * can be applied on bytes rather than on a guess.
 *
 * Format reference: PostgreSQL manual, COPY, "Binary Format".
 */

/** "PGCOPY\n\377\r\n\0", then flags (int32 0), then header extension (int32 0). */
const COPY_SIGNATURE = Buffer.from([
  0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00,
]);

const HEADER_BYTES = COPY_SIGNATURE.length;

/** File trailer: a field count of -1. */
const TRAILER_BYTES = 2;

const FIELD_COUNT = 6;

/**
 * UTF-8 upper bound per UTF-16 code unit.
 *
 * A BMP code unit encodes to at most 3 bytes; a surrogate pair encodes to 4
 * bytes across 2 units, i.e. 2 per unit; an unpaired surrogate becomes U+FFFD
 * at 3 bytes. So 3x the string length is a strict bound, and reserving it lets
 * the string be written in a single pass without a preceding byteLength scan.
 */
const MAX_UTF8_BYTES_PER_CHAR = 3;

const JSONB_VERSION_BYTE = 0x01;

/** Writes a 64-bit big-endian integer without going through BigInt. */
function writeInt64BE(buffer: Buffer, offset: number, value: number): void {
  // Math.floor rather than a bit shift: bitwise operators truncate to 32 bits.
  // For negative values this still yields the correct two's-complement pair,
  // because the low word is computed as a non-negative remainder.
  const high = Math.floor(value / 4_294_967_296);
  const low = value - high * 4_294_967_296;
  buffer.writeInt32BE(high, offset);
  buffer.writeUInt32BE(low, offset + 4);
}

export class CopyBinaryEncoder {
  private buffer: Buffer;
  private offset = 0;
  private rowCount = 0;

  constructor(private readonly initialCapacity = 512 * 1024) {
    this.buffer = Buffer.allocUnsafe(initialCapacity);
    this.reset();
  }

  /** Payload bytes written so far, including the header. */
  get byteLength(): number {
    return this.offset;
  }

  get rows(): number {
    return this.rowCount;
  }

  /** Clears the encoder and re-writes the COPY header. */
  reset(): void {
    COPY_SIGNATURE.copy(this.buffer, 0);
    this.offset = HEADER_BYTES;
    this.rowCount = 0;
  }

  private ensureCapacity(additional: number): void {
    const required = this.offset + additional;
    if (required <= this.buffer.length) return;

    let capacity = this.buffer.length * 2;
    while (capacity < required) capacity *= 2;

    const grown = Buffer.allocUnsafe(capacity);
    this.buffer.copy(grown, 0, 0, this.offset);
    this.buffer = grown;
  }

  /**
   * Appends one row in column order: id, ts, service_id, level, message,
   * attributes.
   */
  writeLogRow(
    id: number,
    timestampMicros: number,
    serviceId: number,
    levelCode: number,
    message: string,
    attributesJson: string,
  ): void {
    // Worst-case size for this row, reserved once so no bounds check is needed
    // between individual field writes.
    const reserve =
      2 + // field count
      12 + // id: 4-byte length + 8-byte value
      12 + // ts: 4-byte length + 8-byte value
      8 + // service_id: 4-byte length + 4-byte value
      6 + // level: 4-byte length + 2-byte value
      4 +
      message.length * MAX_UTF8_BYTES_PER_CHAR +
      4 +
      1 + // jsonb version byte
      attributesJson.length * MAX_UTF8_BYTES_PER_CHAR;

    this.ensureCapacity(reserve);

    const buffer = this.buffer;
    let offset = this.offset;

    buffer.writeInt16BE(FIELD_COUNT, offset);
    offset += 2;

    // id :: bigint
    buffer.writeInt32BE(8, offset);
    writeInt64BE(buffer, offset + 4, id);
    offset += 12;

    // ts :: timestamptz, stored as microseconds since 2000-01-01 UTC
    buffer.writeInt32BE(8, offset);
    writeInt64BE(buffer, offset + 4, timestampMicros - PG_EPOCH_OFFSET_MICROS);
    offset += 12;

    // service_id :: integer
    buffer.writeInt32BE(4, offset);
    buffer.writeInt32BE(serviceId, offset + 4);
    offset += 8;

    // level :: smallint
    buffer.writeInt32BE(2, offset);
    buffer.writeInt16BE(levelCode, offset + 4);
    offset += 6;

    // message :: text -- length is backfilled once the byte count is known
    const messageLengthOffset = offset;
    offset += 4;
    const messageBytes = buffer.write(message, offset, 'utf8');
    buffer.writeInt32BE(messageBytes, messageLengthOffset);
    offset += messageBytes;

    // attributes :: jsonb -- a version byte followed by the JSON text
    const attributesLengthOffset = offset;
    offset += 4;
    buffer[offset] = JSONB_VERSION_BYTE;
    offset += 1;
    const attributeBytes = buffer.write(attributesJson, offset, 'utf8');
    buffer.writeInt32BE(attributeBytes + 1, attributesLengthOffset);
    offset += attributeBytes;

    this.offset = offset;
    this.rowCount += 1;
  }

  /**
   * Appends the trailer and returns the payload.
   *
   * The returned buffer is a view over internal storage, so the caller must
   * finish writing it before the encoder is reset or reused.
   */
  finish(): Buffer {
    this.ensureCapacity(TRAILER_BYTES);
    this.buffer.writeInt16BE(-1, this.offset);
    this.offset += TRAILER_BYTES;
    return this.buffer.subarray(0, this.offset);
  }

  /**
   * Releases capacity grown during a traffic spike.
   *
   * Buffers are off-heap, so V8's heap limit does not police them; left
   * unchecked, one oversized batch would permanently pin memory the 256 MB
   * container cannot spare.
   */
  shrinkIfOversized(maxRetainedBytes: number): void {
    if (this.buffer.length <= maxRetainedBytes) return;
    this.buffer = Buffer.allocUnsafe(this.initialCapacity);
    this.reset();
  }
}
