import type { Level } from './levels.ts';

/** Attribute values are constrained to JSON scalars; nesting is rejected at ingest. */
export type AttributeValue = string | number | boolean;

export type Attributes = Record<string, AttributeValue>;

/** A log entry as it arrives on the wire, before validation. */
export interface RawLogEntry {
  timestamp?: unknown;
  level?: unknown;
  service?: unknown;
  message?: unknown;
  attributes?: unknown;
}

/**
 * A validated entry, normalised into the exact shapes the COPY encoder needs.
 *
 * `timestampMicros` is microseconds since the Unix epoch: an integer, so
 * ordering and cursor round-trips are exact rather than float-approximate.
 * `attributesJson` is pre-serialised because the encoder needs bytes, not an
 * object, and serialising once here avoids a second pass later.
 */
export interface ValidLogRow {
  timestampMicros: number;
  levelCode: number;
  service: string;
  message: string;
  attributesJson: string;
}

export interface RejectedEntry {
  index: number;
  reason: string;
}

export interface IngestResult {
  accepted: number;
  rejected: RejectedEntry[];
}

/** A row as returned by GET /logs. */
export interface LogRecord {
  id: string;
  timestamp: string;
  level: Level;
  service: string;
  message: string;
  attributes: Attributes;
}

export interface QueryPage {
  logs: LogRecord[];
  next_cursor: string | null;
}

export interface AggregateBucket {
  start: string;
  group: string | null;
  count: number;
}
