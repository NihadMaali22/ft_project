import { levelToCode, INVALID_LEVEL } from './levels.ts';
import { parseIsoToMicros } from './time.ts';
import type { RawLogEntry, ValidLogRow } from './types.ts';

/**
 * Hand-written validators.
 *
 * A schema library (zod, ajv with its compile step, class-validator) would cost
 * several microseconds per entry. At the 15k entries/second target under a
 * 0.5 CPU budget that is a double-digit percentage of the entire CPU allowance,
 * spent on work these ~40 lines do in well under a microsecond.
 */

/** Entries may not be timestamped more than five minutes into the future. */
const MAX_FUTURE_SKEW_MICROS = 5 * 60 * 1_000_000;

// Defensive ceilings. Set far above anything a real logging client emits; they
// exist to stop a single malformed request from consuming the row buffer.
const MAX_SERVICE_LENGTH = 255;
const MAX_MESSAGE_LENGTH = 65_536;
const MAX_ATTRIBUTE_KEY_LENGTH = 255;
const MAX_ATTRIBUTE_VALUE_LENGTH = 8192;
const MAX_ATTRIBUTE_COUNT = 128;

const EMPTY_ATTRIBUTES_JSON = '{}';

/**
 * Either a validated row or a rejection reason.
 *
 * Modelled as a bare union rather than a tagged result object so the success
 * path allocates exactly one object - the row itself - and the failure path
 * allocates only the reason string.
 */
export type ValidationResult = ValidLogRow | string;

export function isRejection(result: ValidationResult): result is string {
  return typeof result === 'string';
}

/**
 * Validates one entry against the ingestion contract.
 *
 * `nowMillis` is passed in rather than read here so that every entry in a batch
 * is judged against a single consistent clock reading.
 */
export function validateEntry(entry: unknown, nowMillis: number): ValidationResult {
  if (entry === null || typeof entry !== 'object' || Array.isArray(entry)) {
    return 'entry must be a JSON object';
  }

  const candidate = entry as RawLogEntry;

  // --- timestamp ---
  const rawTimestamp = candidate.timestamp;
  if (rawTimestamp === undefined || rawTimestamp === null) {
    return 'timestamp is required';
  }
  if (typeof rawTimestamp !== 'string') {
    return 'timestamp must be an ISO 8601 string';
  }
  const timestampMicros = parseIsoToMicros(rawTimestamp);
  if (timestampMicros === null) {
    return `invalid timestamp: ${JSON.stringify(rawTimestamp)} is not a valid ISO 8601 instant`;
  }
  if (timestampMicros > nowMillis * 1000 + MAX_FUTURE_SKEW_MICROS) {
    return 'timestamp is more than five minutes in the future';
  }

  // --- level ---
  const rawLevel = candidate.level;
  if (rawLevel === undefined || rawLevel === null) {
    return 'level is required';
  }
  if (typeof rawLevel !== 'string') {
    return 'level must be one of: debug, info, warn, error';
  }
  const levelCode = levelToCode(rawLevel);
  if (levelCode === INVALID_LEVEL) {
    return `invalid level: '${rawLevel}'`;
  }

  // --- service ---
  const rawService = candidate.service;
  if (rawService === undefined || rawService === null) {
    return 'service is required';
  }
  if (typeof rawService !== 'string' || rawService.length === 0) {
    return 'service must be a non-empty string';
  }
  if (rawService.length > MAX_SERVICE_LENGTH) {
    return `service exceeds ${MAX_SERVICE_LENGTH} characters`;
  }

  // --- message ---
  const rawMessage = candidate.message;
  if (rawMessage === undefined || rawMessage === null) {
    return 'message is required';
  }
  if (typeof rawMessage !== 'string' || rawMessage.length === 0) {
    return 'message must be a non-empty string';
  }
  if (rawMessage.length > MAX_MESSAGE_LENGTH) {
    return `message exceeds ${MAX_MESSAGE_LENGTH} characters`;
  }

  // --- attributes ---
  const rawAttributes = candidate.attributes;
  let attributesJson = EMPTY_ATTRIBUTES_JSON;

  if (rawAttributes !== undefined && rawAttributes !== null) {
    if (typeof rawAttributes !== 'object' || Array.isArray(rawAttributes)) {
      return 'attributes must be a flat object';
    }

    const keys = Object.keys(rawAttributes);
    if (keys.length > MAX_ATTRIBUTE_COUNT) {
      return `attributes exceeds ${MAX_ATTRIBUTE_COUNT} keys`;
    }

    const record = rawAttributes as Record<string, unknown>;
    for (let i = 0; i < keys.length; i++) {
      const key = keys[i] as string;
      if (key.length === 0) {
        return 'attribute keys must be non-empty';
      }
      if (key.length > MAX_ATTRIBUTE_KEY_LENGTH) {
        return `attribute key '${key.slice(0, 32)}...' exceeds ${MAX_ATTRIBUTE_KEY_LENGTH} characters`;
      }

      const value = record[key];
      const valueType = typeof value;

      if (valueType === 'string') {
        if ((value as string).length > MAX_ATTRIBUTE_VALUE_LENGTH) {
          return `attribute '${key}' exceeds ${MAX_ATTRIBUTE_VALUE_LENGTH} characters`;
        }
      } else if (valueType === 'number') {
        // JSON cannot carry NaN or Infinity, but a non-JSON caller could.
        if (!Number.isFinite(value)) {
          return `attribute '${key}' must be a finite number`;
        }
      } else if (valueType !== 'boolean') {
        // Covers null, arrays and nested objects, all of which are typeof
        // 'object', as well as any other non-scalar.
        return `attribute '${key}' must be a string, number, or boolean (nested objects and arrays are not allowed)`;
      }
    }

    if (keys.length > 0) {
      attributesJson = JSON.stringify(rawAttributes);
    }
  }

  return {
    timestampMicros,
    levelCode,
    service: rawService,
    message: rawMessage,
    attributesJson,
  };
}
