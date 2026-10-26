import { MAX_PAYLOAD } from './constants.js';

export const TAG_VOID = 0x00;
export const TAG_RAW_BYTES = 0x01;
export const TAG_UTF8_STRING = 0x02;
export const TAG_BOOLEAN = 0x03;
export const TAG_FLOAT64 = 0x04;
export const TAG_INT32 = 0x05;
export const TAG_JSON = 0x06;
export const TAG_ERROR = 0xff;

// Pre-allocated immutable singletons for zero-allocation fast paths
export const BUF_TRUE = new Uint8Array([TAG_BOOLEAN, 1]);
export const BUF_FALSE = new Uint8Array([TAG_BOOLEAN, 0]);
export const BUF_VOID = new Uint8Array([TAG_VOID]);

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

// Reusable scratch buffer for packing structured primitives without heap allocations
const scratchBuf = new Uint8Array(MAX_PAYLOAD);
const scratchView = new DataView(scratchBuf.buffer, scratchBuf.byteOffset, scratchBuf.byteLength);
const scratchPayloadSlice = scratchBuf.subarray(1, MAX_PAYLOAD);

/**
 * Encodes an arbitrary JS value into a target buffer slice.
 *
 * @param value The value to encode (Uint8Array, string, boolean, number, object, or void)
 * @param target Uint8Array buffer slice (at least 249 bytes available)
 * @param targetOffset Offset inside target buffer where payload starts
 * @returns Total bytes written into target (including 1-byte tag), or -1 if payload exceeds MAX_PAYLOAD.
 */
export function encodePayload(
  value: unknown,
  target: Uint8Array = scratchBuf,
  targetOffset: number = 0
): number {
  if (value === undefined || value === null) {
    target[targetOffset] = TAG_VOID;
    return 1;
  }

  // 1. Raw Uint8Array (fastest: 1-byte tag + raw copy)
  if (value instanceof Uint8Array) {
    const len = value.byteLength;
    if (len + 1 > MAX_PAYLOAD) return -1;
    target[targetOffset] = TAG_RAW_BYTES;
    if (len > 0) {
      target.set(value, targetOffset + 1);
    }
    return len + 1;
  }

  // 2. Boolean (1-byte tag + 1-byte bool value)
  if (typeof value === 'boolean') {
    target[targetOffset] = TAG_BOOLEAN;
    target[targetOffset + 1] = value ? 1 : 0;
    return 2;
  }

  // 3. Number (integers packed into 4B Int32; floats packed into 8B Float64)
  if (typeof value === 'number') {
    const view = target === scratchBuf
      ? scratchView
      : new DataView(target.buffer, target.byteOffset, target.byteLength);

    if (Number.isInteger(value) && value >= -2147483648 && value <= 2147483647) {
      target[targetOffset] = TAG_INT32;
      view.setInt32(targetOffset + 1, value, true);
      return 5;
    } else {
      target[targetOffset] = TAG_FLOAT64;
      view.setFloat64(targetOffset + 1, value, true);
      return 9;
    }
  }

  // 4. UTF-8 String (TextEncoder.encodeInto directly into target buffer, zero allocations)
  if (typeof value === 'string') {
    target[targetOffset] = TAG_UTF8_STRING;
    const destSlice = (target === scratchBuf && targetOffset === 0)
      ? scratchPayloadSlice
      : target.subarray(targetOffset + 1, targetOffset + MAX_PAYLOAD);
    const result = textEncoder.encodeInto(value, destSlice);
    // If string did not fully fit within MAX_PAYLOAD - 1
    if (result.read !== value.length) {
      return -1;
    }
    return result.written + 1;
  }

  // 5. Plain object / JSON fallback (compact serializer into slot)
  if (typeof value === 'object') {
    try {
      const jsonStr = JSON.stringify(value);
      target[targetOffset] = TAG_JSON;
      const destSlice = (target === scratchBuf && targetOffset === 0)
        ? scratchPayloadSlice
        : target.subarray(targetOffset + 1, targetOffset + MAX_PAYLOAD);
      const result = textEncoder.encodeInto(jsonStr, destSlice);
      if (result.read !== jsonStr.length) {
        return -1;
      }
      return result.written + 1;
    } catch {
      return -1;
    }
  }

  return -1;
}

/**
 * Decodes payload bytes from a slot into their original JS value.
 */
export function decodePayload(buffer: Uint8Array): unknown {
  const len = buffer.byteLength;
  if (len === 0) {
    return undefined;
  }

  const tag = buffer[0];

  switch (tag) {
    case TAG_VOID:
      return undefined;
    case TAG_BOOLEAN:
      return buffer[1] === 1;
    case TAG_INT32:
      return buffer[1] | (buffer[2] << 8) | (buffer[3] << 16) | (buffer[4] << 24);
    case TAG_FLOAT64: {
      const view = new DataView(buffer.buffer, buffer.byteOffset + 1, len - 1);
      return view.getFloat64(0, true);
    }
    case TAG_RAW_BYTES:
      return buffer.subarray(1);
    case TAG_UTF8_STRING:
      return textDecoder.decode(buffer.subarray(1));
    case TAG_JSON:
      return JSON.parse(textDecoder.decode(buffer.subarray(1)));
    case TAG_ERROR:
      return new Error(textDecoder.decode(buffer.subarray(1)));
    default:
      return buffer.subarray(1);
  }
}

/**
 * Encodes an Error instance into the slot payload.
 */
export function encodeError(
  err: unknown,
  target: Uint8Array = scratchBuf,
  targetOffset: number = 0
): number {
  target[targetOffset] = TAG_ERROR;
  const msg = err instanceof Error ? err.message : String(err);
  const destSlice = target.subarray(targetOffset + 1, targetOffset + MAX_PAYLOAD);
  const result = textEncoder.encodeInto(msg, destSlice);
  return result.written + 1;
}
