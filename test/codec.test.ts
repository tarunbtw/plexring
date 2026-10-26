import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  encodePayload,
  decodePayload,
  encodeError,
  TAG_VOID,
  TAG_RAW_BYTES,
  TAG_BOOLEAN,
  TAG_INT32,
  TAG_FLOAT64,
  TAG_UTF8_STRING,
  TAG_JSON,
  TAG_ERROR,
} from '../src/codec.js';
import { MAX_PAYLOAD } from '../src/constants.js';

describe('Zero-Allocation Binary Codec', () => {
  it('should round-trip undefined and null as void', () => {
    const buf = new Uint8Array(MAX_PAYLOAD);
    const len1 = encodePayload(undefined, buf);
    assert.equal(len1, 1);
    assert.equal(buf[0], TAG_VOID);
    assert.equal(decodePayload(buf.subarray(0, len1)), undefined);

    const len2 = encodePayload(null, buf);
    assert.equal(len2, 1);
    assert.equal(decodePayload(buf.subarray(0, len2)), undefined);
  });

  it('should round-trip raw Uint8Array buffers zero-copy', () => {
    const buf = new Uint8Array(MAX_PAYLOAD);
    const input = new Uint8Array([1, 2, 3, 4, 5, 255]);
    const len = encodePayload(input, buf);
    assert.equal(len, input.byteLength + 1);
    assert.equal(buf[0], TAG_RAW_BYTES);

    const decoded = decodePayload(buf.subarray(0, len)) as Uint8Array;
    assert.deepEqual(decoded, input);
  });

  it('should round-trip booleans', () => {
    const buf = new Uint8Array(MAX_PAYLOAD);
    const lenTrue = encodePayload(true, buf);
    assert.equal(lenTrue, 2);
    assert.equal(buf[0], TAG_BOOLEAN);
    assert.equal(decodePayload(buf.subarray(0, lenTrue)), true);

    const lenFalse = encodePayload(false, buf);
    assert.equal(lenFalse, 2);
    assert.equal(decodePayload(buf.subarray(0, lenFalse)), false);
  });

  it('should round-trip integer and floating point numbers', () => {
    const buf = new Uint8Array(MAX_PAYLOAD);

    // Int32
    const lenInt = encodePayload(123456, buf);
    assert.equal(lenInt, 5);
    assert.equal(buf[0], TAG_INT32);
    assert.equal(decodePayload(buf.subarray(0, lenInt)), 123456);

    // Negative Int32
    const lenNeg = encodePayload(-98765, buf);
    assert.equal(lenNeg, 5);
    assert.equal(decodePayload(buf.subarray(0, lenNeg)), -98765);

    // Float64
    const lenFloat = encodePayload(3.141592653589793, buf);
    assert.equal(lenFloat, 9);
    assert.equal(buf[0], TAG_FLOAT64);
    assert.equal(decodePayload(buf.subarray(0, lenFloat)), 3.141592653589793);
  });

  it('should round-trip UTF-8 strings including emojis and unicode', () => {
    const buf = new Uint8Array(MAX_PAYLOAD);
    const text = 'Hello, PlexRing! 🚀 High-Frequency SPSC ⚡️';
    const len = encodePayload(text, buf);
    assert.ok(len > 1);
    assert.equal(buf[0], TAG_UTF8_STRING);

    const decoded = decodePayload(buf.subarray(0, len));
    assert.equal(decoded, text);
  });

  it('should round-trip JSON objects that fit within the slot', () => {
    const buf = new Uint8Array(MAX_PAYLOAD);
    const obj = { userId: 42, role: 'admin', active: true };
    const len = encodePayload(obj, buf);
    assert.ok(len > 1);
    assert.equal(buf[0], TAG_JSON);

    const decoded = decodePayload(buf.subarray(0, len));
    assert.deepEqual(decoded, obj);
  });

  it('should reject payloads that exceed MAX_PAYLOAD with -1', () => {
    const buf = new Uint8Array(MAX_PAYLOAD);
    // 250 bytes of raw data (exceeds 248 byte payload with tag)
    const bigBytes = new Uint8Array(250);
    assert.equal(encodePayload(bigBytes, buf), -1);

    // Long string that exceeds 248 bytes
    const longString = 'a'.repeat(300);
    assert.equal(encodePayload(longString, buf), -1);
  });

  it('should encode and decode Error objects', () => {
    const buf = new Uint8Array(MAX_PAYLOAD);
    const err = new TypeError('Invalid cryptographic signature');
    const len = encodeError(err, buf);
    assert.ok(len > 1);
    assert.equal(buf[0], TAG_ERROR);

    const decoded = decodePayload(buf.subarray(0, len));
    assert.ok(decoded instanceof Error);
    assert.equal(decoded.message, 'Invalid cryptographic signature');
  });
});
