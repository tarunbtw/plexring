import { test, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  CACHE_LINE_BYTES,
  HEADER_BYTES,
  WRITE_IDX,
  READ_IDX,
  SLEEP_FLAG,
  CAPACITY,
  MASK,
  SLOT_SIZE,
  SLOT_SHIFT,
  DATA_BYTES,
  TOTAL_RING_BYTES,
  SLOT_HEADER_BYTES,
  MAX_PAYLOAD,
  OFFSET_JOB_TYPE,
  OFFSET_REQUEST_ID,
  OFFSET_PAYLOAD_LEN,
  OFFSET_PAYLOAD,
  STATUS_OK,
  STATUS_ERROR,
  SPIN_LIMIT,
  FUTEX_TIMEOUT_MS,
} from '../src/constants.js';

describe('Constants & Cache-Line Alignment Invariants', () => {
  it('should enforce 128-byte cache-line separation between atomic control indices', () => {
    assert.equal(CACHE_LINE_BYTES, 128);
    assert.equal(HEADER_BYTES, 384);

    // Byte offsets in Int32Array (each element is 4 bytes)
    const writeByteOffset = WRITE_IDX * 4;
    const readByteOffset = READ_IDX * 4;
    const sleepByteOffset = SLEEP_FLAG * 4;

    assert.equal(writeByteOffset, 0);
    assert.equal(readByteOffset, 128);
    assert.equal(sleepByteOffset, 256);

    // Spacing must be at least 128 bytes to prevent false sharing on Apple Silicon and x86
    assert.ok(readByteOffset - writeByteOffset >= 128);
    assert.ok(sleepByteOffset - readByteOffset >= 128);
    assert.ok(HEADER_BYTES - sleepByteOffset >= 128);
  });

  it('should enforce power-of-two sizing for ring capacity and slots', () => {
    assert.equal(CAPACITY, 1024);
    assert.equal(MASK, 1023);
    assert.equal(SLOT_SIZE, 256);
    assert.equal(SLOT_SHIFT, 8);

    // Powers of two invariant
    assert.equal(CAPACITY & (CAPACITY - 1), 0);
    assert.equal(SLOT_SIZE & (SLOT_SIZE - 1), 0);

    // Bit-shift matches slot size multiplication
    assert.equal(1 << SLOT_SHIFT, SLOT_SIZE);

    // Bitmasking matches modulo
    for (let i = 0; i < 5000; i++) {
      assert.equal(i & MASK, i % CAPACITY);
    }
  });

  it('should compute exact ring byte boundaries without rounding gaps', () => {
    assert.equal(DATA_BYTES, 1024 * 256);
    assert.equal(DATA_BYTES, 262144);
    assert.equal(TOTAL_RING_BYTES, 384 + 262144);
    assert.equal(TOTAL_RING_BYTES, 262528);
  });

  it('should validate 256-byte binary slot layout schema', () => {
    assert.equal(SLOT_HEADER_BYTES, 7);
    assert.equal(MAX_PAYLOAD, 249);
    assert.equal(OFFSET_JOB_TYPE, 0);
    assert.equal(OFFSET_REQUEST_ID, 1);
    assert.equal(OFFSET_PAYLOAD_LEN, 5);
    assert.equal(OFFSET_PAYLOAD, 7);

    // Ensure slot header + payload matches exact slot size
    assert.equal(OFFSET_PAYLOAD + MAX_PAYLOAD, SLOT_SIZE);
  });

  it('should validate status codes and synchronization constants', () => {
    assert.equal(STATUS_OK, 0);
    assert.equal(STATUS_ERROR, 1);
    assert.equal(SPIN_LIMIT, 2000);
    assert.equal(FUTEX_TIMEOUT_MS, 50);
  });
});
