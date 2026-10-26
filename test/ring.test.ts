import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { SpscRing } from '../src/ring.js';
import {
  CAPACITY,
  MAX_PAYLOAD,
  TOTAL_RING_BYTES,
  WRITE_IDX,
  READ_IDX,
  SLEEP_FLAG,
} from '../src/constants.js';

describe('SpscRing Buffer Engine', () => {
  it('should initialize with default SharedArrayBuffer or throw if buffer is undersized', () => {
    const ring = new SpscRing();
    assert.equal(ring.sab.byteLength, TOTAL_RING_BYTES);
    assert.equal(ring.getPendingCount(), 0);

    // Should throw if undersized buffer is provided
    assert.throws(
      () => new SpscRing(new SharedArrayBuffer(100)),
      RangeError
    );
  });

  it('should push and pop slots with exact payload fidelity', () => {
    const ring = new SpscRing();
    const payload = new Uint8Array([10, 20, 30, 40, 50]);

    const pushed = ring.tryPush(1, 1001, payload);
    assert.equal(pushed, true);
    assert.equal(ring.getPendingCount(), 1);

    let received = false;
    const popped = ring.tryPop((type, reqId, data) => {
      received = true;
      assert.equal(type, 1);
      assert.equal(reqId, 1001);
      assert.deepEqual(data, payload);
    });

    assert.equal(popped, true);
    assert.equal(received, true);
    assert.equal(ring.getPendingCount(), 0);
  });

  it('should reject payloads exceeding MAX_PAYLOAD (249 bytes)', () => {
    const ring = new SpscRing();
    const oversizedPayload = new Uint8Array(MAX_PAYLOAD + 1);
    const maxPayload = new Uint8Array(MAX_PAYLOAD);

    assert.equal(ring.tryPush(1, 1, oversizedPayload), false);
    assert.equal(ring.tryPush(1, 1, maxPayload), true);
  });

  it('should return false on tryPop when ring is empty', () => {
    const ring = new SpscRing();
    let invoked = false;
    const popped = ring.tryPop(() => {
      invoked = true;
    });

    assert.equal(popped, false);
    assert.equal(invoked, false);
  });

  it('should detect full ring and apply backpressure at 1024 slots', () => {
    const ring = new SpscRing();
    const payload = new Uint8Array([1, 2, 3]);

    // Fill all 1024 slots
    for (let i = 0; i < CAPACITY; i++) {
      const ok = ring.tryPush(1, i, payload);
      assert.equal(ok, true, `Failed to push slot ${i}`);
    }

    assert.equal(ring.getPendingCount(), CAPACITY);

    // 1025th push must fail (ring is full)
    const overflow = ring.tryPush(1, 9999, payload);
    assert.equal(overflow, false, 'Overflow push should return false');

    // Pop 1 slot
    let poppedId = -1;
    ring.tryPop((_, id) => {
      poppedId = id;
    });
    assert.equal(poppedId, 0);
    assert.equal(ring.getPendingCount(), CAPACITY - 1);

    // Now push succeeds
    const succeedAfterPop = ring.tryPush(1, 9999, payload);
    assert.equal(succeedAfterPop, true);
    assert.equal(ring.getPendingCount(), CAPACITY);
  });

  it('should pin slot in memory during consumption before advancing READ_IDX', () => {
    const ring = new SpscRing();
    const payload = new Uint8Array([42]);

    ring.tryPush(1, 555, payload);

    let readIndexDuringCallback = -1;
    ring.tryPop(() => {
      // In consumer callback: READ_IDX in ctrl array must STILL be 0
      readIndexDuringCallback = Atomics.load(ring.ctrl, READ_IDX);
    });

    assert.equal(readIndexDuringCallback, 0);
    // After callback finishes: READ_IDX is now updated to 1
    assert.equal(Atomics.load(ring.ctrl, READ_IDX), 1);
  });

  it('should reset SLEEP_FLAG on producer push if consumer was marked sleeping', () => {
    const ring = new SpscRing();
    // Simulate consumer parking
    Atomics.store(ring.ctrl, SLEEP_FLAG, 1);
    assert.equal(ring.isConsumerSleeping(), true);

    ring.tryPush(1, 777, new Uint8Array([9]));

    // tryPush should have reset SLEEP_FLAG to 0 to prevent lost wakeup
    assert.equal(Atomics.load(ring.ctrl, SLEEP_FLAG), 0);
    assert.equal(ring.isConsumerSleeping(), false);
  });

  it('should handle monotonic index wrap-around without data corruption across 50,000 cycles', () => {
    const ring = new SpscRing();
    const totalOps = 50000;
    const testPayload = new Uint8Array([7, 14, 21, 28]);

    for (let i = 0; i < totalOps; i++) {
      const pushed = ring.tryPush(i % 256, i, testPayload);
      assert.equal(pushed, true);

      let verified = false;
      ring.tryPop((type, id, data) => {
        assert.equal(type, i % 256);
        assert.equal(id, i);
        assert.equal(data[0], 7);
        assert.equal(data[1], 14);
        assert.equal(data[2], 21);
        assert.equal(data[3], 28);
        verified = true;
      });

      assert.equal(verified, true);
    }

    assert.equal(ring.getPendingCount(), 0);
  });
});
