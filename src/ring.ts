import {
  HEADER_BYTES,
  TOTAL_RING_BYTES,
  CAPACITY,
  MASK,
  SLOT_SHIFT,
  MAX_PAYLOAD,
  WRITE_IDX,
  READ_IDX,
  SLEEP_FLAG,
  OFFSET_JOB_TYPE,
  OFFSET_REQUEST_ID,
  OFFSET_PAYLOAD_LEN,
  OFFSET_PAYLOAD,
} from './constants.js';

export type SlotConsumerCallback = (
  typeOrStatus: number,
  requestId: number,
  payloadView: Uint8Array
) => void;

/**
 * High-performance, lock-free Single-Producer Single-Consumer (SPSC) ring buffer
 * backed by a pre-allocated SharedArrayBuffer.
 *
 * Invariant Guarantees:
 * 1. Zero locks and zero CAS loops (single readIdx/writeIdx pair).
 * 2. Producer only mutates writeIdx; Consumer only mutates readIdx.
 * 3. 128-byte cache line separation eliminates false sharing on Apple Silicon and x86.
 * 4. Zero-copy consumption: the slot is pinned in-place while consumer callback executes,
 *    and READ_IDX is only published after the callback completes.
 */
export class SpscRing {
  public readonly sab: SharedArrayBuffer;
  public readonly ctrl: Int32Array;
  public readonly u8: Uint8Array;
  public readonly view: DataView;

  // Thread-local shadow index caching:
  // Producer exclusively updates writeIdx; Consumer exclusively updates readIdx.
  // Each thread maintains its own index locally to avoid unnecessary atomic loads.
  private localWriteIdx = 0;
  private localReadIdx = 0;

  constructor(sab?: SharedArrayBuffer) {
    if (sab) {
      if (sab.byteLength < TOTAL_RING_BYTES) {
        throw new RangeError(
          `SharedArrayBuffer too small: expected at least ${TOTAL_RING_BYTES} bytes, got ${sab.byteLength}`
        );
      }
      this.sab = sab;
    } else {
      this.sab = new SharedArrayBuffer(TOTAL_RING_BYTES);
    }

    this.ctrl = new Int32Array(this.sab, 0, HEADER_BYTES / 4);
    this.u8 = new Uint8Array(this.sab);
    this.view = new DataView(this.sab);

    // Sync local shadow indices with initial shared state
    this.localWriteIdx = Atomics.load(this.ctrl, WRITE_IDX);
    this.localReadIdx = Atomics.load(this.ctrl, READ_IDX);
  }

  /**
   * Called by Producer ONLY.
   *
   * Writes the 7-byte slot header + payload into the ring slot and publishes
   * it via an atomic release fence (Atomics.store).
   *
   * @returns true if enqueued successfully, false if ring is full or payload exceeds 249 bytes.
   */
  public tryPush(jobType: number, requestId: number, payload: Uint8Array): boolean {
    const len = payload.byteLength;
    if (len > MAX_PAYLOAD) {
      return false;
    }

    const write = this.localWriteIdx;
    const read = Atomics.load(this.ctrl, READ_IDX);

    // Check capacity using wrap-around safe subtraction
    if (((write - read) | 0) >= CAPACITY) {
      return false; // Ring is full
    }

    const slotIdx = write & MASK;
    const offset = HEADER_BYTES + (slotIdx << SLOT_SHIFT);

    // 1. Write slot header and payload into SharedArrayBuffer (non-atomic writes are safe
    // because consumer will not access this slot until WRITE_IDX is advanced below)
    this.u8[offset + OFFSET_JOB_TYPE] = jobType;
    this.view.setUint32(offset + OFFSET_REQUEST_ID, requestId, true);
    this.view.setUint16(offset + OFFSET_PAYLOAD_LEN, len, true);
    if (len > 0) {
      this.u8.set(payload, offset + OFFSET_PAYLOAD);
    }

    // 2. Publish slot: Atomics.store acts as a release fence so all payload bytes
    // written above are guaranteed visible to the consumer before WRITE_IDX advances
    const nextWrite = (write + 1) | 0;
    this.localWriteIdx = nextWrite;
    Atomics.store(this.ctrl, WRITE_IDX, nextWrite);

    // 3. Wake consumer ONLY if marked sleeping in kernel futex or waitAsync
    if (Atomics.load(this.ctrl, SLEEP_FLAG) === 1) {
      Atomics.store(this.ctrl, SLEEP_FLAG, 0);
      Atomics.notify(this.ctrl, WRITE_IDX, 1);
    }

    return true;
  }

  /**
   * Called by Consumer ONLY.
   *
   * Reads the next available slot into a zero-copy subarray view and executes `onSlot`.
   * Releases the slot back to the producer by advancing READ_IDX via release fence.
   *
   * @returns true if a slot was read and processed, false if ring is empty.
   */
  public tryPop(onSlot: SlotConsumerCallback): boolean {
    const read = this.localReadIdx;
    const write = Atomics.load(this.ctrl, WRITE_IDX); // Acquire fence

    if (read === write) {
      return false; // Ring is empty
    }

    const slotIdx = read & MASK;
    const offset = HEADER_BYTES + (slotIdx << SLOT_SHIFT);

    const typeOrStatus = this.u8[offset + OFFSET_JOB_TYPE];
    const requestId = this.view.getUint32(offset + OFFSET_REQUEST_ID, true);
    const len = this.view.getUint16(offset + OFFSET_PAYLOAD_LEN, true);

    // Zero-copy pinned view into the slot. The producer cannot overwrite this slot
    // until we store nextRead into READ_IDX below.
    const payloadView = this.u8.subarray(offset + OFFSET_PAYLOAD, offset + OFFSET_PAYLOAD + len);

    // Execute callback while slot remains pinned in memory
    onSlot(typeOrStatus, requestId, payloadView);

    // Release slot back to producer
    const nextRead = (read + 1) | 0;
    this.localReadIdx = nextRead;
    Atomics.store(this.ctrl, READ_IDX, nextRead);

    return true;
  }

  /**
   * O(1) Queue Depth check for generic callers.
   */
  public getPendingCount(): number {
    return (Atomics.load(this.ctrl, WRITE_IDX) - Atomics.load(this.ctrl, READ_IDX)) | 0;
  }

  /**
   * Fast queue depth check for Producer (uses localWriteIdx without redundant atomic load).
   */
  public getProducerPendingCount(): number {
    return (this.localWriteIdx - Atomics.load(this.ctrl, READ_IDX)) | 0;
  }

  /**
   * Fast queue depth check for Consumer (uses localReadIdx without redundant atomic load).
   */
  public getConsumerPendingCount(): number {
    return (Atomics.load(this.ctrl, WRITE_IDX) - this.localReadIdx) | 0;
  }

  /**
   * Ultra-fast O(1) check if consumer has any pending jobs (single atomic comparison).
   */
  public hasPendingJobs(): boolean {
    return Atomics.load(this.ctrl, WRITE_IDX) !== this.localReadIdx;
  }

  /**
   * Check if consumer is currently parked in kernel futex sleep.
   */
  public isConsumerSleeping(): boolean {
    return Atomics.load(this.ctrl, SLEEP_FLAG) === 1;
  }
}

