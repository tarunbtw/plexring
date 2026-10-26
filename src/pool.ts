import { Worker } from 'node:worker_threads';
import os from 'node:os';
import { SpscRing } from './ring.js';
import {
  TOTAL_RING_BYTES,
  WRITE_IDX,
  SLEEP_FLAG,
  STATUS_OK,
  STATUS_ERROR,
  MAX_PAYLOAD,
  MSG_SHUTDOWN,
  MSG_FALLBACK_JOB,
  MSG_FALLBACK_RES,
} from './constants.js';
import { decodePayload } from './codec.js';
import type {
  PlexOptions,
  PoolStats,
  WorkerTelemetry,
  WorkerOutboundMessage,
} from './types.js';

interface WorkerContext {
  id: number;
  worker: Worker;
  submitRing: SpscRing;
  resultRing: SpscRing;
  lastReadResultIdx: number;
}

const TABLE_SIZE = 65536;
const TABLE_MASK = TABLE_SIZE - 1;

/**
 * High-performance Worker Pool utilizing dedicated SPSC rings per worker,
 * Atomics.waitAsync non-blocking batch draining, and O(1) least-loaded routing.
 */
export class PlexRing {
  private readonly workers: WorkerContext[] = [];
  private readonly workerCount: number;

  // Pre-allocated flat arrays for O(1) Promise tracking without Map() GC churn
  private readonly pendingResolvers: (((val: Uint8Array) => void) | undefined)[] = new Array(TABLE_SIZE);
  private readonly pendingRejecters: (((err: Error) => void) | undefined)[] = new Array(TABLE_SIZE);

  private nextRequestId = 1;
  private isDestroyed = false;

  constructor(options: PlexOptions) {
    this.workerCount = options.workers ?? (os.availableParallelism ? os.availableParallelism() : os.cpus().length);
    if (this.workerCount <= 0) {
      throw new RangeError('Worker count must be at least 1');
    }

    const workerUrl = options.workerFile instanceof URL
      ? options.workerFile
      : new URL(options.workerFile, 'file://' + process.cwd() + '/');

    for (let i = 0; i < this.workerCount; i++) {
      const submitSab = new SharedArrayBuffer(TOTAL_RING_BYTES);
      const resultSab = new SharedArrayBuffer(TOTAL_RING_BYTES);

      const submitRing = new SpscRing(submitSab);
      const resultRing = new SpscRing(resultSab);

      const worker = new Worker(workerUrl, {
        workerData: { submitSab, resultSab },
      });

      const ctx: WorkerContext = {
        id: i,
        worker,
        submitRing,
        resultRing,
        lastReadResultIdx: 0,
      };

      // Listen for fallback responses sent via postMessage
      worker.on('message', (msg: WorkerOutboundMessage) => {
        if (msg && msg.type === MSG_FALLBACK_RES) {
          const slot = msg.requestId & TABLE_MASK;
          const resolve = this.pendingResolvers[slot];
          const reject = this.pendingRejecters[slot];
          this.pendingResolvers[slot] = undefined;
          this.pendingRejecters[slot] = undefined;

          if (msg.status === STATUS_OK) {
            resolve?.(msg.payload);
          } else {
            const err = decodePayload(msg.payload);
            reject?.(err instanceof Error ? err : new Error(String(err)));
          }
        }
      });

      this.workers.push(ctx);

      // Arm non-blocking asynchronous result draining for this worker's resultRing
      this.armWaitAsync(ctx);
    }
  }

  /**
   * Arms Atomics.waitAsync on the worker's resultRing.
   * When worker pushes results and notifies WRITE_IDX, V8 resolves the microtask
   * on the main thread and drains all accumulated results in a tight batch loop.
   */
  private armWaitAsync(ctx: WorkerContext): void {
    while (!this.isDestroyed) {
      // 1. Snapshot write index BEFORE draining
      const expectedWrite = Atomics.load(ctx.resultRing.ctrl, WRITE_IDX);

      // 2. Drain all completed results accumulated so far
      this.drainResults(ctx);

      // 3. Double-check if worker pushed more results while draining
      if (ctx.resultRing.getPendingCount() > 0) {
        continue;
      }

      // 4. Arm non-blocking asynchronous waiter. If worker pushed a result after step 1,
      // waitAsync synchronously returns 'not-equal' and never deadlocks!
      const waitRes = Atomics.waitAsync(ctx.resultRing.ctrl, WRITE_IDX, expectedWrite);
      if (waitRes.async) {
        waitRes.value.then(() => {
          this.armWaitAsync(ctx);
        });
        return;
      } else {
        if (waitRes.value === 'not-equal') {
          continue;
        }
        setImmediate(() => this.armWaitAsync(ctx));
        return;
      }
    }
  }

  /**
   * Drains all available completed jobs from the worker's resultRing in a single batch.
   */
  private drainResults(ctx: WorkerContext): void {
    while (
      ctx.resultRing.tryPop((status, requestId, payloadView) => {
        const slot = requestId & TABLE_MASK;
        const resolve = this.pendingResolvers[slot];
        const reject = this.pendingRejecters[slot];
        this.pendingResolvers[slot] = undefined;
        this.pendingRejecters[slot] = undefined;

        if (!resolve && !reject) {
          return;
        }

        if (status === STATUS_OK) {
          // Fast copy payloadView before releasing slot in resultRing
          resolve?.(new Uint8Array(payloadView));
        } else {
          const decoded = decodePayload(payloadView);
          const err = decoded instanceof Error ? decoded : new Error(String(decoded));
          reject?.(err);
        }
      })
    ) {}
  }

  /**
   * Dispatches a job to the pool.
   *
   * Fast Path: <= 249 bytes pushed directly into the least-loaded worker's SPSC submitRing (~300ns).
   * Fallback Path: > 249 bytes or ring-full backpressure dispatched seamlessly via worker.postMessage.
   */
  public run(jobType: number, payload: Uint8Array): Promise<Uint8Array> {
    if (this.isDestroyed) {
      return Promise.reject(new Error('PlexRing pool has been destroyed'));
    }

    const reqId = this.nextRequestId++;
    const slot = reqId & TABLE_MASK;

    const promise = new Promise<Uint8Array>((resolve, reject) => {
      this.pendingResolvers[slot] = resolve;
      this.pendingRejecters[slot] = reject;
    });

    // Select worker with lowest pending queue depth in O(1)
    let bestWorker = this.workers[0];
    let minPending = bestWorker.submitRing.getPendingCount();

    for (let i = 1; i < this.workerCount; i++) {
      const pending = this.workers[i].submitRing.getPendingCount();
      if (pending < minPending) {
        minPending = pending;
        bestWorker = this.workers[i];
      }
    }

    // Opportunistically drain any ready results from the selected worker
    this.drainResults(bestWorker);

    // 1. Fast Path: If fits within 249B, push directly into SPSC submitRing
    if (payload.byteLength <= MAX_PAYLOAD) {
      if (bestWorker.submitRing.tryPush(jobType, reqId, payload)) {
        return promise;
      }
    }

    // 2. Fallback Path: Oversized payload (>249B) or all slots temporarily full
    bestWorker.worker.postMessage({
      type: MSG_FALLBACK_JOB,
      jobType,
      requestId: reqId,
      payload,
    });

    // Poke sleeping worker so futex wakes up immediately to process parentPort message
    if (bestWorker.submitRing.isConsumerSleeping()) {
      Atomics.store(bestWorker.submitRing.ctrl, SLEEP_FLAG, 0);
      Atomics.notify(bestWorker.submitRing.ctrl, WRITE_IDX, 1);
    }

    return promise;
  }

  /**
   * Returns real-time, zero-overhead telemetry across all worker rings.
   */
  public stats(): PoolStats {
    let totalPending = 0;
    const workerStats: WorkerTelemetry[] = [];

    for (const ctx of this.workers) {
      const pending = ctx.submitRing.getPendingCount();
      totalPending += pending;
      workerStats.push({
        workerId: ctx.id,
        pendingJobs: pending,
        isSleeping: ctx.submitRing.isConsumerSleeping(),
      });
    }

    return {
      workerCount: this.workerCount,
      totalPendingJobs: totalPending,
      workers: workerStats,
    };
  }

  /**
   * Gracefully tears down the worker pool and terminates all threads.
   */
  public async destroy(): Promise<void> {
    if (this.isDestroyed) return;
    this.isDestroyed = true;

    const terminationPromises = this.workers.map(async (ctx) => {
      ctx.worker.postMessage({ type: MSG_SHUTDOWN });
      // Poke worker in case it is parked in futex
      Atomics.notify(ctx.submitRing.ctrl, WRITE_IDX, 1);
      await ctx.worker.terminate();
    });

    await Promise.all(terminationPromises);

    // Reject any remaining pending promises
    for (let i = 0; i < TABLE_SIZE; i++) {
      const reject = this.pendingRejecters[i];
      if (reject) {
        reject(new Error('Pool was destroyed while request was in-flight'));
        this.pendingResolvers[i] = undefined;
        this.pendingRejecters[i] = undefined;
      }
    }
  }

  /**
   * Modern async disposable support (using pool = ...)
   */
  public async [Symbol.asyncDispose](): Promise<void> {
    await this.destroy();
  }
}
