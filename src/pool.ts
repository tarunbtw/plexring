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
import {
  decodePayload,
  BUF_TRUE,
  BUF_FALSE,
  BUF_VOID,
  TAG_BOOLEAN,
  TAG_VOID,
} from './codec.js';
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
  onWaitAsyncReady: () => void;
}

const TABLE_SIZE = 65536;
const TABLE_MASK = TABLE_SIZE - 1;

export class PlexRing {
  private readonly workers: WorkerContext[] = [];
  private readonly workerCount: number;

  // Pre-allocated flat arrays for O(1) Promise tracking with zero closure allocations
  private readonly pendingResolvers: (((val: any) => void) | undefined)[] = new Array(TABLE_SIZE);
  private readonly pendingRejecters: (((err: Error) => void) | undefined)[] = new Array(TABLE_SIZE);
  private readonly pendingRawMode: Uint8Array = new Uint8Array(TABLE_SIZE);

  private nextRequestId = 1;
  private lastActiveWorkerIdx = 0;
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
        onWaitAsyncReady: () => {
          Atomics.store(resultRing.ctrl, SLEEP_FLAG, 0);
          this.armWaitAsync(ctx);
        },
      };

      // Listen for fallback responses sent via postMessage
      worker.on('message', (msg: WorkerOutboundMessage) => {
        if (msg && msg.type === MSG_FALLBACK_RES) {
          const slot = msg.requestId & TABLE_MASK;
          const resolve = this.pendingResolvers[slot];
          if (!resolve) return;
          const reject = this.pendingRejecters[slot];
          const isRaw = this.pendingRawMode[slot] === 1;
          this.pendingResolvers[slot] = undefined;
          this.pendingRejecters[slot] = undefined;
          this.pendingRawMode[slot] = 0;

          if (msg.status === STATUS_OK) {
            try {
              resolve(isRaw ? msg.payload : decodePayload(msg.payload));
            } catch (err) {
              reject?.(err instanceof Error ? err : new Error(String(err)));
            }
          } else {
            try {
              const err = decodePayload(msg.payload);
              reject?.(err instanceof Error ? err : new Error(String(err)));
            } catch (err) {
              reject?.(err instanceof Error ? err : new Error(String(err)));
            }
          }
        }
      });

      // Prevent uncaught worker error events from crashing main process
      worker.on('error', () => {
        if (!this.isDestroyed) {
          this.drainResults(ctx);
        }
      });

      worker.on('exit', (code: number) => {
        if (!this.isDestroyed && code !== 0) {
          this.drainResults(ctx);
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
      if (ctx.resultRing.getConsumerPendingCount() > 0) {
        continue;
      }

      // Mark that Main Thread is arming waitAsync
      Atomics.store(ctx.resultRing.ctrl, SLEEP_FLAG, 1);

      // Double check after setting flag to avoid race
      if (ctx.resultRing.getConsumerPendingCount() > 0) {
        Atomics.store(ctx.resultRing.ctrl, SLEEP_FLAG, 0);
        continue;
      }

      // 4. Arm non-blocking asynchronous waiter. If worker pushed a result after step 1,
      // waitAsync synchronously returns 'not-equal' and never deadlocks!
      const waitRes = Atomics.waitAsync(ctx.resultRing.ctrl, WRITE_IDX, expectedWrite);
      if (waitRes.async) {
        waitRes.value.then(ctx.onWaitAsyncReady);
        return;
      } else {
        Atomics.store(ctx.resultRing.ctrl, SLEEP_FLAG, 0);
        if (waitRes.value === 'not-equal') {
          continue;
        }
        setImmediate(ctx.onWaitAsyncReady);
        return;
      }
    }
  }

  /**
   * Reusable consumer callback for draining results with zero per-pop closure allocations.
   */
  private readonly onSlotResult = (status: number, requestId: number, payloadView: Uint8Array): void => {
    const slot = requestId & TABLE_MASK;
    const resolve = this.pendingResolvers[slot];
    if (!resolve) return;
    const reject = this.pendingRejecters[slot];
    const isRaw = this.pendingRawMode[slot] === 1;
    this.pendingResolvers[slot] = undefined;
    this.pendingRejecters[slot] = undefined;
    this.pendingRawMode[slot] = 0;

    if (status === STATUS_OK) {
      if (isRaw) {
        resolve(new Uint8Array(payloadView));
      } else {
        const len = payloadView.byteLength;
        if (len === 2 && payloadView[0] === TAG_BOOLEAN) {
          resolve(payloadView[1] === 1);
        } else if (len === 1 && payloadView[0] === TAG_VOID) {
          resolve(undefined);
        } else {
          try {
            resolve(decodePayload(payloadView));
          } catch (err) {
            reject?.(err instanceof Error ? err : new Error(String(err)));
          }
        }
      }
    } else {
      try {
        const decoded = decodePayload(payloadView);
        reject?.(decoded instanceof Error ? decoded : new Error(String(decoded)));
      } catch (err) {
        reject?.(err instanceof Error ? err : new Error(String(err)));
      }
    }
  };

  /**
   * Drains all available completed jobs from the worker's resultRing in a single batch.
   */
  private drainResults(ctx: WorkerContext): void {
    while (ctx.resultRing.tryPop(this.onSlotResult)) {}
  }

  /**
   * Dispatches a job to the pool.
   *
   * Fast Path: <= 249 bytes pushed directly into the least-loaded worker's SPSC submitRing (~300ns).
   * Fallback Path: > 249 bytes or ring-full backpressure dispatched seamlessly via worker.postMessage.
   */
  public run<T = any>(jobType: number, payload: Uint8Array, raw: boolean = true): Promise<T> {
    if (this.isDestroyed) {
      return Promise.reject(new Error('PlexRing pool has been destroyed'));
    }

    const reqId = this.nextRequestId++;
    if (this.nextRequestId > 0x7fffffff) this.nextRequestId = 1;
    const slot = reqId & TABLE_MASK;

    const promise = new Promise<T>((resolve, reject) => {
      this.pendingResolvers[slot] = resolve;
      this.pendingRejecters[slot] = reject;
      this.pendingRawMode[slot] = raw ? 1 : 0;
    });

    // Warm-affinity spillover scheduler:
    // If last active worker has 0 pending jobs, it is warm and spinning in user space!
    // Reusing it avoids kernel futex wakeup on sequential streams.
    let bestWorker = this.workers[this.lastActiveWorkerIdx];
    let minPending = bestWorker.submitRing.getProducerPendingCount();

    if (minPending > 0) {
      // Last active worker has a queue! Spill over to next available worker in round-robin order
      const start = (this.lastActiveWorkerIdx + 1) % this.workerCount;
      for (let i = 0; i < this.workerCount; i++) {
        const idx = (start + i) % this.workerCount;
        const candidate = this.workers[idx];
        const pending = candidate.submitRing.getProducerPendingCount();
        if (pending < minPending) {
          minPending = pending;
          bestWorker = candidate;
          this.lastActiveWorkerIdx = idx;
          if (minPending === 0) break;
        }
      }
    }

    // Opportunistically drain any ready results
    if (this.workerCount <= 4) {
      for (let i = 0; i < this.workerCount; i++) {
        this.drainResults(this.workers[i]);
      }
    } else {
      this.drainResults(bestWorker);
    }

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
        this.pendingResolvers[i] = undefined;
        this.pendingRejecters[i] = undefined;
        reject(new Error('Pool was destroyed while request was in-flight'));
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
