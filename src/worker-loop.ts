import { parentPort, workerData } from 'node:worker_threads';
import { SpscRing } from './ring.js';
import {
  WRITE_IDX,
  SLEEP_FLAG,
  STATUS_OK,
  STATUS_ERROR,
  SPIN_LIMIT,
  FUTEX_TIMEOUT_MS,
  MSG_SHUTDOWN,
  MSG_FALLBACK_JOB,
  MSG_FALLBACK_RES,
  MAX_PAYLOAD,
} from './constants.js';
import { encodeError, encodePayload, decodePayload } from './codec.js';
import type { RawJobHandler, WorkerInboundMessage } from './types.js';

export interface WorkerInitData {
  submitSab: SharedArrayBuffer;
  resultSab: SharedArrayBuffer;
}

const EMPTY_PAYLOAD = new Uint8Array(0);

/**
 * Initializes and starts the adaptive spin-then-park execution loop
 * inside a Node.js worker thread.
 */
export function startWorkerLoop(handlers: Record<number, RawJobHandler>): void {
  if (!parentPort || !workerData) {
    throw new Error('startWorkerLoop must be called inside a Node.js Worker thread');
  }

  const { submitSab, resultSab } = workerData as WorkerInitData;
  const submitRing = new SpscRing(submitSab);
  const resultRing = new SpscRing(resultSab);

  let running = true;

  // Listen for oversized fallback jobs and lifecycle shutdown signals from Main Thread
  parentPort.on('message', async (msg: WorkerInboundMessage) => {
    if (msg.type === MSG_SHUTDOWN) {
      running = false;
      return;
    }

    if (msg.type === MSG_FALLBACK_JOB) {
      const { jobType, requestId, payload } = msg;
      const handler = handlers[jobType];

      if (!handler) {
        const errBuf = new Uint8Array(MAX_PAYLOAD);
        const errLen = encodeError(new Error(`No handler registered for jobType ${jobType}`), errBuf);
        parentPort!.postMessage({
          type: MSG_FALLBACK_RES,
          status: STATUS_ERROR,
          requestId,
          payload: errBuf.subarray(0, errLen),
        });
        return;
      }

      try {
        const result = await handler(payload);
        parentPort!.postMessage({
          type: MSG_FALLBACK_RES,
          status: STATUS_OK,
          requestId,
          payload: result,
        });
      } catch (err) {
        const errBuf = new Uint8Array(MAX_PAYLOAD);
        const errLen = encodeError(err, errBuf);
        parentPort!.postMessage({
          type: MSG_FALLBACK_RES,
          status: STATUS_ERROR,
          requestId,
          payload: errBuf.subarray(0, errLen),
        });
      }
    }
  });

  // Push completed task result back to Main Thread via Result SPSC Ring or fallback
  const pushResult = (status: number, requestId: number, resBytes: Uint8Array): void => {
    if (resBytes.byteLength <= MAX_PAYLOAD) {
      // Fast path: push into Result SPSC Ring
      while (!resultRing.tryPush(status, requestId, resBytes)) {
        // Result ring is momentarily full; spin briefly until Main Thread drains
      }
    } else {
      // Oversized result: send via parentPort fallback
      parentPort!.postMessage({
        type: MSG_FALLBACK_RES,
        status,
        requestId,
        payload: resBytes,
      });
    }
  };

  // Process a single slot pinned in the submit ring
  const processSlot = (jobType: number, requestId: number, payloadView: Uint8Array): void => {
    const handler = handlers[jobType];
    if (!handler) {
      const errBuf = new Uint8Array(MAX_PAYLOAD);
      const len = encodeError(new Error(`No handler registered for jobType ${jobType}`), errBuf);
      pushResult(STATUS_ERROR, requestId, errBuf.subarray(0, len));
      return;
    }

    try {
      const out = handler(payloadView);
      if (out instanceof Promise) {
        // If async handler, wait and push result asynchronously
        out
          .then((res) => pushResult(STATUS_OK, requestId, res))
          .catch((err) => {
            const errBuf = new Uint8Array(MAX_PAYLOAD);
            const len = encodeError(err, errBuf);
            pushResult(STATUS_ERROR, requestId, errBuf.subarray(0, len));
          });
      } else {
        // Synchronous fast path (zero microtask overhead)
        pushResult(STATUS_OK, requestId, out);
      }
    } catch (err) {
      const errBuf = new Uint8Array(MAX_PAYLOAD);
      const len = encodeError(err, errBuf);
      pushResult(STATUS_ERROR, requestId, errBuf.subarray(0, len));
    }
  };

  // 3-Stage Adaptive Spin-then-Park Execution Loop
  function loop(): void {
    while (running) {
      // Stage 1: Tight batch drain of all available jobs
      let processedAny = false;
      while (submitRing.tryPop(processSlot)) {
        processedAny = true;
      }

      if (processedAny) {
        continue;
      }

      // Stage 2: Adaptive user-space spin (avoids ~10µs kernel futex entry on microsecond gaps)
      let foundJob = false;
      for (let i = 0; i < SPIN_LIMIT; i++) {
        if (submitRing.getPendingCount() > 0) {
          foundJob = true;
          break;
        }
      }

      if (foundJob) {
        continue;
      }

      // Stage 3: Double-checked futex sleep
      // 1. Snapshot expected write index BEFORE checking pending count
      const expectedWrite = Atomics.load(submitRing.ctrl, WRITE_IDX);

      if (submitRing.getPendingCount() > 0) {
        continue;
      }

      Atomics.store(submitRing.ctrl, SLEEP_FLAG, 1);

      // Double-check: Did producer enqueue any jobs before/during SLEEP_FLAG store?
      if (submitRing.getPendingCount() > 0) {
        Atomics.store(submitRing.ctrl, SLEEP_FLAG, 0);
        continue;
      }

      // Park thread in kernel until Main Thread calls Atomics.notify()
      // If producer pushed after step 1, wait returns 'not-equal' immediately!
      const waitRes = Atomics.wait(submitRing.ctrl, WRITE_IDX, expectedWrite, FUTEX_TIMEOUT_MS);
      Atomics.store(submitRing.ctrl, SLEEP_FLAG, 0);

      // Yield once on timeout so parentPort I/O callbacks can execute
      if (waitRes === 'timed-out') {
        setImmediate(loop);
        return;
      }
    }
  }

  loop();
}
