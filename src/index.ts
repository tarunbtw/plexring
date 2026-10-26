import { isMainThread } from 'node:worker_threads';
import { PlexRing } from './pool.js';
import { startWorkerLoop } from './worker-loop.js';
import {
  encodePayload,
  decodePayload,
  TAG_RAW_BYTES,
  TAG_JSON,
  BUF_TRUE,
  BUF_FALSE,
  BUF_VOID,
} from './codec.js';
import { MAX_PAYLOAD } from './constants.js';
import type {
  PlexOptions,
  PoolStats,
  RawJobHandler,
  TypedJobHandler,
} from './types.js';

export { PlexRing } from './pool.js';
export { SpscRing } from './ring.js';
export * from './constants.js';
export * from './codec.js';
export * from './types.js';

export interface WorkerSpecDef<T extends Record<string, TypedJobHandler<any, any>>> {
  handlers: T;
  keyMap: Map<string, number>;
  keys: string[];
}

/**
 * Defines worker tasks with full TypeScript inference and auto-registers them
 * when executed inside a worker thread.
 */
export function defineWorker<T extends Record<string, TypedJobHandler<any, any>>>(
  handlers: T
): WorkerSpecDef<T> {
  const keys = Object.keys(handlers).sort();
  const keyMap = new Map<string, number>();

  keys.forEach((key, idx) => {
    keyMap.set(key, idx);
  });

  // If running inside worker thread, start the SPSC spin-park execution loop automatically
  if (!isMainThread) {
    const rawHandlers: Record<number, RawJobHandler> = {};
    const workerScratchBuf = new Uint8Array(MAX_PAYLOAD);

    function serializeResult(result: unknown, key: string): Uint8Array {
      if (result === true) return BUF_TRUE;
      if (result === false) return BUF_FALSE;
      if (result === undefined || result === null) return BUF_VOID;

      const len = encodePayload(result, workerScratchBuf);
      if (len !== -1) {
        return workerScratchBuf.subarray(0, len);
      }

      // Oversized result handling (triggers fallback postMessage return in worker-loop)
      if (result instanceof Uint8Array) {
        const bigBuf = new Uint8Array(result.byteLength + 1);
        bigBuf[0] = TAG_RAW_BYTES;
        bigBuf.set(result, 1);
        return bigBuf;
      }

      const jsonBytes = new TextEncoder().encode(JSON.stringify(result));
      const bigBuf = new Uint8Array(jsonBytes.byteLength + 1);
      bigBuf[0] = TAG_JSON;
      bigBuf.set(jsonBytes, 1);
      return bigBuf;
    }

    for (let i = 0; i < keys.length; i++) {
      const key = keys[i];
      const fn = handlers[key];

      rawHandlers[i] = (payloadView: Uint8Array): Uint8Array | Promise<Uint8Array> => {
        const decodedInput = decodePayload(payloadView);
        const result = fn(decodedInput);

        if (result instanceof Promise) {
          return result.then((res) => serializeResult(res, key));
        }

        return serializeResult(result, key);
      };
    }

    startWorkerLoop(rawHandlers);
  }

  return { handlers, keyMap, keys };
}

/**
 * Type-safe interface wrapper over PlexRing.
 */
export interface TypedPlex<T extends Record<string, TypedJobHandler<any, any>>> {
  /**
   * Dispatches a named task to the worker pool with end-to-end type safety.
   */
  run<K extends keyof T>(
    task: K,
    input: Parameters<T[K]>[0]
  ): Promise<Awaited<ReturnType<T[K]>>>;

  /**
   * Dispatches a raw byte job by numeric ID.
   */
  runRaw(jobType: number, payload: Uint8Array): Promise<Uint8Array>;

  /**
   * Returns real-time telemetry stats across all workers.
   */
  stats(): PoolStats;

  /**
   * Gracefully destroys the pool and terminates all workers.
   */
  destroy(): Promise<void>;

  /**
   * Async disposable for `using pool = ...`
   */
  [Symbol.asyncDispose](): Promise<void>;
}

/**
 * Creates a type-safe Plex worker pool.
 */
export function createPlex<T extends Record<string, TypedJobHandler<any, any>>>(
  spec: WorkerSpecDef<T> | (string[]),
  options: PlexOptions
): TypedPlex<T> {
  const pool = new PlexRing(options);

  // Derive deterministic key mapping (alphabetical sort)
  const keys = Array.isArray(spec) ? [...spec].sort() : spec.keys;
  const keyMap = new Map<string, number>();
  keys.forEach((key, idx) => keyMap.set(key, idx));

  const scratchBuf = new Uint8Array(249);

  return {
    async run<K extends keyof T>(task: K, input: Parameters<T[K]>[0]): Promise<Awaited<ReturnType<T[K]>>> {
      const jobType = keyMap.get(task as string);
      if (jobType === undefined) {
        throw new Error(`Unknown task name: "${String(task)}"`);
      }

      let payloadBytes: Uint8Array;
      if (input === true) {
        payloadBytes = BUF_TRUE;
      } else if (input === false) {
        payloadBytes = BUF_FALSE;
      } else if (input === undefined || input === null) {
        payloadBytes = BUF_VOID;
      } else {
        const len = encodePayload(input, scratchBuf);
        if (len !== -1) {
          payloadBytes = scratchBuf.subarray(0, len);
        } else {
          // Oversized input: serialize with tag byte for fallback postMessage path
          if ((input as unknown) instanceof Uint8Array) {
            const bigBuf = new Uint8Array((input as unknown as Uint8Array).byteLength + 1);
            bigBuf[0] = TAG_RAW_BYTES;
            bigBuf.set(input as unknown as Uint8Array, 1);
            payloadBytes = bigBuf;
          } else {
            const jsonBytes = new TextEncoder().encode(JSON.stringify(input));
            const bigBuf = new Uint8Array(jsonBytes.byteLength + 1);
            bigBuf[0] = TAG_JSON;
            bigBuf.set(jsonBytes, 1);
            payloadBytes = bigBuf;
          }
        }
      }

      const resBytes = await pool.run(jobType, payloadBytes);
      return decodePayload(resBytes) as Awaited<ReturnType<T[K]>>;
    },

    runRaw(jobType: number, payload: Uint8Array): Promise<Uint8Array> {
      return pool.run(jobType, payload);
    },

    stats(): PoolStats {
      return pool.stats();
    },

    destroy(): Promise<void> {
      return pool.destroy();
    },

    [Symbol.asyncDispose](): Promise<void> {
      return pool.destroy();
    },
  };
}
