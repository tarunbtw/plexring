import crypto from 'node:crypto';
import { defineWorker } from '../../src/index.js';

export const workerSpec = defineWorker({
  echo: (bytes: Uint8Array): Uint8Array => {
    return bytes;
  },

  sha256: (bytes: Uint8Array): Uint8Array => {
    return new Uint8Array(crypto.createHash('sha256').update(bytes).digest());
  },

  add: (params: { a: number; b: number }): number => {
    return params.a + params.b;
  },

  reverseString: (str: string): string => {
    return str.split('').reverse().join('');
  },

  throwError: (msg: string): void => {
    throw new Error(`Worker exception: ${msg}`);
  },

  oversizedFallback: (size: number): Uint8Array => {
    // Generates oversized payload (e.g., 2048 bytes) to test fallback postMessage routing
    const out = new Uint8Array(size);
    out.fill(42);
    return out;
  },
});

export type TestWorkerSpec = typeof workerSpec.handlers;
