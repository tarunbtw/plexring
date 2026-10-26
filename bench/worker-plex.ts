import crypto from 'node:crypto';
import { defineWorker } from '../src/index.js';

export const workerSpec = defineWorker({
  echo: (payload: Uint8Array): Uint8Array => {
    return payload;
  },

  hash: (payload: Uint8Array): Uint8Array => {
    return new Uint8Array(crypto.createHash('sha256').update(payload).digest());
  },

  authCheck: (payload: Uint8Array): boolean => {
    // Simulated token verify (checks signature + digest)
    let sum = 0;
    for (let i = 0; i < payload.length; i++) {
      sum = (sum + payload[i] * 31) | 0;
    }
    return sum !== 0;
  },
});

export type BenchWorkerSpec = typeof workerSpec.handlers;
