# plexring ⚡️

> Ultra-low latency, lock-free `SharedArrayBuffer` SPSC worker pool for Node.js micro-tasks.

[![CI](https://github.com/tarunbtw/plexring/actions/workflows/ci.yml/badge.svg)](https://github.com/tarunbtw/plexring/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Node.js: >=20.0.0](https://img.shields.io/badge/node-%3E%3D20.0.0-green.svg)](https://nodejs.org/)

---

## The Problem: Piscina's Micro-Task Tax

In Node.js, [Piscina](https://github.com/piscinajs/piscina) is the standard worker pool library. While excellent for heavy CPU-bound tasks (image encoding, multi-megabyte parsing), Piscina's own documentation explicitly admits that small payloads suffer:

> *"small objects: overhead not worth it"*

Every job dispatch and result return via `postMessage` triggers V8 HTML5 Structured Clone serialization, kernel context switches via libuv `eventfd`, and continuous heap allocation/GC pauses. For micro-tasks taking **2µs to 50µs** (cryptographic token verification, schema validation, rate-limiting rules, packet checksums), the **15µs–35µs IPC overhead exceeds the work itself**.

**`plexring`** bypasses V8 structured clone serialization and libuv mutexes entirely by allocating dedicated pairs of lock-free Single-Producer Single-Consumer (SPSC) ring buffers per worker backed by pre-allocated `SharedArrayBuffer` memory.

---

## Performance: Head-to-Head vs. Piscina

Tested on 10 Worker Threads (`node --import tsx/esm bench/vs-piscina.ts`):

### 1. Sequential Round-Trip Latency (Concurrency = 1)
| Workload | Piscina Throughput | **PlexRing Throughput** | Speedup | Latency (med) |
| :--- | :--- | :--- | :--- | :--- |
| **64-Byte Pure IPC Echo** *(0µs work, isolates IPC tax)* | 70,947 ops/s | **240,112 ops/s** | **3.4x faster** | **3.8 µs** vs 13.7 µs |
| **128-Byte SHA-256 Micro-Hash** *(~5µs CPU work)* | 79,247 ops/s | **182,970 ops/s** | **2.3x faster** | **5.2 µs** vs 11.8 µs |
| **Auth Verification Simulation** *(~15µs CPU work)* | 71,855 ops/s | **234,073 ops/s** | **3.3x faster** | **3.9 µs** vs 13.4 µs |

### 2. Pipelined High-Concurrency Throughput (Multi-Core Saturation)
*Simulates real-world server traffic (HTTP/gRPC/WebSocket) with pipelined in-flight tasks:*

| Workload | Piscina Throughput | **PlexRing Throughput** | Speedup |
| :--- | :--- | :--- | :--- |
| **64-Byte Echo (Concurrency = 16)** | 101,070 ops/s | **1,051,208 ops/s** | **10.4x faster** |
| **64-Byte Echo (Concurrency = 64)** | 110,800 ops/s | **1,002,745 ops/s** | **9.1x faster** |
| **128-Byte SHA-256 (Concurrency = 16)** | 106,664 ops/s | **1,018,866 ops/s** | **9.6x faster** |
| **200-Byte AuthCheck (Concurrency = 16)** | 102,215 ops/s | **1,208,715 ops/s** | **11.8x faster** |

---

## Architectural Guarantees

```
[ CONTROL HEADER: 384 Bytes (3 x 128-Byte Cache Lines) ]
+------------------------------------------------------------------------------------+
| Cache Line 0 (Bytes 0..127):   WRITE_IDX (Int32[0])  | Pad (Producer exclusive)    |
| Cache Line 1 (Bytes 128..255): READ_IDX  (Int32[32]) | Pad (Consumer exclusive)    |
| Cache Line 2 (Bytes 256..383): SLEEP_FLAG(Int32[64]) | Pad (Consumer park flag)    |
+------------------------------------------------------------------------------------+

[ 1024 PRE-ALLOCATED SLOTS: 256 Bytes Each (Offsets 384..262,527) ]
Byte Offset:   0          1..4 (4B)           5..6 (2B)        7..255 (249B)
            +------+---------------------+-----------------+-------------------+
Field:      | u8   | u32 (Little-Endian) | u16 (LE)        | Uint8Array[249]   |
            +------+---------------------+-----------------+-------------------+
Submit Ring:|jobTyp| requestId           | payloadLen      | payloadBytes      |
Result Ring:|status| requestId           | payloadLen      | resultBytes       |
            +------+---------------------+-----------------+-------------------+
```

- **Zero Locks & Zero CAS Loops**: Dedicated SPSC ring per worker eliminates multi-threaded contention. Monotonic wrap-around indices (`idx & 1023`) and bit-shift multiplication (`slotIdx << 8`).
- **128-Byte Cache Line Padding**: `WRITE_IDX`, `READ_IDX`, and `SLEEP_FLAG` each sit on dedicated 128-byte cache lines, eliminating false sharing on both Intel/AMD (64B) and Apple Silicon (128B).
- **Zero-Copy Slot Pinning**: The consumer receives a pinned `Uint8Array.subarray` view into the slot. `READ_IDX` is only published via atomic release fence after the user handler finishes, guaranteeing the producer cannot overwrite active data.
- **Adaptive 3-Stage Worker Loop**:
  1. *Tight Batch Drain*: Processes all available jobs in the SPSC ring.
  2. *Adaptive User-Space Spin*: Spins for up to 2,000 iterations polling pending jobs, avoiding expensive kernel futex transitions on bursty traffic.
  3. *Double-Checked Futex Sleep*: Enters kernel sleep via `Atomics.wait()` only when genuinely idle, with a 50ms timeout ensuring libuv I/O pump.
- **Asynchronous Coalesced Return Draining**: Main thread never blocks. Completed jobs are batch-drained using V8 `Atomics.waitAsync` microtasks, resolving up to 1,024 Promises per wakeup tick.
- **Transparent Fallback**: Payloads > 249 bytes (or temporary ring overflow) automatically and transparently route through standard `worker.postMessage`.

---

## Quickstart

### 1. Define Worker Tasks (`worker.ts`)

```typescript
import crypto from 'node:crypto';
import { defineWorker } from 'plexring';

export const workerSpec = defineWorker({
  // Fast-path zero-copy binary view
  hashSha256: (bytes: Uint8Array): Uint8Array => {
    return new Uint8Array(crypto.createHash('sha256').update(bytes).digest());
  },

  // Structured input with zero-allocation auto-codec
  verifyToken: (input: { userId: number; token: string }): boolean => {
    return input.userId > 0 && input.token.length === 64;
  },
});

export type AppWorkerSpec = typeof workerSpec.handlers;
```

### 2. Dispatch from Main Thread (`server.ts`)

```typescript
import { createPlex } from 'plexring';
import { workerSpec, type AppWorkerSpec } from './worker.js';

const pool = createPlex<AppWorkerSpec>(workerSpec, {
  workerFile: new URL('./worker.js', import.meta.url),
  workers: 4,
});

// Full end-to-end TypeScript autocomplete & type safety!
const isValid = await pool.run('verifyToken', {
  userId: 42,
  token: 'a9f8e7b6c5d4e3f2a1b0c9d8e7f6a5b4c3d2e1f0a9b8c7d6e5f4a3b2c1d0e9f8',
});

console.log('Valid:', isValid);

// Real-time telemetry without mutex locking
console.log(pool.stats());

await pool.destroy();
```

---

## API Reference

### `defineWorker(handlers)`
Registers named tasks inside a worker thread and auto-starts the lock-free SPSC loop. Maps handler keys alphabetically to 1-byte job IDs under the hood.

### `createPlex<Spec>(spec, options)`
Initializes the worker pool with $N$ worker threads and SharedArrayBuffer rings.
- `options.workerFile`: Path or URL to the compiled worker script.
- `options.workers`: Number of worker threads (defaults to hardware concurrency).

Methods:
- `pool.run(taskName, input)`: Dispatches task and returns typed Promise.
- `pool.stats()`: Returns queue depths and worker states.
- `pool.destroy()`: Gracefully terminates all workers and frees buffers.
- `[Symbol.asyncDispose]()`: Native support for `using pool = createPlex(...)`.

---

## Real-World HTTP Load Testing & k6 Simulation

PlexRing includes a production-grade HTTP Auth Microservice benchmark simulating real-world API gateway token verification.

### Option A: Automated All-in-One Benchmark
Runs an automated keep-alive HTTP benchmark with 50 concurrent virtual users comparing PlexRing vs Piscina:

```bash
npm run bench:http
```

### Option B: Running the Standalone Server & k6

1. Start the HTTP server on port 3000:
```bash
npm run server
```

2. Run the included k6 load test suite:
```bash
k6 run bench/k6-test.js
```

---

## License

MIT © Tarun

