import { Bench } from 'tinybench';
import { fileURLToPath } from 'node:url';
import os from 'node:os';
import Piscina from 'piscina';
import { createPlex } from '../src/index.js';
import { workerSpec, type BenchWorkerSpec } from './worker-plex.js';

const plexWorkerPath = fileURLToPath(new URL('./worker-plex.ts', import.meta.url));
const piscinaWorkerPath = fileURLToPath(new URL('./worker-piscina.ts', import.meta.url));

const WORKERS = Math.max(2, os.availableParallelism ? os.availableParallelism() : os.cpus().length);

interface ConcurrentResult {
  workload: string;
  plexOpsSec: number;
  piscinaOpsSec: number;
}

async function benchSlidingWindow(
  fn: () => Promise<unknown>,
  concurrency: number,
  total: number
): Promise<number> {
  let inFlight = 0;
  let completed = 0;
  const t0 = performance.now();

  await new Promise<void>((resolve, reject) => {
    function launch() {
      while (inFlight < concurrency && (completed + inFlight) < total) {
        inFlight++;
        fn()
          .then(() => {
            inFlight--;
            completed++;
            if (completed === total) {
              resolve();
            } else {
              launch();
            }
          })
          .catch(reject);
      }
    }
    launch();
  });

  const durMs = performance.now() - t0;
  return Math.round((total / durMs) * 1000);
}

async function runBenchmarks() {
  console.log('='.repeat(75));
  console.log(' PLEXRING vs PISCINA: HEAD-TO-HEAD PERFORMANCE BENCHMARK');
  console.log(` Hardware Cores: ${WORKERS} Workers (Dynamic Core Detection)`);
  console.log('='.repeat(75));

  // Initialize pools
  const plex = createPlex<BenchWorkerSpec>(workerSpec, {
    workerFile: plexWorkerPath,
    workers: WORKERS,
  });

  const piscina = new Piscina({
    filename: piscinaWorkerPath,
    minThreads: WORKERS,
    maxThreads: WORKERS,
  });

  // Warmup both pools
  const warmupBuf = new Uint8Array(64);
  for (let i = 0; i < 200; i++) {
    await plex.run('echo', warmupBuf);
    await piscina.run({ task: 'echo', payload: warmupBuf });
  }

  // Workload 1: 64-byte Pure IPC Echo (Sequential Round-trip Latency)
  console.log('\n>>> Part 1: Sequential Round-Trip Latency (Concurrency = 1)');
  console.log('--- Workload 1: 64-Byte Pure IPC Echo (Zero CPU Work, Isolates IPC Tax) ---');
  const echoBench = new Bench({ time: 1000, warmup: false });
  const echoPayload = new Uint8Array(64);
  echoPayload.fill(7);

  echoBench
    .add('PlexRing (SharedArrayBuffer SPSC)', async () => {
      await plex.run('echo', echoPayload);
    })
    .add('Piscina (MessagePort postMessage)', async () => {
      await piscina.run({ task: 'echo', payload: echoPayload });
    });

  await echoBench.run();
  console.table(echoBench.table());

  // Workload 2: 128-byte SHA-256 Micro-Hash (Sequential)
  console.log('\n--- Workload 2: 128-Byte SHA-256 Micro-Hash (~5µs CPU Work) ---');
  const hashBench = new Bench({ time: 1000, warmup: false });
  const hashPayload = new Uint8Array(128);
  hashPayload.fill(42);

  hashBench
    .add('PlexRing (SharedArrayBuffer SPSC)', async () => {
      await plex.run('hash', hashPayload);
    })
    .add('Piscina (MessagePort postMessage)', async () => {
      await piscina.run({ task: 'hash', payload: hashPayload });
    });

  await hashBench.run();
  console.table(hashBench.table());

  // Workload 3: Auth Token Verification Simulation (Sequential)
  console.log('\n--- Workload 3: Auth Token Verification (~15µs CPU Work) ---');
  const authBench = new Bench({ time: 1000, warmup: false });
  const authPayload = new Uint8Array(200);
  authPayload.fill(99);

  authBench
    .add('PlexRing (SharedArrayBuffer SPSC)', async () => {
      await plex.run('authCheck', authPayload);
    })
    .add('Piscina (MessagePort postMessage)', async () => {
      await piscina.run({ task: 'authCheck', payload: authPayload });
    });

  await authBench.run();
  console.table(authBench.table());

  // Workload 4: Concurrent Pipelined Throughput (Multi-Core Saturation)
  console.log('\n>>> Part 2: Pipelined High-Concurrency Throughput (Multi-Core Saturation)');
  console.log('Simulates real-world HTTP/gRPC server load with concurrent in-flight requests.\n');

  const concurrentResults: {
    Workload: string;
    'PlexRing (ops/s)': string;
    'Piscina (ops/s)': string;
    Speedup: string;
  }[] = [];

  // Pipelined Echo: Concurrency = 16
  process.stdout.write('  [1/4] Running 64B Echo (Concurrency = 16)... ');
  const echo16Total = 20000;
  const echo16Plex = await benchSlidingWindow(() => plex.run('echo', echoPayload), 16, echo16Total);
  const echo16Piscina = await benchSlidingWindow(() => piscina.run({ task: 'echo', payload: echoPayload }), 16, echo16Total);
  const echo16Speedup = (echo16Plex / echo16Piscina).toFixed(2) + 'x';
  console.log(`done! Plex: ${echo16Plex.toLocaleString()} ops/s | Piscina: ${echo16Piscina.toLocaleString()} ops/s (${echo16Speedup})`);
  concurrentResults.push({
    Workload: '64B Echo (Concurrency = 16)',
    'PlexRing (ops/s)': echo16Plex.toLocaleString(),
    'Piscina (ops/s)': echo16Piscina.toLocaleString(),
    Speedup: echo16Speedup,
  });

  // Pipelined Echo: Concurrency = 64
  process.stdout.write('  [2/4] Running 64B Echo (Concurrency = 64)... ');
  const echo64Total = 25000;
  const echo64Plex = await benchSlidingWindow(() => plex.run('echo', echoPayload), 64, echo64Total);
  const echo64Piscina = await benchSlidingWindow(() => piscina.run({ task: 'echo', payload: echoPayload }), 64, echo64Total);
  const echo64Speedup = (echo64Plex / echo64Piscina).toFixed(2) + 'x';
  console.log(`done! Plex: ${echo64Plex.toLocaleString()} ops/s | Piscina: ${echo64Piscina.toLocaleString()} ops/s (${echo64Speedup})`);
  concurrentResults.push({
    Workload: '64B Echo (Concurrency = 64)',
    'PlexRing (ops/s)': echo64Plex.toLocaleString(),
    'Piscina (ops/s)': echo64Piscina.toLocaleString(),
    Speedup: echo64Speedup,
  });

  // Pipelined Hash: Concurrency = 16
  process.stdout.write('  [3/4] Running 128B SHA-256 (Concurrency = 16)... ');
  const hash16Total = 15000;
  const hash16Plex = await benchSlidingWindow(() => plex.run('hash', hashPayload), 16, hash16Total);
  const hash16Piscina = await benchSlidingWindow(() => piscina.run({ task: 'hash', payload: hashPayload }), 16, hash16Total);
  const hash16Speedup = (hash16Plex / hash16Piscina).toFixed(2) + 'x';
  console.log(`done! Plex: ${hash16Plex.toLocaleString()} ops/s | Piscina: ${hash16Piscina.toLocaleString()} ops/s (${hash16Speedup})`);
  concurrentResults.push({
    Workload: '128B SHA-256 (Concurrency = 16)',
    'PlexRing (ops/s)': hash16Plex.toLocaleString(),
    'Piscina (ops/s)': hash16Piscina.toLocaleString(),
    Speedup: hash16Speedup,
  });

  // Pipelined Auth: Concurrency = 16
  process.stdout.write('  [4/4] Running 200B AuthCheck (Concurrency = 16)... ');
  const auth16Total = 15000;
  const auth16Plex = await benchSlidingWindow(() => plex.run('authCheck', authPayload), 16, auth16Total);
  const auth16Piscina = await benchSlidingWindow(() => piscina.run({ task: 'authCheck', payload: authPayload }), 16, auth16Total);
  const auth16Speedup = (auth16Plex / auth16Piscina).toFixed(2) + 'x';
  console.log(`done! Plex: ${auth16Plex.toLocaleString()} ops/s | Piscina: ${auth16Piscina.toLocaleString()} ops/s (${auth16Speedup})`);
  concurrentResults.push({
    Workload: '200B AuthCheck (Concurrency = 16)',
    'PlexRing (ops/s)': auth16Plex.toLocaleString(),
    'Piscina (ops/s)': auth16Piscina.toLocaleString(),
    Speedup: auth16Speedup,
  });

  console.log('\n--- Consolidated Concurrency Benchmark Summary ---');
  console.table(concurrentResults);

  // Teardown
  await plex.destroy();
  await piscina.destroy();
  console.log('\nBenchmark completed successfully.');
}

runBenchmarks().catch(console.error);

