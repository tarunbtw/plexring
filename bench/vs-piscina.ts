import { Bench } from 'tinybench';
import { fileURLToPath } from 'node:url';
import Piscina from 'piscina';
import { createPlex } from '../src/index.js';
import { workerSpec, type BenchWorkerSpec } from './worker-plex.js';

const plexWorkerPath = fileURLToPath(new URL('./worker-plex.ts', import.meta.url));
const piscinaWorkerPath = fileURLToPath(new URL('./worker-piscina.ts', import.meta.url));

const WORKERS = 4;

async function runBenchmarks() {
  console.log('='.repeat(70));
  console.log(' PLEXRING vs PISCINA: HEAD-TO-HEAD PERFORMANCE BENCHMARK');
  console.log(` Hardware Cores: ${WORKERS} Workers`);
  console.log('='.repeat(70));

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
  for (let i = 0; i < 100; i++) {
    await plex.run('echo', warmupBuf);
    await piscina.run({ task: 'echo', payload: warmupBuf });
  }

  // Workload 1: 64-byte Pure IPC Echo
  console.log('\n>>> Workload 1: 64-Byte Pure IPC Echo (Zero CPU Work, Isolates IPC Tax)');
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

  // Workload 2: 128-byte SHA-256 Micro-Hash
  console.log('\n>>> Workload 2: 128-Byte SHA-256 Micro-Hash (~5µs CPU Work)');
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

  // Workload 3: Auth Token Verification Simulation
  console.log('\n>>> Workload 3: Auth Token Verification (~15µs CPU Work)');
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

  // Teardown
  await plex.destroy();
  await piscina.destroy();
  console.log('\nBenchmark completed successfully.');
}

runBenchmarks().catch(console.error);
