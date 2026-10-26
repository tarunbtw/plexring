import http from 'node:http';
import os from 'node:os';
import { fileURLToPath } from 'node:url';
import Piscina from 'piscina';
import { createPlex } from '../src/index.js';
import { workerSpec, type BenchWorkerSpec } from './worker-plex.js';

const plexWorkerPath = fileURLToPath(new URL('./worker-plex.ts', import.meta.url));
const piscinaWorkerPath = fileURLToPath(new URL('./worker-piscina.ts', import.meta.url));

const WORKERS = Math.max(2, os.availableParallelism ? os.availableParallelism() : os.cpus().length);
const CONCURRENCY = 50; // 50 Virtual Users / HTTP Keep-Alive connections
const TEST_DURATION_MS = 3000; // 3 seconds per endpoint

async function main() {
  console.log('='.repeat(75));
  console.log('  PLEXRING vs PISCINA: REAL-WORLD HTTP LOAD BENCHMARK');
  console.log(`  Hardware Workers:     ${WORKERS} threads per pool`);
  console.log(`  Concurrent Clients:   ${CONCURRENCY} keep-alive HTTP connections`);
  console.log(`  Workload:             128-Byte Token Verification & Auth Signature Check`);
  console.log('='.repeat(75));

  const plex = createPlex<BenchWorkerSpec>(workerSpec, {
    workerFile: plexWorkerPath,
    workers: WORKERS,
  });

  const piscina = new Piscina({
    filename: piscinaWorkerPath,
    minThreads: WORKERS,
    maxThreads: WORKERS,
  });

  const tokenPayload = new Uint8Array(128);
  tokenPayload.fill(42);

  const server = http.createServer(async (req, res) => {
    if (req.url === '/plex') {
      try {
        const valid = await plex.run('authCheck', tokenPayload);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(valid ? '{"valid":true}' : '{"valid":false}');
      } catch (err: any) {
        res.writeHead(500).end(err.message);
      }
      return;
    }

    if (req.url === '/piscina') {
      try {
        const valid = await piscina.run({ task: 'authCheck', payload: tokenPayload });
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(valid ? '{"valid":true}' : '{"valid":false}');
      } catch (err: any) {
        res.writeHead(500).end(err.message);
      }
      return;
    }

    res.writeHead(404).end();
  });

  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address() as { port: number };
  const port = address.port;

  interface BenchResult {
    target: string;
    rps: number;
    total: number;
    avgMs: string;
    p50Ms: string;
    p90Ms: string;
    p95Ms: string;
    p99Ms: string;
  }

  async function runLoad(target: string, path: string): Promise<BenchResult> {
    const latencies: number[] = [];
    let completed = 0;
    let running = true;
    const agent = new http.Agent({ keepAlive: true, maxSockets: CONCURRENCY });

    const worker = async () => {
      while (running) {
        const t0 = performance.now();
        await new Promise<void>((resolve, reject) => {
          const req = http.get(
            {
              hostname: '127.0.0.1',
              port,
              path,
              agent,
            },
            (res) => {
              res.resume();
              res.on('end', resolve);
            }
          );
          req.on('error', reject);
        });
        latencies.push(performance.now() - t0);
        completed++;
      }
    };

    const clients = Array.from({ length: CONCURRENCY }, () => worker());
    await new Promise((r) => setTimeout(r, TEST_DURATION_MS));
    running = false;
    await Promise.all(clients);
    agent.destroy();

    latencies.sort((a, b) => a - b);
    const sum = latencies.reduce((acc, v) => acc + v, 0);
    const avgMs = (sum / latencies.length).toFixed(2);
    const p50Ms = latencies[Math.floor(latencies.length * 0.50)].toFixed(2);
    const p90Ms = latencies[Math.floor(latencies.length * 0.90)].toFixed(2);
    const p95Ms = latencies[Math.floor(latencies.length * 0.95)].toFixed(2);
    const p99Ms = latencies[Math.floor(latencies.length * 0.99)].toFixed(2);
    const rps = Math.round((completed / TEST_DURATION_MS) * 1000);

    return { target, rps, total: completed, avgMs, p50Ms, p90Ms, p95Ms, p99Ms };
  }

  // Warmup connections
  process.stdout.write('Warming up server and pools... ');
  await runLoad('Warmup Plex', '/plex');
  await runLoad('Warmup Piscina', '/piscina');
  console.log('done!\n');

  console.log(`[1/2] Benchmarking Piscina (/piscina) with ${CONCURRENCY} VUs for 3s...`);
  const piscinaRes = await runLoad('Piscina (/piscina)', '/piscina');
  console.log(`      Completed ${piscinaRes.total.toLocaleString()} requests -> ${piscinaRes.rps.toLocaleString()} req/s (p99: ${piscinaRes.p99Ms}ms)\n`);

  console.log(`[2/2] Benchmarking PlexRing (/plex) with ${CONCURRENCY} VUs for 3s...`);
  const plexRes = await runLoad('PlexRing (/plex)', '/plex');
  console.log(`      Completed ${plexRes.total.toLocaleString()} requests -> ${plexRes.rps.toLocaleString()} req/s (p99: ${plexRes.p99Ms}ms)\n`);

  const speedup = (plexRes.rps / piscinaRes.rps).toFixed(2) + 'x';

  console.log('--- Consolidated HTTP Server Performance Summary ---');
  console.table([
    {
      Target: 'PlexRing (/plex)',
      'Throughput (req/s)': plexRes.rps.toLocaleString(),
      'Total Requests': plexRes.total.toLocaleString(),
      'Avg Latency': `${plexRes.avgMs} ms`,
      'p50': `${plexRes.p50Ms} ms`,
      'p95': `${plexRes.p95Ms} ms`,
      'p99': `${plexRes.p99Ms} ms`,
      Speedup: `${speedup} higher RPS`,
    },
    {
      Target: 'Piscina (/piscina)',
      'Throughput (req/s)': piscinaRes.rps.toLocaleString(),
      'Total Requests': piscinaRes.total.toLocaleString(),
      'Avg Latency': `${piscinaRes.avgMs} ms`,
      'p50': `${piscinaRes.p50Ms} ms`,
      'p95': `${piscinaRes.p95Ms} ms`,
      'p99': `${piscinaRes.p99Ms} ms`,
      Speedup: 'Baseline',
    },
  ]);

  await new Promise<void>((r) => server.close(() => r()));
  await plex.destroy();
  await piscina.destroy();

  console.log('\nHTTP Benchmark completed successfully.');
}

main().catch(console.error);
