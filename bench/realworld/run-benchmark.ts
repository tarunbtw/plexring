import http from 'node:http';
import os from 'node:os';
import { fileURLToPath } from 'node:url';
import Piscina from 'piscina';
import { createPlex } from '../../src/index.js';
import { workerSpec, type RealworldWorkerSpec } from './worker-plex.js';
import { db } from './db.js';

const plexWorkerPath = fileURLToPath(new URL('./worker-plex.ts', import.meta.url));
const piscinaWorkerPath = fileURLToPath(new URL('./worker-piscina.ts', import.meta.url));

const WORKERS = Math.max(2, os.availableParallelism ? os.availableParallelism() : os.cpus().length);
const CONCURRENCY = 30; // 30 concurrent virtual clients tuned for 2-vCPU / 2GB RAM
const DURATION_MS = 3000; // 3 seconds per engine

async function main() {
  console.log('='.repeat(78));
  console.log('  REAL-WORLD PRODUCTION AUTH & DATABASE SERVICE BENCHMARK');
  console.log(`  Hardware Threads:    ${WORKERS} workers per pool`);
  console.log(`  Database Size:       ${db.countUsers().toLocaleString()} pre-seeded user accounts`);
  console.log(`  Virtual Users:       ${CONCURRENCY} concurrent keep-alive HTTP clients`);
  console.log(`  Workload:            Database Lookup + Cryptographic HMAC-SHA256 Token Verify`);
  console.log('='.repeat(78));

  const plex = createPlex<RealworldWorkerSpec>(workerSpec, {
    workerFile: plexWorkerPath,
    workers: WORKERS,
  });

  const piscina = new Piscina({
    filename: piscinaWorkerPath,
    minThreads: WORKERS,
    maxThreads: WORKERS,
  });

  function readJson(req: http.IncomingMessage): Promise<any> {
    return new Promise((resolve, reject) => {
      let raw = '';
      req.on('data', (c) => (raw += c));
      req.on('end', () => {
        try {
          resolve(JSON.parse(raw));
        } catch {
          resolve({});
        }
      });
      req.on('error', reject);
    });
  }

  const server = http.createServer(async (req, res) => {
    const url = req.url || '/';

    if (url === '/api/plex/verify') {
      const body = await readJson(req);
      const user = db.findUserById(body.userId || 1);
      if (!user) {
        res.writeHead(404).end();
        return;
      }
      const isValid = await plex.run('verifyAuthToken', {
        userId: user.id,
        token: body.token || 'tok',
        salt: user.salt,
      });
      db.saveSession(user.id, body.token || 'tok');
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(isValid ? '{"status":"authenticated"}' : '{"status":"unauthorized"}');
      return;
    }

    if (url === '/api/piscina/verify') {
      const body = await readJson(req);
      const user = db.findUserById(body.userId || 1);
      if (!user) {
        res.writeHead(404).end();
        return;
      }
      const isValid = await piscina.run({
        task: 'verifyAuthToken',
        payload: {
          userId: user.id,
          token: body.token || 'tok',
          salt: user.salt,
        },
      });
      db.saveSession(user.id, body.token || 'tok');
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(isValid ? '{"status":"authenticated"}' : '{"status":"unauthorized"}');
      return;
    }

    res.writeHead(404).end();
  });

  await new Promise<void>((r) => server.listen(0, '127.0.0.1', () => r()));
  const port = (server.address() as any).port;

  interface LoadResult {
    target: string;
    rps: number;
    total: number;
    avgMs: string;
    p50Ms: string;
    p95Ms: string;
    p99Ms: string;
    rssMb: string;
    heapUsedMb: string;
  }

  async function runLoad(target: string, path: string): Promise<LoadResult> {
    const latencies: number[] = [];
    let completed = 0;
    let running = true;
    const agent = new http.Agent({ keepAlive: true, maxSockets: CONCURRENCY });

    const worker = async (vuId: number) => {
      let iter = 0;
      while (running) {
        const userId = ((completed + vuId + iter) % 10000) + 1;
        const postData = JSON.stringify({
          userId,
          token: `sess_${userId}_${vuId}_${iter++}`,
        });

        const t0 = performance.now();
        await new Promise<void>((resolve, reject) => {
          const req = http.request(
            {
              hostname: '127.0.0.1',
              port,
              path,
              method: 'POST',
              agent,
              headers: {
                'Content-Type': 'application/json',
                'Content-Length': Buffer.byteLength(postData),
              },
            },
            (res) => {
              res.resume();
              res.on('end', resolve);
            }
          );
          req.on('error', reject);
          req.write(postData);
          req.end();
        });
        latencies.push(performance.now() - t0);
        completed++;
      }
    };

    const clients = Array.from({ length: CONCURRENCY }, (_, i) => worker(i));
    await new Promise((r) => setTimeout(r, DURATION_MS));
    running = false;
    await Promise.all(clients);
    agent.destroy();

    latencies.sort((a, b) => a - b);
    const sum = latencies.reduce((acc, v) => acc + v, 0);
    const avgMs = (sum / latencies.length).toFixed(2);
    const p50Ms = latencies[Math.floor(latencies.length * 0.50)].toFixed(2);
    const p95Ms = latencies[Math.floor(latencies.length * 0.95)].toFixed(2);
    const p99Ms = latencies[Math.floor(latencies.length * 0.99)].toFixed(2);
    const rps = Math.round((completed / DURATION_MS) * 1000);
    const mem = process.memoryUsage();

    return {
      target,
      rps,
      total: completed,
      avgMs,
      p50Ms,
      p95Ms,
      p99Ms,
      rssMb: (mem.rss / 1024 / 1024).toFixed(1) + ' MB',
      heapUsedMb: (mem.heapUsed / 1024 / 1024).toFixed(1) + ' MB',
    };
  }

  process.stdout.write('Warming up server and pools... ');
  await runLoad('Warmup Plex', '/api/plex/verify');
  await runLoad('Warmup Piscina', '/api/piscina/verify');
  console.log('done!\n');

  console.log(`[1/2] Benchmarking Piscina with ${CONCURRENCY} VUs for 3s...`);
  const piscinaRes = await runLoad('Piscina Engine', '/api/piscina/verify');
  console.log(`      Completed ${piscinaRes.total.toLocaleString()} requests -> ${piscinaRes.rps.toLocaleString()} req/s (p99: ${piscinaRes.p99Ms}ms)\n`);

  console.log(`[2/2] Benchmarking PlexRing with ${CONCURRENCY} VUs for 3s...`);
  const plexRes = await runLoad('PlexRing Engine', '/api/plex/verify');
  console.log(`      Completed ${plexRes.total.toLocaleString()} requests -> ${plexRes.rps.toLocaleString()} req/s (p99: ${plexRes.p99Ms}ms)\n`);

  const speedup = (plexRes.rps / piscinaRes.rps).toFixed(2) + 'x';

  console.log('--- Real-World Microservice Benchmark Summary ---');
  console.table([
    {
      Engine: 'PlexRing (SharedArrayBuffer SPSC)',
      'Req/sec': plexRes.rps.toLocaleString(),
      'Total Requests': plexRes.total.toLocaleString(),
      'Avg Latency': `${plexRes.avgMs} ms`,
      'p50': `${plexRes.p50Ms} ms`,
      'p95': `${plexRes.p95Ms} ms`,
      'p99': `${plexRes.p99Ms} ms`,
      'Memory (RSS)': plexRes.rssMb,
      'Heap Used': plexRes.heapUsedMb,
      Speedup: `${speedup} higher RPS`,
    },
    {
      Engine: 'Piscina (postMessage Clone)',
      'Req/sec': piscinaRes.rps.toLocaleString(),
      'Total Requests': piscinaRes.total.toLocaleString(),
      'Avg Latency': `${piscinaRes.avgMs} ms`,
      'p50': `${piscinaRes.p50Ms} ms`,
      'p95': `${piscinaRes.p95Ms} ms`,
      'p99': `${piscinaRes.p99Ms} ms`,
      'Memory (RSS)': piscinaRes.rssMb,
      'Heap Used': piscinaRes.heapUsedMb,
      Speedup: 'Baseline',
    },
  ]);

  await new Promise<void>((r) => server.close(() => r()));
  await plex.destroy();
  await piscina.destroy();

  console.log('\nReal-world benchmark completed successfully.');
}

main().catch(console.error);
