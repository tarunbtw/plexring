import http from 'node:http';
import os from 'node:os';
import { fileURLToPath } from 'node:url';
import Piscina from 'piscina';
import { createPlex } from '../src/index.js';
import { workerSpec, type BenchWorkerSpec } from './worker-plex.js';

const plexWorkerPath = fileURLToPath(new URL('./worker-plex.ts', import.meta.url));
const piscinaWorkerPath = fileURLToPath(new URL('./worker-piscina.ts', import.meta.url));

const WORKERS = Math.max(2, os.availableParallelism ? os.availableParallelism() : os.cpus().length);
const PORT = parseInt(process.env.PORT || '3000', 10);

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

// Pre-allocated realistic 128-byte authentication token
const tokenPayload = new Uint8Array(128);
tokenPayload.fill(42);

export const server = http.createServer(async (req, res) => {
  const url = req.url || '/';

  if (url === '/plex') {
    try {
      const valid = await plex.run('authCheck', tokenPayload);
      res.writeHead(200, {
        'Content-Type': 'application/json',
        'Cache-Control': 'no-store',
      });
      res.end(valid ? '{"valid":true,"engine":"plexring"}' : '{"valid":false}');
    } catch (err: any) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  if (url === '/piscina') {
    try {
      const valid = await piscina.run({ task: 'authCheck', payload: tokenPayload });
      res.writeHead(200, {
        'Content-Type': 'application/json',
        'Cache-Control': 'no-store',
      });
      res.end(valid ? '{"valid":true,"engine":"piscina"}' : '{"valid":false}');
    } catch (err: any) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  if (url === '/stats') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(plex.stats(), null, 2));
    return;
  }

  if (url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', uptime: process.uptime() }));
    return;
  }

  res.writeHead(404, { 'Content-Type': 'application/json' });
  res.end('{"error":"Not Found"}');
});

// Auto-start if invoked directly from CLI
const isDirectRun = process.argv[1]?.endsWith('http-server.ts') || process.argv[1]?.endsWith('http-server.js');
if (isDirectRun) {
  server.listen(PORT, '0.0.0.0', () => {
    console.log('='.repeat(65));
    console.log('  PLEXRING vs PISCINA: HTTP AUTH MICROSERVICE');
    console.log(`  Listening on:     http://localhost:${PORT}`);
    console.log(`  Workers:          ${WORKERS} threads per pool`);
    console.log('  Endpoints:');
    console.log(`    GET/POST http://localhost:${PORT}/plex    -> PlexRing Auth Check`);
    console.log(`    GET/POST http://localhost:${PORT}/piscina -> Piscina Auth Check`);
    console.log(`    GET      http://localhost:${PORT}/stats   -> Telemetry`);
    console.log('='.repeat(65));
  });

  const shutdown = async () => {
    console.log('\nGracefully shutting down HTTP server...');
    server.close();
    await plex.destroy();
    await piscina.destroy();
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}
