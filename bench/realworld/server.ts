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
const PORT = parseInt(process.env.PORT || '3000', 10);

// Initialize production pools
export const plex = createPlex<RealworldWorkerSpec>(workerSpec, {
  workerFile: plexWorkerPath,
  workers: WORKERS,
});

export const piscina = new Piscina({
  filename: piscinaWorkerPath,
  minThreads: WORKERS,
  maxThreads: WORKERS,
});

/**
 * Reads and parses JSON body from incoming HTTP request
 */
function readJsonBody(req: http.IncomingMessage): Promise<any> {
  return new Promise((resolve, reject) => {
    let raw = '';
    req.on('data', (chunk) => {
      raw += chunk;
      if (raw.length > 65536) {
        req.destroy();
        reject(new Error('Payload too large'));
      }
    });
    req.on('end', () => {
      if (!raw) {
        resolve({});
        return;
      }
      try {
        resolve(JSON.parse(raw));
      } catch (err) {
        reject(new Error('Invalid JSON'));
      }
    });
    req.on('error', reject);
  });
}

export const server = http.createServer(async (req, res) => {
  const url = req.url || '/';
  const method = req.method || 'GET';

  // 1. PlexRing Real-World Auth Verification Endpoint
  if (url === '/api/plex/verify' && method === 'POST') {
    try {
      const body = await readJsonBody(req);
      const userId = Number(body.userId) || 1;
      const token = String(body.token || 'auth_token_xyz_default');

      // Step A: Real Database Query
      const user = db.findUserById(userId);
      if (!user) {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end('{"error":"User not found"}');
        return;
      }

      // Step B: Offload Cryptographic Token Verification to PlexRing
      const isValid = await plex.run('verifyAuthToken', {
        userId: user.id,
        token,
        salt: user.salt,
      });

      if (!isValid) {
        res.writeHead(401, { 'Content-Type': 'application/json' });
        res.end('{"error":"Unauthorized signature mismatch"}');
        return;
      }

      // Step C: Save session to database and return authenticated payload
      db.saveSession(user.id, token);

      res.writeHead(200, {
        'Content-Type': 'application/json',
        'Cache-Control': 'no-store',
      });
      res.end(
        JSON.stringify({
          status: 'authenticated',
          engine: 'plexring',
          user: {
            id: user.id,
            email: user.email,
            role: user.role,
          },
        })
      );
    } catch (err: any) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  // 2. Piscina Real-World Auth Verification Endpoint
  if (url === '/api/piscina/verify' && method === 'POST') {
    try {
      const body = await readJsonBody(req);
      const userId = Number(body.userId) || 1;
      const token = String(body.token || 'auth_token_xyz_default');

      // Step A: Real Database Query
      const user = db.findUserById(userId);
      if (!user) {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end('{"error":"User not found"}');
        return;
      }

      // Step B: Offload Cryptographic Token Verification to Piscina
      const isValid = await piscina.run({
        task: 'verifyAuthToken',
        payload: {
          userId: user.id,
          token,
          salt: user.salt,
        },
      });

      if (!isValid) {
        res.writeHead(401, { 'Content-Type': 'application/json' });
        res.end('{"error":"Unauthorized signature mismatch"}');
        return;
      }

      // Step C: Save session to database and return authenticated payload
      db.saveSession(user.id, token);

      res.writeHead(200, {
        'Content-Type': 'application/json',
        'Cache-Control': 'no-store',
      });
      res.end(
        JSON.stringify({
          status: 'authenticated',
          engine: 'piscina',
          user: {
            id: user.id,
            email: user.email,
            role: user.role,
          },
        })
      );
    } catch (err: any) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  // 3. Telemetry and System Health
  if (url === '/api/stats') {
    const mem = process.memoryUsage();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(
      JSON.stringify(
        {
          memory: {
            rssMb: (mem.rss / 1024 / 1024).toFixed(2),
            heapUsedMb: (mem.heapUsed / 1024 / 1024).toFixed(2),
            heapTotalMb: (mem.heapTotal / 1024 / 1024).toFixed(2),
          },
          database: {
            usersSeeded: db.countUsers(),
            activeSessions: db.countSessions(),
          },
          plexRing: plex.stats(),
        },
        null,
        2
      )
    );
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
const isDirectRun =
  process.argv[1]?.endsWith('server.ts') || process.argv[1]?.endsWith('server.js');
if (isDirectRun) {
  server.listen(PORT, '0.0.0.0', () => {
    console.log('='.repeat(70));
    console.log('  PRODUCTION AUTH & USER VERIFICATION SERVICE');
    console.log(`  Listening on:     http://0.0.0.0:${PORT}`);
    console.log(`  Database:         10,000 indexed users pre-seeded in memory`);
    console.log(`  Hardware Workers: ${WORKERS} threads per pool`);
    console.log('  Endpoints:');
    console.log(`    POST http://localhost:${PORT}/api/plex/verify    (PlexRing engine)`);
    console.log(`    POST http://localhost:${PORT}/api/piscina/verify (Piscina engine)`);
    console.log(`    GET  http://localhost:${PORT}/api/stats          (Telemetry)`);
    console.log('='.repeat(70));
  });

  const shutdown = async () => {
    console.log('\nShutting down server...');
    server.close();
    await plex.destroy();
    await piscina.destroy();
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}
