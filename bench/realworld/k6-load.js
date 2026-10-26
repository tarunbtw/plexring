import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Counter } from 'k6/metrics';

// Custom latency & throughput metrics
const plexLatency = new Trend('plex_auth_latency', true);
const piscinaLatency = new Trend('piscina_auth_latency', true);
const plexCounter = new Counter('plex_auth_reqs');
const piscinaCounter = new Counter('piscina_auth_reqs');

const BASE_URL = __ENV.BASE_URL || 'http://localhost:3000';
const DURATION = __ENV.DURATION || '10s';
const VUS = parseInt(__ENV.VUS || '30', 10); // 30 VUs: perfectly tuned for 2-vCPU cloud VM

export const options = {
  scenarios: {
    // Stage 1: Benchmark PlexRing with real database + cryptographic auth check
    plex_production_load: {
      executor: 'constant-vus',
      vus: VUS,
      duration: DURATION,
      exec: 'testPlex',
    },
    // Stage 2: Benchmark Piscina with the exact same database + cryptographic auth check
    piscina_production_load: {
      executor: 'constant-vus',
      vus: VUS,
      duration: DURATION,
      startTime: DURATION, // Runs right after Plex completes
      exec: 'testPiscina',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'], // 99% of requests must succeed
  },
};

const headers = { 'Content-Type': 'application/json' };

export function testPlex() {
  // Pick random user from 10,000 pre-seeded database accounts
  const userId = Math.floor(Math.random() * 10000) + 1;
  const payload = JSON.stringify({
    userId,
    token: `bearer_tok_${userId}_${__VU}_${__ITER}`,
  });

  const res = http.post(`${BASE_URL}/api/plex/verify`, payload, { headers });
  plexLatency.add(res.timings.duration);
  plexCounter.add(1);

  check(res, {
    'status is 200': (r) => r.status === 200,
    'authenticated': (r) => r.json('status') === 'authenticated',
    'engine is plexring': (r) => r.json('engine') === 'plexring',
    'user matches': (r) => r.json('user.id') === userId,
  });
}

export function testPiscina() {
  const userId = Math.floor(Math.random() * 10000) + 1;
  const payload = JSON.stringify({
    userId,
    token: `bearer_tok_${userId}_${__VU}_${__ITER}`,
  });

  const res = http.post(`${BASE_URL}/api/piscina/verify`, payload, { headers });
  piscinaLatency.add(res.timings.duration);
  piscinaCounter.add(1);

  check(res, {
    'status is 200': (r) => r.status === 200,
    'authenticated': (r) => r.json('status') === 'authenticated',
    'engine is piscina': (r) => r.json('engine') === 'piscina',
    'user matches': (r) => r.json('user.id') === userId,
  });
}
