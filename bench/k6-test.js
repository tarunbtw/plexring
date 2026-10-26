import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Counter } from 'k6/metrics';

// Custom metrics to compare both pools in the final k6 report
const plexDuration = new Trend('plex_req_duration', true);
const piscinaDuration = new Trend('piscina_req_duration', true);
const plexReqs = new Counter('plex_req_count');
const piscinaReqs = new Counter('piscina_req_count');

const BASE_URL = __ENV.BASE_URL || 'http://localhost:3000';
const DURATION = __ENV.DURATION || '10s';
const VUS = parseInt(__ENV.VUS || '50', 10);

export const options = {
  scenarios: {
    // Scenario 1: PlexRing SharedArrayBuffer lock-free SPSC pool
    plex_benchmark: {
      executor: 'constant-vus',
      vus: VUS,
      duration: DURATION,
      exec: 'testPlex',
    },
    // Scenario 2: Piscina MessagePort postMessage structured clone pool
    piscina_benchmark: {
      executor: 'constant-vus',
      vus: VUS,
      duration: DURATION,
      startTime: DURATION, // Starts immediately after Plex benchmark finishes
      exec: 'testPiscina',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'], // <1% errors
  },
};

export function testPlex() {
  const res = http.get(`${BASE_URL}/plex`);
  plexDuration.add(res.timings.duration);
  plexReqs.add(1);

  check(res, {
    'status is 200': (r) => r.status === 200,
    'engine is plexring': (r) => r.json('engine') === 'plexring',
    'token is valid': (r) => r.json('valid') === true,
  });
}

export function testPiscina() {
  const res = http.get(`${BASE_URL}/piscina`);
  piscinaDuration.add(res.timings.duration);
  piscinaReqs.add(1);

  check(res, {
    'status is 200': (r) => r.status === 200,
    'engine is piscina': (r) => r.json('engine') === 'piscina',
    'token is valid': (r) => r.json('valid') === true,
  });
}
