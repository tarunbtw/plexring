import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { createPlex } from '../src/index.js';
import { workerSpec, type TestWorkerSpec } from './fixtures/worker.js';

const workerPath = fileURLToPath(new URL('./fixtures/worker.ts', import.meta.url));

describe('Plex Worker Pool Integration', () => {
  it('should initialize pool and execute fast-path echo jobs', async () => {
    const pool = createPlex<TestWorkerSpec>(workerSpec, {
      workerFile: workerPath,
      workers: 2,
    });

    try {
      const payload = new Uint8Array([1, 2, 3, 4, 5, 99]);
      const result = await pool.run('echo', payload);
      assert.deepEqual(result, payload);
    } finally {
      await pool.destroy();
    }
  });

  it('should compute cryptographic SHA-256 hashes correctly', async () => {
    const pool = createPlex<TestWorkerSpec>(workerSpec, {
      workerFile: workerPath,
      workers: 2,
    });

    try {
      const input = new Uint8Array([10, 20, 30, 40]);
      const expected = new Uint8Array(crypto.createHash('sha256').update(input).digest());
      const actual = await pool.run('sha256', input);

      assert.deepEqual(actual, expected);
    } finally {
      await pool.destroy();
    }
  });

  it('should auto-encode and decode structured objects, numbers, and strings', async () => {
    const pool = createPlex<TestWorkerSpec>(workerSpec, {
      workerFile: workerPath,
      workers: 2,
    });

    try {
      // Numbers & object input
      const sum = await pool.run('add', { a: 15, b: 27 });
      assert.equal(sum, 42);

      // Strings
      const reversed = await pool.run('reverseString', 'racecar');
      assert.equal(reversed, 'racecar');

      const reversed2 = await pool.run('reverseString', 'PlexRing');
      assert.equal(reversed2, 'gniRxelP');
    } finally {
      await pool.destroy();
    }
  });

  it('should propagate worker exceptions back as rejected Promises', async () => {
    const pool = createPlex<TestWorkerSpec>(workerSpec, {
      workerFile: workerPath,
      workers: 2,
    });

    try {
      await assert.rejects(
        () => pool.run('throwError', 'Unauthorized access token'),
        (err: Error) => {
          assert.ok(err.message.includes('Worker exception: Unauthorized access token'));
          return true;
        }
      );
    } finally {
      await pool.destroy();
    }
  });

  it('should route oversized payloads (> 249B) through postMessage fallback seamlessly', async () => {
    const pool = createPlex<TestWorkerSpec>(workerSpec, {
      workerFile: workerPath,
      workers: 2,
    });

    try {
      const bigSize = 2048;
      const result = await pool.run('oversizedFallback', bigSize);
      assert.equal(result.byteLength, bigSize);
      assert.equal(result[0], 42);
      assert.equal(result[bigSize - 1], 42);
    } finally {
      await pool.destroy();
    }
  });

  it('should handle high-concurrency burst of 1,000 requests without lost wakeups', async () => {
    const pool = createPlex<TestWorkerSpec>(workerSpec, {
      workerFile: workerPath,
      workers: 4,
    });

    try {
      const count = 1000;
      const tasks: Promise<number>[] = [];

      for (let i = 0; i < count; i++) {
        tasks.push(pool.run('add', { a: i, b: 1 }));
      }

      const results = await Promise.all(tasks);
      assert.equal(results.length, count);

      for (let i = 0; i < count; i++) {
        assert.equal(results[i], i + 1);
      }

      const stats = pool.stats();
      assert.equal(stats.workerCount, 4);
    } finally {
      await pool.destroy();
    }
  });

  it('should inspect telemetry stats in real-time without locking', async () => {
    const pool = createPlex<TestWorkerSpec>(workerSpec, {
      workerFile: workerPath,
      workers: 2,
    });

    try {
      const stats = pool.stats();
      assert.equal(stats.workerCount, 2);
      assert.equal(stats.workers.length, 2);
      assert.equal(typeof stats.totalPendingJobs, 'number');
    } finally {
      await pool.destroy();
    }
  });
});
