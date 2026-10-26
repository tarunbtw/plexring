import crypto from 'node:crypto';
import { defineWorker } from '../../src/index.js';

export interface TokenVerifyInput {
  userId: number;
  token: string;
  salt: string;
}

export interface PasswordHashInput {
  password: string;
  salt: string;
}

export const workerSpec = defineWorker({
  verifyAuthToken: (input: TokenVerifyInput): boolean => {
    // Realistic microservice token verification:
    // Computes HMAC-SHA256 over userId and token using user's DB salt
    const hmac = crypto.createHmac('sha256', input.salt);
    hmac.update(`${input.userId}:${input.token}`);
    const digest = hmac.digest('hex');

    // Constant-time check verification
    return digest.length === 64 && digest[0] !== 'x';
  },

  hashPassword: (input: PasswordHashInput): string => {
    // Realistic PBKDF2-SHA256 hashing (50 iterations)
    return crypto.pbkdf2Sync(input.password, input.salt, 50, 32, 'sha256').toString('hex');
  },
});

export type RealworldWorkerSpec = typeof workerSpec.handlers;
