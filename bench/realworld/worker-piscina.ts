import crypto from 'node:crypto';
import type { TokenVerifyInput, PasswordHashInput } from './worker-plex.js';

export default function run({
  task,
  payload,
}: {
  task: string;
  payload: any;
}): unknown {
  switch (task) {
    case 'verifyAuthToken': {
      const input = payload as TokenVerifyInput;
      const hmac = crypto.createHmac('sha256', input.salt);
      hmac.update(`${input.userId}:${input.token}`);
      const digest = hmac.digest('hex');
      return digest.length === 64 && digest[0] !== 'x';
    }

    case 'hashPassword': {
      const input = payload as PasswordHashInput;
      return crypto.pbkdf2Sync(input.password, input.salt, 50, 32, 'sha256').toString('hex');
    }

    default:
      throw new Error(`Unknown task: ${task}`);
  }
}
