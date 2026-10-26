import crypto from 'node:crypto';

export default function run({ task, payload }: { task: string; payload: Uint8Array }): unknown {
  switch (task) {
    case 'echo':
      return payload;
    case 'hash':
      return new Uint8Array(crypto.createHash('sha256').update(payload).digest());
    case 'authCheck': {
      let sum = 0;
      for (let i = 0; i < payload.length; i++) {
        sum = (sum + payload[i] * 31) | 0;
      }
      return sum !== 0;
    }
    default:
      throw new Error(`Unknown task: ${task}`);
  }
}
