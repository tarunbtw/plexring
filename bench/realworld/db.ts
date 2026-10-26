/**
 * In-Memory Mock Database simulating a realistic user & session store.
 * Pre-seeds 10,000 users and provides indexed O(1) lookups.
 */

export interface UserRecord {
  id: number;
  email: string;
  name: string;
  salt: string;
  role: 'admin' | 'developer' | 'operator' | 'user';
  createdAt: number;
}

export interface SessionRecord {
  sessionId: string;
  userId: number;
  expiresAt: number;
}

class MockDatabase {
  private readonly users = new Map<number, UserRecord>();
  private readonly sessions = new Map<string, SessionRecord>();

  constructor(userCount = 10000) {
    const roles: UserRecord['role'][] = ['admin', 'developer', 'operator', 'user'];
    for (let i = 1; i <= userCount; i++) {
      this.users.set(i, {
        id: i,
        email: `user_${i}@production.internal`,
        name: `User ${i}`,
        salt: `salt_secret_token_seed_${(i * 2654435761) >>> 0}`,
        role: roles[i % roles.length],
        createdAt: Date.now() - (i * 3600000),
      });
    }
  }

  public findUserById(id: number): UserRecord | undefined {
    return this.users.get(id);
  }

  public saveSession(userId: number, sessionId: string, ttlSeconds = 3600): SessionRecord {
    const session: SessionRecord = {
      sessionId,
      userId,
      expiresAt: Date.now() + ttlSeconds * 1000,
    };
    this.sessions.set(sessionId, session);
    return session;
  }

  public getSession(sessionId: string): SessionRecord | undefined {
    const session = this.sessions.get(sessionId);
    if (!session || session.expiresAt < Date.now()) {
      return undefined;
    }
    return session;
  }

  public countUsers(): number {
    return this.users.size;
  }

  public countSessions(): number {
    return this.sessions.size;
  }
}

export const db = new MockDatabase();
