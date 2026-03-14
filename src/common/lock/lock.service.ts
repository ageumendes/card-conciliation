import { Injectable, Logger } from '@nestjs/common';
import { hostname } from 'os';
import { DbService } from '../../db/db.service';

@Injectable()
export class LockService {
  private readonly logger = new Logger(LockService.name);
  private readonly hostName = hostname();

  constructor(private readonly dbService: DbService) {}

  async acquire(lockKey: string, ttlSeconds: number, ownerId: string): Promise<boolean> {
    const now = new Date();
    const expiresAt = new Date(now.getTime() + ttlSeconds * 1000);

    return this.dbService.transaction(async (tx) => {
      const rows = await this.dbService.queryTx<{ OWNER?: string; EXPIRES_AT?: Date }>(
        tx,
        'SELECT OWNER, EXPIRES_AT FROM T_APP_LOCKS WHERE LOCK_KEY = ?',
        [lockKey],
      );

      if (!rows.length) {
        try {
          await this.dbService.executeTx(
            tx,
            'INSERT INTO T_APP_LOCKS (LOCK_KEY, OWNER, ACQUIRED_AT, EXPIRES_AT) VALUES (?, ?, ?, ?)',
            [lockKey, ownerId, now, expiresAt],
          );
          return true;
        } catch (error) {
          if (process.env.DEBUG === 'true') {
            this.logger.debug(`Lock insert failed for ${lockKey}: ${String(error)}`);
          }
          return false;
        }
      }

      const current = rows[0];
      const currentOwner = String(current?.OWNER ?? '').trim();
      const currentExpiry = current?.EXPIRES_AT ? new Date(current.EXPIRES_AT) : null;
      const ownerIsStale = this.isStaleOwner(currentOwner);
      if (currentExpiry && currentExpiry > now && !ownerIsStale) {
        return false;
      }
      if (ownerIsStale) {
        this.logger.warn(`Lock órfão detectado para ${lockKey}: owner=${currentOwner}`);
      }

      await this.dbService.executeTx(
        tx,
        'UPDATE T_APP_LOCKS SET OWNER = ?, ACQUIRED_AT = ?, EXPIRES_AT = ? WHERE LOCK_KEY = ?',
        [ownerId, now, expiresAt, lockKey],
      );
      return true;
    });
  }

  async release(lockKey: string, ownerId: string): Promise<void> {
    await this.dbService.execute(
      'DELETE FROM T_APP_LOCKS WHERE LOCK_KEY = ? AND OWNER = ?',
      [lockKey, ownerId],
    );
  }

  private isStaleOwner(ownerId: string): boolean {
    if (!ownerId) {
      return false;
    }
    const [ownerHostRaw, ownerPidRaw] = ownerId.split(':');
    const ownerHost = String(ownerHostRaw ?? '').trim();
    const ownerPid = Number(ownerPidRaw);
    if (!ownerHost || !Number.isFinite(ownerPid) || ownerPid <= 0) {
      return false;
    }
    if (ownerHost !== this.hostName) {
      return false;
    }
    try {
      process.kill(ownerPid, 0);
      return false;
    } catch (error) {
      const code = (error as NodeJS.ErrnoException)?.code;
      return code === 'ESRCH';
    }
  }
}
