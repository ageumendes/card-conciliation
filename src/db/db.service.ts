import { Global, Inject, Injectable, Logger, Module, OnModuleDestroy } from '@nestjs/common';
import * as Firebird from 'node-firebird';
import { firebirdProvider, FIREBIRD_POOL } from '../config/firebird.provider';
import { normalizeDbRows } from '../utils/db-normalize';

@Injectable()
export class DbService implements OnModuleDestroy {
  private readonly logger = new Logger(DbService.name);

  constructor(@Inject(FIREBIRD_POOL) private readonly pool: any) {}

  async query<T = unknown>(sql: string, params: unknown[] = []): Promise<T[]> {
    return this.select<T>(sql, params);
  }

  async select<T = unknown>(sql: string, params: unknown[] = []): Promise<T[]> {
    return this.withDb((db: any) => this.runSelect<T>(db, sql, params));
  }

  async execute(sql: string, params: unknown[] = []): Promise<unknown> {
    return this.withDb((db: any) => this.runExecute(db, sql, params));
  }

  async transaction<T>(handler: (tx: any) => Promise<T>): Promise<T> {
    return this.withDb(
      (db: any) =>
        new Promise<T>((resolve, reject) => {
          db.transaction(Firebird.ISOLATION_READ_COMMITED, async (err: Error, tx: any) => {
            if (err) {
              reject(err);
              return;
            }
            try {
              const result = await handler(tx);
              tx.commit((commitErr: Error) => {
                if (commitErr) {
                  tx.rollback(() => reject(commitErr));
                  return;
                }
                resolve(result);
              });
            } catch (error) {
              tx.rollback(() => reject(error));
            }
          });
        }),
    );
  }

  async queryTx<T = unknown>(tx: any, sql: string, params: unknown[] = []): Promise<T[]> {
    return new Promise<T[]>((resolve, reject) => {
      tx.query(sql, params, (err: Error, result: T[]) => {
        if (err) {
          reject(err);
          return;
        }
        resolve(normalizeDbRows(result as any) as T[]);
      });
    });
  }

  async executeTx(tx: any, sql: string, params: unknown[] = []): Promise<unknown> {
    return this.runExecuteTx(tx, sql, params);
  }

  private async withDb<T>(handler: (db: any) => Promise<T>): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      this.pool.get((err: Error, db: any) => {
        if (err) {
          this.logger.error('Firebird pool error', err);
          reject(err);
          return;
        }

        handler(db)
          .then(resolve)
          .catch(reject)
          .finally(() => db.detach());
      });
    });
  }

  private async runSelect<T>(db: any, sql: string, params: unknown[]): Promise<T[]> {
    return new Promise<T[]>((resolve, reject) => {
      db.query(sql, params, (err: Error, result: T[]) => {
        if (err) {
          reject(err);
          return;
        }
        resolve(normalizeDbRows(result as any) as T[]);
      });
    });
  }

  private async runExecute(db: any, sql: string, params: unknown[]): Promise<unknown> {
    return new Promise<unknown>((resolve, reject) => {
      db.query(sql, params, (err: Error, result: unknown) => {
        if (err) {
          reject(err);
          return;
        }
        resolve(result);
      });
    });
  }

  private async runExecuteTx(tx: any, sql: string, params: unknown[]): Promise<unknown> {
    return new Promise<unknown>((resolve, reject) => {
      tx.query(sql, params, (err: Error, result: unknown) => {
        if (err) {
          reject(err);
          return;
        }
        resolve(result);
      });
    });
  }

  async onModuleDestroy() {
    if (this.pool && typeof this.pool.destroy === 'function') {
      this.pool.destroy();
    }
  }
}

@Global()
@Module({
  providers: [firebirdProvider, DbService],
  exports: [DbService],
})
export class DbModule {}
