import { Injectable, Logger } from '@nestjs/common';
import { DbService } from '../../db/db.service';

@Injectable()
export class ReconciliationMatchRepository {
  private readonly logger = new Logger(ReconciliationMatchRepository.name);
  private matchColumnsCache?: Set<string>;

  constructor(private readonly dbService: DbService) {}

  private async getMatchColumns(): Promise<Set<string>> {
    if (this.matchColumnsCache) {
      return this.matchColumnsCache;
    }
    const rows = await this.dbService.query<{ FIELD_NAME: string }>(
      "SELECT TRIM(rf.RDB$FIELD_NAME) as FIELD_NAME FROM RDB$RELATION_FIELDS rf WHERE rf.RDB$RELATION_NAME = ?",
      ['T_RECONCILIATION_MATCH'],
    );
    const set = new Set<string>(rows.map((row) => String(row.FIELD_NAME).trim().toUpperCase()));
    this.matchColumnsCache = set;
    return set;
  }

  async getByReconciliationId(reconciliationId: number): Promise<Record<string, any> | null> {
    const rows = await this.dbService.query<Record<string, any>>(
      'SELECT * FROM T_RECONCILIATION_MATCH WHERE RECONCILIATION_ID = ?',
      [reconciliationId],
    );
    return rows[0] ?? null;
  }

  async insertMatch(
    tx: any,
    payload: {
      reconciliationId: number;
      interdataSaleId?: number | null;
      acqProvider: string;
      acqSaleId?: number | null;
      matchRule?: string | null;
      matchMeta?: string | null;
      matchLayer?: number | null;
      matchConfidence?: number | null;
      matchReason?: string | null;
      runId?: number | null;
      erpSnapshot?: string | null;
      acqSnapshot?: string | null;
      metaJson?: string | null;
    },
  ) {
    const availableColumns = await this.getMatchColumns();
    const baseColumns = [
      'RECONCILIATION_ID',
      'INTERDATA_SALE_ID',
      'ACQ_PROVIDER',
      'ACQ_SALE_ID',
      'MATCH_RULE',
      'MATCH_META',
      'CREATED_AT',
    ];
    const baseValues = [
      payload.reconciliationId,
      payload.interdataSaleId ?? null,
      payload.acqProvider,
      payload.acqSaleId ?? null,
      payload.matchRule ?? null,
      payload.matchMeta ?? null,
      new Date(),
    ];
    const extras: Array<[string, unknown]> = [
      ['MATCH_LAYER', payload.matchLayer ?? null],
      ['MATCH_CONFIDENCE', payload.matchConfidence ?? null],
      ['MATCH_REASON', payload.matchReason ?? null],
      ['RUN_ID', payload.runId ?? null],
      ['ERP_SNAPSHOT', payload.erpSnapshot ?? null],
      ['ACQ_SNAPSHOT', payload.acqSnapshot ?? null],
      ['META_JSON', payload.metaJson ?? null],
    ];
    const filteredExtras = extras.filter(([column]) => availableColumns.has(column));
    const columns = [...baseColumns, ...filteredExtras.map(([column]) => column)];
    const params = [...baseValues, ...filteredExtras.map(([, value]) => value ?? null)];
    const sql =
      `INSERT INTO T_RECONCILIATION_MATCH (${columns.join(', ')}) VALUES (${columns.map(() => '?').join(', ')})`;
    try {
      return await this.dbService.executeTx(tx, sql, params);
    } catch (error) {
      if (this.isMissingMatchLayerColumn(error)) {
        this.logger.error(
          'Schema desatualizado: rode migration 2026_01_add_match_layer_confidence_reason.sql',
        );
        const fallbackSql =
          'INSERT INTO T_RECONCILIATION_MATCH (RECONCILIATION_ID, INTERDATA_SALE_ID, ACQ_PROVIDER, ACQ_SALE_ID, MATCH_RULE, MATCH_META, CREATED_AT) VALUES (?, ?, ?, ?, ?, ?, ?)';
        const fallbackParams = [
          payload.reconciliationId,
          payload.interdataSaleId ?? null,
          payload.acqProvider,
          payload.acqSaleId ?? null,
          payload.matchRule ?? null,
          payload.matchMeta ?? null,
          params[9],
        ];
        return this.dbService.executeTx(tx, fallbackSql, fallbackParams);
      }
      throw error;
    }
  }

  private isMissingMatchLayerColumn(error: unknown): boolean {
    const code = Number((error as { code?: number }).code ?? NaN);
    const message = String((error as { message?: string }).message ?? '').toUpperCase();
    if (code === -206 || message.includes('SQL ERROR CODE = -206') || message.includes('COLUMN UNKNOWN')) {
      return (
        message.includes('MATCH_LAYER') ||
        message.includes('MATCH_CONFIDENCE') ||
        message.includes('MATCH_REASON')
      );
    }
    return false;
  }
}
