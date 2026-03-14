import { Injectable, Logger } from '@nestjs/common';
import { createHash } from 'crypto';
import { DbService } from '../../db/db.service';

type CanonDuplicateSource = 'INTERDATA' | 'CIELO' | 'SIPAG' | 'SICREDI';

type CleanupSummary = {
  source: CanonDuplicateSource;
  dateFrom: string | null;
  dateTo: string | null;
  groups: number;
  moved: number;
  skippedGroups: number;
};

type SourceConfig = {
  source: CanonDuplicateSource;
  tableName: string;
  activeReconSql: string;
};

type DuplicateExpression = {
  key: string;
  alias: string;
  expr: string;
};

@Injectable()
export class CanonDuplicateCleanupService {
  private readonly logger = new Logger(CanonDuplicateCleanupService.name);
  private archiveReady = false;
  private relationColumnsCache = new Map<string, Set<string>>();

  constructor(private readonly dbService: DbService) {}

  async cleanupAfterImport(
    source: CanonDuplicateSource,
    dateFrom?: string | null,
    dateTo?: string | null,
  ): Promise<CleanupSummary> {
    const config = this.getSourceConfig(source);
    await this.ensureArchiveTable();
    const columns = await this.getRelationColumns(config.tableName);
    const expressions = this.buildDuplicateExpressions(source, columns, 't');
    const expressionMap = new Map(expressions.map((entry) => [entry.key, entry]));
    const selectColumns = expressions.map((entry) => `${entry.expr} AS ${entry.alias}`).join(',\n            ');
    const groupByColumns = expressions.map((entry) => entry.expr).join(',\n            ');
    const orderBy = this.buildSurvivorOrderBy(source, columns, 't', config.activeReconSql);

    const normalizedFrom = dateFrom?.trim() || null;
    const normalizedTo = dateTo?.trim() || normalizedFrom;

    return this.dbService.transaction(async (tx) => {
      const groups = await this.dbService.queryTx<Record<string, unknown> & { TOTAL: number }>(
        tx,
        `
          SELECT
            ${selectColumns},
            COUNT(*) AS TOTAL
          FROM ${config.tableName} t
          WHERE 1 = 1
            ${normalizedFrom ? `AND ${expressionMap.get('CANON_SALE_DATE')?.expr ?? 'NULL'} >= ?` : ''}
            ${normalizedTo ? `AND ${expressionMap.get('CANON_SALE_DATE')?.expr ?? 'NULL'} <= ?` : ''}
          GROUP BY
            ${groupByColumns}
          HAVING COUNT(*) > 1
        `,
        [
          ...(normalizedFrom ? [normalizedFrom] : []),
          ...(normalizedTo ? [normalizedTo] : []),
        ],
      );

      let moved = 0;
      let skippedGroups = 0;

      for (const group of groups) {
        const duplicateConditions = expressions
          .map((entry) => this.nullSafeEquals(entry.expr, group[entry.alias]))
          .join('\n              AND ');
        const rows = await this.dbService.queryTx<Record<string, unknown> & { HAS_ACTIVE_RECON?: number }>(
          tx,
          `
            SELECT
              t.*,
              CASE WHEN EXISTS (${config.activeReconSql}) THEN 1 ELSE 0 END AS HAS_ACTIVE_RECON
            FROM ${config.tableName} t
            WHERE ${duplicateConditions}
            ORDER BY
              ${orderBy}
          `,
          [],
        );

        const activeRows = rows.filter((row) => Number(row.HAS_ACTIVE_RECON ?? 0) === 1);
        if (activeRows.length > 1) {
          skippedGroups += 1;
          this.logger.warn(
            `Canon duplicate cleanup skip: source=${source} multiple_active_recon ids=${activeRows.map((row) => row.ID).join(',')}`,
          );
          continue;
        }

        const survivors = rows.slice(0, 1);
        const duplicates = rows.slice(survivors.length);
        for (const row of duplicates) {
          await this.archiveDuplicateTx(tx, config, row);
          await this.dbService.executeTx(tx, `DELETE FROM ${config.tableName} WHERE ID = ?`, [row.ID]);
          moved += 1;
        }
      }

      const summary: CleanupSummary = {
        source,
        dateFrom: normalizedFrom,
        dateTo: normalizedTo,
        groups: groups.length,
        moved,
        skippedGroups,
      };
      this.logger.log(
        `[CANON DUP CLEANUP] source=${source} dateFrom=${summary.dateFrom ?? 'ALL'} dateTo=${summary.dateTo ?? 'ALL'} groups=${summary.groups} moved=${summary.moved} skippedGroups=${summary.skippedGroups}`,
      );
      return summary;
    });
  }

  private getSourceConfig(source: CanonDuplicateSource): SourceConfig {
    if (source === 'INTERDATA') {
      return {
        source,
        tableName: 'T_INTERDATA_SALES',
        activeReconSql:
          'SELECT 1 FROM T_RECONCILIATION r WHERE r.INTERDATA_ID = t.ID AND COALESCE(r.IS_ACTIVE, 1) = 1',
      };
    }
    return {
      source,
      tableName:
        source === 'CIELO'
          ? 'T_CIELO_SALES'
          : source === 'SIPAG'
            ? 'T_SIPAG_SALES'
            : 'T_SICREDI_SALES',
      activeReconSql:
        `SELECT 1 FROM T_RECONCILIATION r WHERE r.ACQUIRER = '${source}' AND r.ACQUIRER_ID = t.ID AND COALESCE(r.IS_ACTIVE, 1) = 1`,
    };
  }

  private nullSafeEquals(column: string, value: unknown): string {
    if (column === 'NULL') {
      return '1 = 1';
    }
    if (value === null || typeof value === 'undefined') {
      return `${column} IS NULL`;
    }
    if (value instanceof Date) {
      const year = value.getFullYear();
      const month = String(value.getMonth() + 1).padStart(2, '0');
      const day = String(value.getDate()).padStart(2, '0');
      const hours = String(value.getHours()).padStart(2, '0');
      const minutes = String(value.getMinutes()).padStart(2, '0');
      const seconds = String(value.getSeconds()).padStart(2, '0');
      const isDateOnly = hours === '00' && minutes === '00' && seconds === '00';
      const literal = isDateOnly
        ? `${year}-${month}-${day}`
        : `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
      return `${column} = '${literal}'`;
    }
    if (typeof value === 'number' && Number.isFinite(value)) {
      return `${column} = ${value}`;
    }
    if (typeof value === 'boolean') {
      return `${column} = ${value ? 1 : 0}`;
    }
    return `${column} = '${String(value).replace(/'/g, "''")}'`;
  }

  private async getRelationColumns(relation: string): Promise<Set<string>> {
    const normalized = relation.trim().toUpperCase();
    const cached = this.relationColumnsCache.get(normalized);
    if (cached) {
      return cached;
    }
    const rows = await this.dbService.query<{ FIELD_NAME: string }>(
      "SELECT TRIM(rf.RDB$FIELD_NAME) as FIELD_NAME FROM RDB$RELATION_FIELDS rf WHERE rf.RDB$RELATION_NAME = ?",
      [normalized],
    );
    const set = new Set<string>(rows.map((row) => String(row.FIELD_NAME).trim().toUpperCase()));
    this.relationColumnsCache.set(normalized, set);
    return set;
  }

  private pickFirstColumn(columns: Set<string>, candidates: string[]): string | null {
    for (const candidate of candidates) {
      if (columns.has(candidate)) {
        return candidate;
      }
    }
    return null;
  }

  private pickCanonExpr(
    alias: string,
    columns: Set<string>,
    key: string,
    candidates: string[],
    options?: {
      trimBlankToNull?: boolean;
      numericCast?: string;
    },
  ): DuplicateExpression {
    const column = this.pickFirstColumn(columns, candidates);
    if (!column) {
      return { key, alias: key, expr: 'NULL' };
    }
    let expr = `${alias}.${column}`;
    if (options?.trimBlankToNull) {
      expr = `NULLIF(TRIM(COALESCE(${expr}, '')), '')`;
    }
    if (options?.numericCast) {
      expr = `CAST(${expr} AS ${options.numericCast})`;
    }
    return { key, alias: key, expr };
  }

  private buildDuplicateExpressions(source: CanonDuplicateSource, columns: Set<string>, alias: string): DuplicateExpression[] {
    const saleDateExpr = columns.has('CANON_SALE_DATE')
      ? `${alias}.CANON_SALE_DATE`
      : columns.has('SALE_DATETIME')
        ? `CAST(${alias}.SALE_DATETIME AS DATE)`
        : 'NULL';

    if (source === 'INTERDATA') {
      return [
        { key: 'CANON_SALE_DATE', alias: 'CANON_SALE_DATE', expr: saleDateExpr },
        this.pickCanonExpr(alias, columns, 'CANON_METHOD', ['CANON_METHOD'], { trimBlankToNull: true }),
        this.pickCanonExpr(alias, columns, 'CANON_BRAND', ['CANON_BRAND'], { trimBlankToNull: true }),
        this.pickCanonExpr(alias, columns, 'CANON_TERMINAL_NO', ['CANON_TERMINAL_NO', 'TERMINAL_NO'], {
          trimBlankToNull: true,
        }),
        this.pickCanonExpr(alias, columns, 'CANON_GROSS_AMOUNT', ['CANON_GROSS_AMOUNT', 'GROSS_AMOUNT'], {
          numericCast: 'NUMERIC(15,2)',
        }),
        this.pickCanonExpr(alias, columns, 'CANON_INSTALLMENT_NO', ['CANON_INSTALLMENT_NO', 'INSTALLMENT_NO']),
        this.pickCanonExpr(alias, columns, 'CANON_INSTALLMENT_TOTAL', ['CANON_INSTALLMENT_TOTAL', 'INSTALLMENT_TOTAL']),
        this.pickCanonExpr(alias, columns, 'CANON_METHOD_GROUP', ['CANON_METHOD_GROUP'], { trimBlankToNull: true }),
      ];
    }

    return [
      { key: 'CANON_SALE_DATE', alias: 'CANON_SALE_DATE', expr: saleDateExpr },
      this.pickCanonExpr(alias, columns, 'CANON_METHOD', ['CANON_METHOD'], { trimBlankToNull: true }),
      this.pickCanonExpr(alias, columns, 'CANON_BRAND', ['CANON_BRAND'], { trimBlankToNull: true }),
      this.pickCanonExpr(alias, columns, 'CANON_TERMINAL_NO', ['CANON_TERMINAL_NO', 'TERMINAL_NO'], {
        trimBlankToNull: true,
      }),
      this.pickCanonExpr(alias, columns, 'CANON_AUTH_CODE', ['CANON_AUTH_CODE', 'AUTH_CODE', 'AUTH_NO'], {
        trimBlankToNull: true,
      }),
      this.pickCanonExpr(alias, columns, 'CANON_NSU', ['CANON_NSU', 'NSU', 'NSU_DOC', 'TRANSACTION_NO'], {
        trimBlankToNull: true,
      }),
      this.pickCanonExpr(alias, columns, 'CANON_GROSS_AMOUNT', ['CANON_GROSS_AMOUNT', 'GROSS_AMOUNT'], {
        numericCast: 'NUMERIC(15,2)',
      }),
      this.pickCanonExpr(alias, columns, 'CANON_INSTALLMENT_NO', ['CANON_INSTALLMENT_NO', 'INSTALLMENT_NO']),
      this.pickCanonExpr(alias, columns, 'CANON_INSTALLMENT_TOTAL', ['CANON_INSTALLMENT_TOTAL', 'INSTALLMENT_TOTAL']),
      this.pickCanonExpr(alias, columns, 'CANON_METHOD_GROUP', ['CANON_METHOD_GROUP'], { trimBlankToNull: true }),
      this.pickCanonExpr(alias, columns, 'CANON_FEE_AMOUNT', ['CANON_FEE_AMOUNT', 'FEE_AMOUNT'], {
        numericCast: 'NUMERIC(15,4)',
      }),
      this.pickCanonExpr(alias, columns, 'CANON_NET_AMOUNT', ['CANON_NET_AMOUNT', 'NET_AMOUNT'], {
        numericCast: 'NUMERIC(15,4)',
      }),
      this.pickCanonExpr(alias, columns, 'CANON_PERC_TAXA', ['CANON_PERC_TAXA', 'PERC_TAXA'], {
        numericCast: 'NUMERIC(15,4)',
      }),
    ];
  }

  private buildSurvivorOrderBy(
    source: CanonDuplicateSource,
    columns: Set<string>,
    alias: string,
    activeReconSql: string,
  ): string {
    const orderBy = [`CASE WHEN EXISTS (${activeReconSql}) THEN 1 ELSE 0 END DESC`];
    if (source === 'INTERDATA') {
      const saleNoColumn = this.pickFirstColumn(columns, ['SALE_NO']);
      if (saleNoColumn) {
        orderBy.push(`CASE WHEN TRIM(COALESCE(${alias}.${saleNoColumn}, '')) <> '' THEN 1 ELSE 0 END DESC`);
      }
      const authNsuColumn = this.pickFirstColumn(columns, ['AUTH_NSU']);
      if (authNsuColumn) {
        orderBy.push(`CASE WHEN TRIM(COALESCE(${alias}.${authNsuColumn}, '')) <> '' THEN 1 ELSE 0 END DESC`);
      }
    }
    orderBy.push(`${alias}.ID ASC`);
    return orderBy.join(', ');
  }

  private async archiveDuplicateTx(
    tx: any,
    config: SourceConfig,
    row: Record<string, unknown>,
  ): Promise<void> {
    await this.dbService.executeTx(
      tx,
      `
        UPDATE OR INSERT INTO T_CANON_DUPLICATE_ITEMS (
          SOURCE_TABLE, ORIGINAL_ID, SOURCE_PROVIDER, DUPLICATE_KEY,
          SALE_DATETIME, CANON_SALE_DATE, CANON_METHOD_GROUP, CANON_GROSS_AMOUNT,
          CANON_TERMINAL_NO, CANON_AUTH_CODE, CANON_NSU, STATUS_TEXT, ROW_HASH,
          SNAPSHOT_JSON, MOVED_AT, MOVE_REASON
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        MATCHING (SOURCE_TABLE, ORIGINAL_ID)
      `,
      [
        config.tableName,
        row.ID,
        config.source,
        this.buildDuplicateKey(row),
        row.SALE_DATETIME ?? null,
        row.CANON_SALE_DATE ?? null,
        row.CANON_METHOD_GROUP ?? null,
        row.CANON_GROSS_AMOUNT ?? null,
        row.CANON_TERMINAL_NO ?? null,
        row.CANON_AUTH_CODE ?? null,
        row.CANON_NSU ?? null,
        this.resolveStatusText(row),
        row.ROW_HASH ?? null,
        JSON.stringify(row),
        new Date(),
        'canon_post_import_cleanup',
      ],
    );
  }

  private resolveStatusText(row: Record<string, unknown>): string | null {
    const raw = row.STATUS_RAW ?? row.STATUS ?? row.PAY_STATUS ?? null;
    return raw === null || typeof raw === 'undefined' ? null : String(raw);
  }

  private buildDuplicateKey(row: Record<string, unknown>): string {
    const rawKey = [
      'CANON_SALE_DATE',
      'CANON_METHOD',
      'CANON_BRAND',
      'CANON_TERMINAL_NO',
      'CANON_AUTH_CODE',
      'CANON_NSU',
      'CANON_GROSS_AMOUNT',
      'CANON_INSTALLMENT_NO',
      'CANON_INSTALLMENT_TOTAL',
      'CANON_METHOD_GROUP',
      'CANON_FEE_AMOUNT',
      'CANON_NET_AMOUNT',
      'CANON_PERC_TAXA',
    ]
      .map((field) => `${field}=${row[field] ?? ''}`)
      .join('|');
    return createHash('sha256').update(rawKey).digest('hex');
  }

  private async ensureArchiveTable(): Promise<void> {
    if (this.archiveReady) {
      return;
    }
    await this.dbService.execute(`
      EXECUTE BLOCK AS
      BEGIN
        IF (NOT EXISTS(
          SELECT 1 FROM RDB$RELATIONS WHERE TRIM(RDB$RELATION_NAME) = 'T_CANON_DUPLICATE_ITEMS'
        )) THEN
        BEGIN
          EXECUTE STATEMENT '
            CREATE TABLE T_CANON_DUPLICATE_ITEMS (
              SOURCE_TABLE VARCHAR(32) NOT NULL,
              ORIGINAL_ID BIGINT NOT NULL,
              SOURCE_PROVIDER VARCHAR(16) NOT NULL,
              DUPLICATE_KEY VARCHAR(255),
              SALE_DATETIME TIMESTAMP,
              CANON_SALE_DATE DATE,
              CANON_METHOD_GROUP VARCHAR(16),
              CANON_GROSS_AMOUNT NUMERIC(15,2),
              CANON_TERMINAL_NO VARCHAR(32),
              CANON_AUTH_CODE VARCHAR(64),
              CANON_NSU VARCHAR(64),
              STATUS_TEXT VARCHAR(80),
              ROW_HASH VARCHAR(64),
              SNAPSHOT_JSON BLOB SUB_TYPE TEXT,
              MOVED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
              MOVE_REASON VARCHAR(80),
              CONSTRAINT PK_T_CANON_DUPLICATE_ITEMS PRIMARY KEY (SOURCE_TABLE, ORIGINAL_ID)
            )
          ';
        END
      END
    `);
    this.archiveReady = true;
  }
}
