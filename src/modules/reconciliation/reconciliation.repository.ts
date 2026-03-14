import { Injectable, Logger } from '@nestjs/common';
import { DbService } from '../../db/db.service';
import { AuditDuplicateRowRaw, DuplicateGroup, DuplicateKey } from './reconciliation.dto';

@Injectable()
export class ReconciliationRepository {
  private readonly logger = new Logger(ReconciliationRepository.name);
  private reconciliationColumnsCache?: Set<string>;
  private relationColumnsCache = new Map<string, Set<string>>();

  constructor(private readonly dbService: DbService) {}

  private async getReconciliationColumns(): Promise<Set<string>> {
    if (this.reconciliationColumnsCache) {
      return this.reconciliationColumnsCache;
    }
    const rows = await this.dbService.query<{ FIELD_NAME: string }>(
      "SELECT TRIM(rf.RDB$FIELD_NAME) as FIELD_NAME FROM RDB$RELATION_FIELDS rf WHERE rf.RDB$RELATION_NAME = ?",
      ['T_RECONCILIATION'],
    );
    const set = new Set<string>(rows.map((row) => String(row.FIELD_NAME).trim().toUpperCase()));
    this.reconciliationColumnsCache = set;
    return set;
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

  private buildDuplicateExpressions(columns: Set<string>, alias: string) {
    const saleDateExpr = columns.has('CANON_SALE_DATE')
      ? `${alias}.CANON_SALE_DATE`
      : columns.has('SALE_DATETIME')
        ? `CAST(${alias}.SALE_DATETIME AS DATE)`
        : 'NULL';
    const methodGroupColumn = this.pickFirstColumn(columns, [
      'CANON_METHOD_GROUP',
      'METHOD_GROUP',
      'PAYMENT_METHOD',
      'CREDIT_DEBIT_IND',
    ]);
    const grossAmountColumn = this.pickFirstColumn(columns, ['CANON_GROSS_AMOUNT', 'GROSS_AMOUNT']);
    const terminalColumn = this.pickFirstColumn(columns, ['CANON_TERMINAL_NO', 'TERMINAL_NO']);
    const nsuColumn = this.pickFirstColumn(columns, ['CANON_NSU', 'NSU', 'NSU_DOC', 'TRANSACTION_NO']);
    const authCodeColumn = this.pickFirstColumn(columns, ['CANON_AUTH_CODE', 'AUTH_CODE', 'AUTH_NO']);

    return {
      canonSaleDate: saleDateExpr,
      canonMethodGroup: methodGroupColumn ? `${alias}.${methodGroupColumn}` : 'NULL',
      canonGrossAmount: grossAmountColumn ? `${alias}.${grossAmountColumn}` : 'NULL',
      canonTerminalNo: terminalColumn ? `${alias}.${terminalColumn}` : 'NULL',
      canonNsu: nsuColumn ? `${alias}.${nsuColumn}` : 'NULL',
      canonAuthCode: authCodeColumn ? `${alias}.${authCodeColumn}` : 'NULL',
      hasCanonSaleDate: columns.has('CANON_SALE_DATE'),
      hasSaleDatetime: columns.has('SALE_DATETIME'),
    };
  }

  private buildMethodGroupExpression(
    columns: Set<string>,
    alias: string,
    extraGroupColumns: string[] = [],
    pixHintColumns: string[] = [],
  ) {
    const groupColumns = [
      'CANON_METHOD_GROUP',
      'METHOD_GROUP',
      ...extraGroupColumns,
    ].filter((column) => columns.has(column));
    const methodColumns = [
      'CANON_METHOD',
      'METHOD',
      'PAYMENT_METHOD',
      'CREDIT_DEBIT_IND',
    ].filter((column) => columns.has(column));
    const pixHintResolved = pixHintColumns.filter((column) => columns.has(column));

    const groupExpr =
      groupColumns.length > 1
        ? `COALESCE(${groupColumns.map((column) => `${alias}.${column}`).join(', ')})`
        : groupColumns.length === 1
          ? `${alias}.${groupColumns[0]}`
          : null;
    const methodExpr =
      methodColumns.length > 1
        ? `COALESCE(${methodColumns.map((column) => `${alias}.${column}`).join(', ')})`
        : methodColumns.length === 1
          ? `${alias}.${methodColumns[0]}`
          : null;

    if (groupExpr && methodExpr) {
      return `COALESCE(${groupExpr}, CASE WHEN UPPER(${methodExpr}) CONTAINING 'PIX' THEN 'PIX' ELSE 'CARD' END)`;
    }
    if (groupExpr) {
      return `COALESCE(${groupExpr}, 'CARD')`;
    }
    if (methodExpr) {
      return `CASE WHEN UPPER(${methodExpr}) CONTAINING 'PIX' THEN 'PIX' ELSE 'CARD' END`;
    }
    if (pixHintResolved.length) {
      const pixHintExpr =
        pixHintResolved.length > 1
          ? `COALESCE(${pixHintResolved.map((column) => `${alias}.${column}`).join(', ')})`
          : `${alias}.${pixHintResolved[0]}`;
      return `CASE WHEN UPPER(${pixHintExpr}) CONTAINING 'PIX' THEN 'PIX' ELSE 'CARD' END`;
    }
    return "'CARD'";
  }

  private buildSaleDateExpression(columns: Set<string>, alias: string) {
    if (columns.has('CANON_SALE_DATE')) {
      return `CAST(${alias}.CANON_SALE_DATE AS DATE)`;
    }
    if (columns.has('SALE_DATETIME')) {
      return `CAST(${alias}.SALE_DATETIME AS DATE)`;
    }
    return 'NULL';
  }

  private buildGrossAmountExpression(columns: Set<string>, alias: string) {
    const grossColumn = this.pickFirstColumn(columns, ['CANON_GROSS_AMOUNT', 'GROSS_AMOUNT']);
    if (!grossColumn) {
      return 'NULL';
    }
    return `CAST(${alias}.${grossColumn} AS NUMERIC(18,2))`;
  }

  async auditDuplicates(params: {
    acquirer: 'CIELO' | 'SIPAG' | 'SICREDI' | 'ALL';
    from: string;
    to: string;
  }): Promise<AuditDuplicateRowRaw[]> {
    const unionParts: string[] = [];
    const values: unknown[] = [];

    const pushSource = async (label: string, table: string, extraGroupColumns: string[] = []) => {
      const alias = 't';
      const set = await this.getRelationColumns(table);
      const saleDateExpr = this.buildSaleDateExpression(set, alias);
      const methodExpr = this.buildMethodGroupExpression(set, alias, extraGroupColumns, ['ACQUIRER', 'ACQ_PROVIDER']);
      const grossExpr = this.buildGrossAmountExpression(set, alias);
      const dateColumn = set.has('CANON_SALE_DATE') ? 'CANON_SALE_DATE' : 'SALE_DATETIME';
      const dateExpr = `CAST(${alias}.${dateColumn} AS DATE)`;
      const activeFilter =
        table === 'T_RECONCILIATION' && set.has('IS_ACTIVE')
          ? ` AND COALESCE(${alias}.IS_ACTIVE, 1) = 1`
          : '';
      values.push(params.from, params.to);
      unionParts.push(
        `SELECT '${label}' AS SRC, ${saleDateExpr} AS SALE_DATE, ${methodExpr} AS METHOD_GROUP, ` +
          `${grossExpr} AS GROSS_AMOUNT, ${alias}.ID AS ID ` +
          `FROM ${table} ${alias} WHERE ${dateExpr} BETWEEN ? AND ?${activeFilter}`,
      );
    };

    await pushSource('ERP', 'T_INTERDATA_SALES');

    if (params.acquirer === 'ALL' || params.acquirer === 'CIELO') {
      await pushSource('ACQ', 'T_CIELO_SALES');
    }
    if (params.acquirer === 'ALL' || params.acquirer === 'SIPAG') {
      await pushSource('ACQ', 'T_SIPAG_SALES');
    }
    if (params.acquirer === 'ALL' || params.acquirer === 'SICREDI') {
      await pushSource('ACQ', 'T_SICREDI_SALES');
    }

    await pushSource('RECON', 'T_RECONCILIATION', ['MATCH_TYPE']);

    const baseSql = unionParts.join(' UNION ALL ');

    const sql =
      'WITH BASE AS (' +
      baseSql +
      ') ' +
      'SELECT ' +
      'SALE_DATE, METHOD_GROUP, GROSS_AMOUNT, ' +
      "SUM(CASE WHEN SRC = 'ERP' THEN 1 ELSE 0 END) AS ERP_COUNT, " +
      "SUM(CASE WHEN SRC = 'ACQ' THEN 1 ELSE 0 END) AS ACQ_COUNT, " +
      "SUM(CASE WHEN SRC = 'RECON' THEN 1 ELSE 0 END) AS RECON_COUNT, " +
      "LIST(IIF(SRC = 'ERP', TRIM(CAST(ID AS VARCHAR(20))), NULL), ',') AS ERP_IDS, " +
      "LIST(IIF(SRC = 'ACQ', TRIM(CAST(ID AS VARCHAR(20))), NULL), ',') AS ACQ_IDS, " +
      "LIST(IIF(SRC = 'RECON', TRIM(CAST(ID AS VARCHAR(20))), NULL), ',') AS RECON_IDS " +
      'FROM BASE ' +
      'GROUP BY SALE_DATE, METHOD_GROUP, GROSS_AMOUNT ' +
      'ORDER BY SALE_DATE, METHOD_GROUP, GROSS_AMOUNT';

    if (process.env.DEBUG === 'true') {
      this.logger.debug(`auditDuplicates SQL: ${sql}`);
      this.logger.debug(`auditDuplicates params: ${JSON.stringify(values)}`);
    }

    return this.dbService.query<AuditDuplicateRowRaw>(sql, values);
  }

  async listPendingBySourcePaged(params: {
    source: 'INTERDATA' | 'CIELO' | 'SIPAG' | 'SICREDI';
    sortBy: 'saleDatetime' | 'grossAmount';
    sortDir: 'asc' | 'desc';
    date?: string;
  }): Promise<any[]> {
    const viewName =
      params.source === 'INTERDATA'
        ? 'V_PENDING_INTERDATA'
        : params.source === 'CIELO'
          ? 'V_PENDING_CIELO'
          : params.source === 'SIPAG'
            ? 'V_PENDING_SIPAG'
            : 'V_PENDING_SICREDI';
    const alias = params.source === 'INTERDATA' ? 'i' : 'v';
    const columns = await this.getRelationColumns(viewName);
    const hasCanonSaleDate = columns.has('CANON_SALE_DATE');
    const hasCanonGrossAmount = columns.has('CANON_GROSS_AMOUNT');

    const filters: string[] = [];
    const values: unknown[] = [];
    if (params.date) {
      if (hasCanonSaleDate) {
        filters.push(`${alias}.CANON_SALE_DATE = ?`);
        values.push(params.date);
      } else {
        filters.push(`CAST(${alias}.SALE_DATETIME AS DATE) = CAST(? AS DATE)`);
        values.push(params.date);
      }
    }
    const where = filters.length ? `WHERE ${filters.join(' AND ')}` : '';

    const orderColumn =
      params.sortBy === 'grossAmount'
        ? hasCanonGrossAmount
          ? `${alias}.CANON_GROSS_AMOUNT`
          : `${alias}.GROSS_AMOUNT`
        : `${alias}.SALE_DATETIME`;
    const orderDir = params.sortDir === 'desc' ? 'DESC' : 'ASC';

    if (params.source === 'INTERDATA') {
      // ⚠️ Paginação desabilitada temporariamente para análise de importação duplicada (DEV)
      const sql =
        `SELECT ${alias}.*, s.LAST_RUN_ID, s.LAST_ATTEMPT_AT, s.LAST_REASON, s.LAST_DETAILS, s.ATTEMPTS ` +
        `FROM V_PENDING_INTERDATA ${alias} LEFT JOIN T_RECON_STATUS s ON s.INTERDATA_ID = ${alias}.ID ` +
        `${where} ORDER BY ${orderColumn} ${orderDir}`;
      if (process.env.NODE_ENV === 'development') {
        this.logger.log(`SQL listPendingBySource: ${sql}`);
      }
      return this.dbService.query(sql, values);
    }
    // ⚠️ Paginação desabilitada temporariamente para análise de importação duplicada (DEV)
    const sql =
      `SELECT ${alias}.* FROM ${viewName} ${alias} ${where} ` +
      `ORDER BY ${orderColumn} ${orderDir}`;
    if (process.env.NODE_ENV === 'development') {
      this.logger.log(`SQL listPendingBySource: ${sql}`);
    }
    return this.dbService.query(sql, values);
  }

  async listDuplicateGroups(params: {
    source: 'INTERDATA' | 'CIELO' | 'SIPAG' | 'SICREDI';
    date?: string;
  }): Promise<DuplicateGroup[]> {
    const tableName =
      params.source === 'INTERDATA'
        ? 'T_INTERDATA_SALES'
        : params.source === 'CIELO'
          ? 'T_CIELO_SALES'
          : params.source === 'SIPAG'
            ? 'T_SIPAG_SALES'
            : 'T_SICREDI_SALES';
    const alias = 't';
    const columns = await this.getRelationColumns(tableName);
    const expressions = this.buildDuplicateExpressions(columns, alias);
    const selectParts = [
      `${expressions.canonSaleDate} AS CANON_SALE_DATE`,
      `${expressions.canonMethodGroup} AS CANON_METHOD_GROUP`,
      `${expressions.canonGrossAmount} AS CANON_GROSS_AMOUNT`,
      `${expressions.canonTerminalNo} AS CANON_TERMINAL_NO`,
      `${expressions.canonNsu} AS CANON_NSU`,
      `${expressions.canonAuthCode} AS CANON_AUTH_CODE`,
      'COUNT(*) AS TOTAL',
    ];
    if (expressions.hasSaleDatetime) {
      selectParts.push(
        `MIN(${alias}.SALE_DATETIME) AS SALE_DATETIME_MIN`,
        `MAX(${alias}.SALE_DATETIME) AS SALE_DATETIME_MAX`,
      );
    }

    const whereParts: string[] = [];
    const baseParams: unknown[] = [];
    if (params.date) {
      if (expressions.hasCanonSaleDate) {
        whereParts.push(`${alias}.CANON_SALE_DATE = ?`);
        baseParams.push(params.date);
      } else if (expressions.hasSaleDatetime) {
        whereParts.push(`CAST(${alias}.SALE_DATETIME AS DATE) = CAST(? AS DATE)`);
        baseParams.push(params.date);
      }
    }
    if (params.source === 'INTERDATA') {
      whereParts.push(
        `NOT EXISTS (SELECT 1 FROM T_RECONCILIATION r WHERE r.INTERDATA_ID = ${alias}.ID AND COALESCE(r.IS_ACTIVE, 1) = 1)`,
      );
    } else {
      whereParts.push(
        `NOT EXISTS (SELECT 1 FROM T_RECONCILIATION r WHERE r.ACQUIRER = ? AND r.ACQUIRER_ID = ${alias}.ID AND COALESCE(r.IS_ACTIVE, 1) = 1)`,
      );
      baseParams.push(params.source);
    }
    const whereClause = whereParts.length ? `WHERE ${whereParts.join(' AND ')}` : '';
    const groupByParts = [
      expressions.canonSaleDate,
      expressions.canonMethodGroup,
      expressions.canonGrossAmount,
      expressions.canonTerminalNo,
      expressions.canonNsu,
      expressions.canonAuthCode,
    ];
    // ⚠️ Paginação desabilitada temporariamente para análise de importação duplicada (DEV)
    const groupsSql =
      `SELECT ${selectParts.join(', ')} FROM ${tableName} ${alias} ${whereClause} ` +
      `GROUP BY ${groupByParts.join(', ')} HAVING COUNT(*) > 1 ORDER BY COUNT(*) DESC`;
    if (process.env.NODE_ENV === 'development') {
      this.logger.log(`SQL listDuplicateGroups: ${groupsSql}`);
    }
    const groupRows = await this.dbService.query<any>(groupsSql, baseParams);

    const results: DuplicateGroup[] = [];
    for (const row of groupRows) {
      const key: DuplicateKey = {
        canonSaleDate: row.CANON_SALE_DATE ?? null,
        canonMethodGroup: row.CANON_METHOD_GROUP ?? null,
        canonGrossAmount:
          row.CANON_GROSS_AMOUNT === null || typeof row.CANON_GROSS_AMOUNT === 'undefined'
            ? null
            : Number(row.CANON_GROSS_AMOUNT),
        canonTerminalNo: row.CANON_TERMINAL_NO ?? null,
        canonNsu: row.CANON_NSU ?? null,
        canonAuthCode: row.CANON_AUTH_CODE ?? null,
      };
      const keyWhereParts: string[] = [];
      const keyParams: unknown[] = [];
      const addKeyCondition = (expr: string, value: unknown) => {
        if (value === null || typeof value === 'undefined') {
          keyWhereParts.push(`${expr} IS NULL`);
          return;
        }
        keyWhereParts.push(`${expr} = ?`);
        keyParams.push(value);
      };
      addKeyCondition(expressions.canonSaleDate, key.canonSaleDate);
      addKeyCondition(expressions.canonMethodGroup, key.canonMethodGroup);
      addKeyCondition(expressions.canonGrossAmount, key.canonGrossAmount);
      addKeyCondition(expressions.canonTerminalNo, key.canonTerminalNo);
      addKeyCondition(expressions.canonNsu, key.canonNsu);
      addKeyCondition(expressions.canonAuthCode, key.canonAuthCode);

      const idsWhereParts = [...whereParts, ...keyWhereParts];
      const idsWhereClause = idsWhereParts.length ? `WHERE ${idsWhereParts.join(' AND ')}` : '';
      const ids = await this.dbService.query<{ ID: number }>(
        `SELECT ${alias}.ID FROM ${tableName} ${alias} ${idsWhereClause} ORDER BY ${alias}.ID`,
        [...baseParams, ...keyParams],
      );

      results.push({
        key,
        count: Number(row.TOTAL ?? 0),
        ids: ids.map((item) => item.ID),
        sampleSaleDatetimeMin: row.SALE_DATETIME_MIN ?? null,
        sampleSaleDatetimeMax: row.SALE_DATETIME_MAX ?? null,
      });
    }

    return results;
  }

  async getInterdataSale(tx: any, id: number): Promise<any | null> {
    const rows = await this.dbService.queryTx(tx, 'SELECT FIRST 1 * FROM T_INTERDATA_SALES WHERE ID = ?', [id]);
    return rows[0] ?? null;
  }

  async getCieloSale(tx: any, id: number): Promise<any | null> {
    const rows = await this.dbService.queryTx(tx, 'SELECT FIRST 1 * FROM T_CIELO_SALES WHERE ID = ?', [id]);
    return rows[0] ?? null;
  }

  async getSipagSale(tx: any, id: number): Promise<any | null> {
    const rows = await this.dbService.queryTx(tx, 'SELECT FIRST 1 * FROM T_SIPAG_SALES WHERE ID = ?', [id]);
    return rows[0] ?? null;
  }

  async deactivateCancelledReconciliations(tx: any) {
    const reasonSuffix = 'status_cancelado_auto_cleanup';
    const sipagSql = `
      UPDATE T_RECONCILIATION r
         SET IS_ACTIVE = 0,
             NOTES = CASE
               WHEN COALESCE(r.NOTES, '') = '' THEN ?
               WHEN UPPER(COALESCE(r.NOTES, '')) CONTAINING UPPER(?) THEN r.NOTES
               ELSE r.NOTES || ' | ' || ?
             END
       WHERE COALESCE(r.IS_ACTIVE, 1) = 1
         AND UPPER(COALESCE(r.ACQUIRER, '')) = 'SIPAG'
         AND EXISTS (
           SELECT 1
             FROM T_SIPAG_SALES s
            WHERE s.ID = r.ACQUIRER_ID
              AND (
                UPPER(COALESCE(s.STATUS, '')) CONTAINING 'CANCEL'
                OR UPPER(COALESCE(s.STATUS, '')) CONTAINING 'UNDONE'
                OR UPPER(COALESCE(s.STATUS, '')) CONTAINING 'UNAUTHORIZED - 90'
              )
         )
    `;
    const sicrediSql = `
      UPDATE T_RECONCILIATION r
         SET IS_ACTIVE = 0,
             NOTES = CASE
               WHEN COALESCE(r.NOTES, '') = '' THEN ?
               WHEN UPPER(COALESCE(r.NOTES, '')) CONTAINING UPPER(?) THEN r.NOTES
               ELSE r.NOTES || ' | ' || ?
             END
       WHERE COALESCE(r.IS_ACTIVE, 1) = 1
         AND UPPER(COALESCE(r.ACQUIRER, '')) = 'SICREDI'
         AND EXISTS (
           SELECT 1
             FROM T_SICREDI_SALES s
            WHERE s.ID = r.ACQUIRER_ID
              AND (
                UPPER(COALESCE(s.STATUS, '')) CONTAINING 'CANCEL'
                OR UPPER(COALESCE(s.STATUS, '')) CONTAINING 'UNDONE'
                OR UPPER(COALESCE(s.STATUS, '')) CONTAINING 'UNAUTHORIZED - 90'
                OR UPPER(COALESCE(s.PAY_STATUS, '')) CONTAINING 'CANCEL'
                OR UPPER(COALESCE(s.PAY_STATUS, '')) CONTAINING 'UNDONE'
                OR UPPER(COALESCE(s.PAY_STATUS, '')) CONTAINING 'UNAUTHORIZED - 90'
              )
         )
    `;
    const cieloSql = `
      UPDATE T_RECONCILIATION r
         SET IS_ACTIVE = 0,
             NOTES = CASE
               WHEN COALESCE(r.NOTES, '') = '' THEN ?
               WHEN UPPER(COALESCE(r.NOTES, '')) CONTAINING UPPER(?) THEN r.NOTES
               ELSE r.NOTES || ' | ' || ?
             END
       WHERE COALESCE(r.IS_ACTIVE, 1) = 1
         AND UPPER(COALESCE(r.ACQUIRER, '')) = 'CIELO'
         AND EXISTS (
           SELECT 1
             FROM T_CIELO_SALES c
            WHERE c.ID = r.ACQUIRER_ID
              AND UPPER(COALESCE(c.STATUS, '')) CONTAINING 'CANCEL'
         )
    `;
    const params = [reasonSuffix, reasonSuffix, reasonSuffix];
    const sipag = await this.dbService.executeTx(tx, sipagSql, params);
    const sicredi = await this.dbService.executeTx(tx, sicrediSql, params);
    const cielo = await this.dbService.executeTx(tx, cieloSql, params);
    return {
      sipag: Number(sipag ?? 0),
      sicredi: Number(sicredi ?? 0),
      cielo: Number(cielo ?? 0),
      total: Number(sipag ?? 0) + Number(sicredi ?? 0) + Number(cielo ?? 0),
    };
  }

  async existsReconciliationByInterdata(tx: any, id: number) {
    const rows = await this.dbService.queryTx<{ ID: number }>(
      tx,
      'SELECT FIRST 1 ID FROM T_RECONCILIATION WHERE INTERDATA_ID = ? AND COALESCE(IS_ACTIVE, 1) = 1',
      [id],
    );
    return rows.length > 0;
  }

  async existsReconciliationByAcquirer(tx: any, acquirer: string, id: number) {
    const rows = await this.dbService.queryTx<{ ID: number }>(
      tx,
      'SELECT FIRST 1 ID FROM T_RECONCILIATION WHERE ACQUIRER = ? AND ACQUIRER_ID = ? AND COALESCE(IS_ACTIVE, 1) = 1',
      [acquirer, id],
    );
    return rows.length > 0;
  }

  async getReconciliationId(tx: any, interdataId: number, acquirer: string, acquirerId: number) {
    const rows = await this.dbService.queryTx<{ ID: number }>(
      tx,
      'SELECT FIRST 1 ID FROM T_RECONCILIATION WHERE INTERDATA_ID = ? AND ACQUIRER = ? AND ACQUIRER_ID = ? AND COALESCE(IS_ACTIVE, 1) = 1',
      [interdataId, acquirer, acquirerId],
    );
    return rows[0]?.ID ?? null;
  }

  listInterdataSales(options: { limit: number; dateFrom?: string; dateTo?: string }): Promise<any[]> {
    const conditions: string[] = [];
    const params: unknown[] = [];
    if (options.dateFrom) {
      conditions.push('SALE_DATETIME >= ?');
      params.push(`${options.dateFrom} 00:00:00`);
    }
    if (options.dateTo) {
      conditions.push('SALE_DATETIME < ?');
      const [year, month, day] = options.dateTo.split('-').map(Number);
      const nextDay = new Date(year, month - 1, day + 1, 0, 0, 0, 0);
      const yyyy = nextDay.getFullYear();
      const mm = String(nextDay.getMonth() + 1).padStart(2, '0');
      const dd = String(nextDay.getDate()).padStart(2, '0');
      params.push(`${yyyy}-${mm}-${dd} 00:00:00`);
    }
    const where = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';
    const sql =
      `SELECT FIRST ${options.limit} * FROM V_PENDING_INTERDATA ${where} ` +
      'ORDER BY SALE_DATETIME ASC';
    return this.dbService.query(sql, params);
  }

  listCieloByIdentifiers(identifiers: string[]): Promise<any[]> {
    if (!identifiers.length) {
      return Promise.resolve([]);
    }
    const params = [...identifiers, ...identifiers, ...identifiers];
    const sql =
      'SELECT * FROM V_PENDING_CIELO WHERE ' +
      '(' +
      `NSU_DOC IN (${identifiers.map(() => '?').join(',')}) ` +
      `OR AUTH_CODE IN (${identifiers.map(() => '?').join(',')}) ` +
      `OR TID IN (${identifiers.map(() => '?').join(',')})` +
      ')';
    return this.dbService.query(sql, params);
  }

  listSipagByIdentifiers(identifiers: string[]): Promise<any[]> {
    if (!identifiers.length) {
      return Promise.resolve([]);
    }
    const params = [...identifiers, ...identifiers, ...identifiers, ...identifiers];
    const sql =
      'SELECT * FROM V_PENDING_SIPAG WHERE ' +
      '(' +
      `TRANSACTION_NO IN (${identifiers.map(() => '?').join(',')}) ` +
      `OR AUTH_NO IN (${identifiers.map(() => '?').join(',')}) ` +
      `OR SALE_ID IN (${identifiers.map(() => '?').join(',')}) ` +
      `OR YOUR_NUMBER IN (${identifiers.map(() => '?').join(',')})` +
      ')';
    return this.dbService.query(sql, params);
  }

  listCieloByPixNsuAmount(options: {
    nsus: string[];
    grossAmount: number;
    startTs: string;
    endTs: string;
  }): Promise<any[]> {
    if (!options.nsus.length) {
      return Promise.resolve([]);
    }
    const placeholders = options.nsus.map(() => '?').join(',');
    const sql =
      'SELECT * FROM V_PENDING_CIELO WHERE ' +
      `(NSU_DOC IN (${placeholders}) OR E_NSU_DOC IN (${placeholders})) ` +
      'AND SALE_DATETIME BETWEEN ? AND ? ' +
      'AND GROSS_AMOUNT = ?';
    const params = [...options.nsus, ...options.nsus, options.startTs, options.endTs, options.grossAmount];
    return this.dbService.query(sql, params);
  }

  listCieloByPixAmountWindow(options: {
    grossAmount: number;
    startTs: string;
    endTs: string;
  }): Promise<any[]> {
    const sql =
      'SELECT * FROM V_PENDING_CIELO WHERE ' +
      'SALE_DATETIME BETWEEN ? AND ? ' +
      'AND GROSS_AMOUNT = ?';
    return this.dbService.query(sql, [options.startTs, options.endTs, options.grossAmount]);
  }

  listCieloByDatetimeAmount(options: {
    startTs: string;
    endTs: string;
    grossAmount: number;
    amountTolerance: number;
  }): Promise<any[]> {
    const sql =
      'SELECT * FROM V_PENDING_CIELO WHERE ' +
      'SALE_DATETIME BETWEEN ? AND ? ' +
      'AND ABS(GROSS_AMOUNT - ?) <= ?';
    return this.dbService.query(sql, [
      options.startTs,
      options.endTs,
      options.grossAmount,
      options.amountTolerance,
    ]);
  }

  listSipagByDatetimeAmount(options: {
    startTs: string;
    endTs: string;
    grossAmount: number;
    amountTolerance: number;
  }): Promise<any[]> {
    const sql =
      'SELECT * FROM V_PENDING_SIPAG WHERE ' +
      'SALE_DATETIME BETWEEN ? AND ? ' +
      'AND ABS(GROSS_AMOUNT - ?) <= ?';
    return this.dbService.query(sql, [
      options.startTs,
      options.endTs,
      options.grossAmount,
      options.amountTolerance,
    ]);
  }

  listCieloByAmountDayBrand(options: {
    saleDate: string;
    grossAmount: number;
    brand?: string;
    amountTolerance: number;
  }) {
    const sql =
      'SELECT * FROM V_PENDING_CIELO WHERE ' +
      'CAST(SALE_DATETIME AS DATE) = CAST(? AS DATE) ' +
      'AND ABS(GROSS_AMOUNT - ?) <= ? ' +
      (options.brand ? 'AND UPPER(BRAND) = ?' : '');
    const params: unknown[] = [options.saleDate, options.grossAmount, options.amountTolerance];
    if (options.brand) {
      params.push(options.brand);
    }
    return this.dbService.query(sql, params);
  }

  listSipagByAmountDayBrand(options: {
    saleDate: string;
    grossAmount: number;
    brand?: string;
    amountTolerance: number;
  }) {
    const sql =
      'SELECT * FROM V_PENDING_SIPAG WHERE ' +
      'CAST(SALE_DATETIME AS DATE) = CAST(? AS DATE) ' +
      'AND ABS(GROSS_AMOUNT - ?) <= ? ' +
      (options.brand ? 'AND UPPER(BRAND) = ?' : '');
    const params: unknown[] = [options.saleDate, options.grossAmount, options.amountTolerance];
    if (options.brand) {
      params.push(options.brand);
    }
    return this.dbService.query(sql, params);
  }

  async existsReconciliation(tx: any, interdataId: number, acquirer: string, acquirerId: number) {
    const rows = await this.dbService.queryTx<{ ID: number }>(
      tx,
      'SELECT FIRST 1 ID FROM T_RECONCILIATION WHERE INTERDATA_ID = ? AND ACQUIRER = ? AND ACQUIRER_ID = ? AND COALESCE(IS_ACTIVE, 1) = 1',
      [interdataId, acquirer, acquirerId],
    );
    return rows.length > 0;
  }

  async existsInterdata(tx: any, id: number) {
    const rows = await this.dbService.queryTx<{ ID: number }>(
      tx,
      'SELECT FIRST 1 ID FROM T_INTERDATA_SALES WHERE ID = ?',
      [id],
    );
    return rows.length > 0;
  }

  async existsCielo(tx: any, id: number) {
    const rows = await this.dbService.queryTx<{ ID: number }>(
      tx,
      'SELECT FIRST 1 ID FROM T_CIELO_SALES WHERE ID = ?',
      [id],
    );
    return rows.length > 0;
  }

  async existsSipag(tx: any, id: number) {
    const rows = await this.dbService.queryTx<{ ID: number }>(
      tx,
      'SELECT FIRST 1 ID FROM T_SIPAG_SALES WHERE ID = ?',
      [id],
    );
    return rows.length > 0;
  }

  async existsSicredi(tx: any, id: number) {
    const rows = await this.dbService.queryTx<{ ID: number }>(
      tx,
      'SELECT FIRST 1 ID FROM T_SICREDI_SALES WHERE ID = ?',
      [id],
    );
    return rows.length > 0;
  }

  async insertReconciliation(
    tx: any,
    payload: unknown[],
    meta?: { source?: string; reason?: string | null; notes?: string | null },
    extras?: Record<string, unknown>,
  ) {
    const baseColumns = [
      'INTERDATA_ID',
      'ACQUIRER_ID',
      'ACQUIRER',
      'ACQ_PROVIDER',
      'SALE_NO',
      'AUTH_NSU',
      'ACQ_AUTH_CODE',
      'ACQ_NSU',
      'SALE_DATETIME',
      'ACQ_SALE_DATETIME',
      'GROSS_AMOUNT',
      'ACQ_GROSS_AMOUNT',
      'NET_AMOUNT',
      'ACQ_NET_AMOUNT',
      'STATUS',
      'MATCH_TYPE',
      'MATCH_SCORE',
      'AMOUNT_DIFF',
      'CREATED_AT',
      'SOURCE',
      'REASON',
      'NOTES',
    ];
    const baseValues = [
      ...payload,
      meta?.source ?? null,
      meta?.reason ?? null,
      meta?.notes ?? null,
    ];
    const availableColumns = await this.getReconciliationColumns();
    const extraEntries = Object.entries(extras ?? {}).filter(([column]) =>
      availableColumns.has(column.toUpperCase()),
    );
    const columns = [...baseColumns, ...extraEntries.map(([column]) => column)];
    const values = [...baseValues, ...extraEntries.map(([, value]) => value ?? null)];
    const sql =
      `INSERT INTO T_RECONCILIATION (${columns.join(', ')}) VALUES (${columns.map(() => '?').join(', ')})`;
    return this.dbService.executeTx(tx, sql, values);
  }

  deleteInterdata(tx: any, id: number) {
    return this.dbService.executeTx(tx, 'DELETE FROM T_INTERDATA_SALES WHERE ID = ?', [id]);
  }

  deleteCielo(tx: any, id: number) {
    return this.dbService.executeTx(tx, 'DELETE FROM T_CIELO_SALES WHERE ID = ?', [id]);
  }

  deleteSipag(tx: any, id: number) {
    return this.dbService.executeTx(tx, 'DELETE FROM T_SIPAG_SALES WHERE ID = ?', [id]);
  }

  deleteSicredi(tx: any, id: number) {
    return this.dbService.executeTx(tx, 'DELETE FROM T_SICREDI_SALES WHERE ID = ?', [id]);
  }
}
