import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { promises as fs } from 'fs';
import * as path from 'path';
import { createHash } from 'crypto';
import { DbService } from '../../db/db.service';
import { parseWorkbook } from './interdata-import.parser';
import { InterdataProgressService } from './interdata-progress.service';
import { canonizeInterdata } from '../../modules/reconciliation/canon/canonize';
import { appendImportLog, ImportLogStatus } from '../../common/import-log';
import {
  InterdataFileInfo,
  InterdataFileStatus,
  NormalizedSale,
} from './interdata-import.types';
import { CanonDuplicateCleanupService } from '../../modules/reconciliation/canon-duplicate-cleanup.service';

type InterdataImportDetailMetric =
  | 'files'
  | 'inserted'
  | 'duplicates'
  | 'invalid'
  | 'review'
  | 'errors';

@Injectable()
export class InterdataImportService {
  private readonly logger = new Logger(InterdataImportService.name);

  constructor(
    private readonly configService: ConfigService,
    private readonly dbService: DbService,
    private readonly progressService: InterdataProgressService,
    private readonly canonDuplicateCleanupService: CanonDuplicateCleanupService,
  ) {}

  private buildProcessedStatus(duplicates: number): ImportLogStatus {
    return duplicates > 0 ? 'processed_with_duplicates' : 'processed';
  }

  getEnabled(): boolean {
    const enabled = this.configService.get<string>('INTERDATA_ENABLED');
    return String(enabled ?? 'true') === 'true';
  }

  getDirectories() {
    return {
      dropDir: this.configService.get<string>('INTERDATA_DROP_DIR') ?? './data/interdata/inbox',
      archiveDir: this.configService.get<string>('INTERDATA_ARCHIVE_DIR') ?? './data/interdata/archive',
      errorDir: this.configService.get<string>('INTERDATA_ERROR_DIR') ?? './data/interdata/error',
    };
  }

  getSourceName(): string {
    return this.configService.get<string>('INTERDATA_SOURCE_NAME') ?? 'INTERDATA';
  }

  getWatchEnabled(): boolean {
    const enabled = this.configService.get<string>('INTERDATA_WATCH_ENABLED');
    return String(enabled ?? 'true') === 'true';
  }

  getWatchDebounceMs(): number {
    const value = this.configService.get<string>('INTERDATA_WATCH_DEBOUNCE_MS');
    const parsed = value ? Number(value) : 1500;
    return Number.isFinite(parsed) ? parsed : 1500;
  }

  getWatchStableMs(): number {
    const value = this.configService.get<string>('INTERDATA_WATCH_STABLE_MS');
    const parsed = value ? Number(value) : 1500;
    return Number.isFinite(parsed) ? parsed : 1500;
  }

  async ensureConfiguredDirs(): Promise<void> {
    const { dropDir, archiveDir, errorDir } = this.getDirectories();
    await this.ensureDir(dropDir);
    await this.ensureDir(archiveDir);
    await this.ensureDir(errorDir);
  }

  async listDropFiles(): Promise<InterdataFileInfo[]> {
    const { dropDir } = this.getDirectories();
    const glob = this.configService.get<string>('INTERDATA_FILE_GLOB') ?? '*.xls*';
    await this.ensureDir(dropDir);

    const entries = await fs.readdir(dropDir);
    const results: InterdataFileInfo[] = [];

    for (const entry of entries) {
      if (!this.matchesGlob(entry, glob)) {
        continue;
      }
      const fullPath = path.join(dropDir, entry);
      const stat = await fs.stat(fullPath);
      if (!stat.isFile()) {
        continue;
      }
      results.push({
        filename: entry,
        size: stat.size,
        mtimeMs: stat.mtimeMs,
      });
    }

    return results;
  }

  async listRecentImportDetails(metric: InterdataImportDetailMetric, limit = 20) {
    const safeLimit = Math.min(Math.max(limit, 1), 100);
    if (metric === 'files') {
      const items = await this.readRecentImportLogEntries(safeLimit, (entry) =>
        entry.category === 'erp_import' &&
        (entry.operation === 'upload_import' || entry.operation === 'scan_drop_file') &&
        (entry.status === 'ok' || entry.status === 'skipped'),
      );
      return {
        metric,
        items: items.map((entry) => ({
          timestamp: entry.timestamp,
          fileName: entry.fileName ?? null,
          status: entry.status,
          operation: entry.operation,
          message: this.extractImportLogMessage(entry),
          details: entry.details ?? null,
        })),
      };
    }

    if (metric === 'errors') {
      const items = await this.readRecentImportLogEntries(safeLimit, (entry) =>
        entry.category === 'erp_import' && entry.status === 'error',
      );
      return {
        metric,
        items: items.map((entry) => ({
          timestamp: entry.timestamp,
          fileName: entry.fileName ?? null,
          status: entry.status,
          operation: entry.operation,
          message: this.extractImportLogMessage(entry),
          details: entry.details ?? null,
        })),
      };
    }

    const table =
      metric === 'inserted'
        ? 'T_INTERDATA_SALES'
        : metric === 'duplicates'
          ? 'T_INTERDATA_SALES_DUPLICATE'
          : 'T_INTERDATA_SALES_INVALID';
    const rows = await this.dbService.query<Record<string, unknown>>(
      `SELECT FIRST ${safeLimit} ID, SALE_NO, SOURCE, SALE_DATETIME, AUTH_NSU, CARD_BRAND_RAW, PAYMENT_TYPE, CARD_MODE, GROSS_AMOUNT, FEES_AMOUNT, NET_AMOUNT, STATUS_RAW, CREATED_AT, ROW_HASH
       FROM ${table}
       ORDER BY CREATED_AT DESC, ID DESC`,
      [],
    );
    return {
      metric,
      items: rows.map((row) => ({
        id: row.ID ?? null,
        saleNo: row.SALE_NO ?? null,
        source: row.SOURCE ?? null,
        saleDatetime: row.SALE_DATETIME ?? null,
        authNsu: row.AUTH_NSU ?? null,
        brand: row.CARD_BRAND_RAW ?? null,
        paymentType: row.PAYMENT_TYPE ?? null,
        cardMode: row.CARD_MODE ?? null,
        grossAmount: row.GROSS_AMOUNT ?? null,
        feesAmount: row.FEES_AMOUNT ?? null,
        netAmount: row.NET_AMOUNT ?? null,
        statusRaw: row.STATUS_RAW ?? null,
        createdAt: row.CREATED_AT ?? null,
        rowHash: row.ROW_HASH ?? null,
      })),
    };
  }

  async scanDropDir() {
    const { dropDir, archiveDir, errorDir } = this.getDirectories();
    await this.ensureDir(dropDir);
    await this.ensureDir(archiveDir);
    await this.ensureDir(errorDir);

    const files = await this.listDropFiles();
    if (files.length === 0) {
      return {
        processedFiles: 0,
        insertedSales: 0,
        skippedDuplicates: 0,
        invalidRows: 0,
        invalidSaved: 0,
        errors: 0,
        reconciliationDateFrom: null,
        reconciliationDateTo: null,
      };
    }

    let processedFiles = 0;
    let insertedSales = 0;
    let skippedDuplicates = 0;
    let invalidRows = 0;
    let invalidSaved = 0;
    let errors = 0;
    let reconciliationDateFrom: string | null = null;
    const reconciliationDateTo = this.toDateOnlyString(new Date());

    for (const file of files) {
      const fullPath = path.join(dropDir, file.filename);
      let sha256 = '';
      try {
        const buffer = await fs.readFile(fullPath);
        sha256 = createHash('sha256').update(buffer).digest('hex');
        const alreadyImported = await this.isImportedFileHash(sha256);
        if (alreadyImported) {
          const duplicateMessage = `Arquivo ja importado anteriormente (hash=${sha256})`;
          this.logger.warn(
            `Arquivo ignorado (hash ja importado): ${file.filename} hash=${sha256}`,
          );
          await this.recordFile(file, sha256, 'SKIPPED_DUPLICATE', duplicateMessage);
          await appendImportLog({
            category: 'erp_import',
            provider: 'INTERDATA',
            operation: 'scan_drop_file',
            status: 'skipped_duplicate_file',
            fileName: file.filename,
            details: { reason: 'hash_already_imported', hash: sha256 },
          });
          processedFiles += 1;
          await this.moveFile(fullPath, archiveDir);
          continue;
        }

        const parsed = parseWorkbook(buffer, file.filename, this.getSourceName());
        reconciliationDateFrom = this.minDateString(
          reconciliationDateFrom,
          this.extractMinSaleDate(parsed.sales),
        );
        const result = await this.saveSales(parsed.sales);
        await this.insertReviewSales(parsed.invalidSales, 'invalid');
        insertedSales += result.inserted;
        skippedDuplicates += result.duplicates;
        invalidRows += parsed.meta.invalidRows;
        invalidSaved += parsed.invalidSales.length;
        processedFiles += 1;

        await this.recordFile(file, sha256, 'PROCESSED', null);
        await this.recordImportedFile(file.filename, sha256, 'INTERDATA');
        await this.moveFile(fullPath, archiveDir);
        await appendImportLog({
          category: 'erp_import',
          provider: 'INTERDATA',
          operation: 'scan_drop_file',
          status: this.buildProcessedStatus(result.duplicates),
          fileName: file.filename,
          details: {
            inserted: result.inserted,
            duplicates: result.duplicates,
            invalidRows: parsed.meta.invalidRows,
            invalidSaved: parsed.invalidSales.length,
            hash: sha256,
          },
        });

        this.logger.log(
          `Importado ${file.filename}: vendas=${parsed.sales.length} inseridas=${result.inserted} duplicadas=${result.duplicates} invalidas=${parsed.meta.invalidRows}`,
        );
      } catch (error) {
        errors += 1;
        const message = error instanceof Error ? error.message : 'Erro ao importar';
        this.logger.error(`Falha ao importar ${file.filename}: ${message}`);
        await this.recordFile(file, sha256, 'ERROR', message);
        await this.safeMoveToError(fullPath, errorDir);
        await appendImportLog({
          category: 'erp_import',
          provider: 'INTERDATA',
          operation: 'scan_drop_file',
          status: 'error',
          fileName: file.filename,
          details: { error: message },
        });
      }
    }

    if (reconciliationDateFrom && reconciliationDateTo) {
      await this.canonDuplicateCleanupService.cleanupAfterImport(
        'INTERDATA',
        reconciliationDateFrom,
        reconciliationDateTo,
      );
    }

    return {
      processedFiles,
      insertedSales,
      skippedDuplicates,
      invalidRows,
      invalidSaved,
      errors,
      reconciliationDateFrom,
      reconciliationDateTo: reconciliationDateFrom ? reconciliationDateTo : null,
    };
  }

  async uploadAndImport(buffer: Buffer, originalName: string, uploadId?: string) {
    const { dropDir, archiveDir, errorDir } = this.getDirectories();
    await this.ensureDir(dropDir);
    await this.ensureDir(archiveDir);
    await this.ensureDir(errorDir);

    const filename = `${Date.now()}-${originalName}`;
    const fullPath = path.join(dropDir, filename);
    this.logger.log(
      `Upload recebido: ${originalName} tamanho=${buffer.length} dropDir=${dropDir}`,
    );
    await fs.writeFile(fullPath, buffer);

    try {
      this.logger.log(`Lendo planilha: ${originalName}`);
      const fileInfo: InterdataFileInfo = {
        filename,
        size: buffer.length,
        mtimeMs: Date.now(),
      };
      const sha256 = createHash('sha256').update(buffer).digest('hex');
      const alreadyImported = await this.isImportedFileHash(sha256);
      if (alreadyImported) {
        const duplicateMessage = `Arquivo ja importado anteriormente (hash=${sha256})`;
        await this.recordFile(fileInfo, sha256, 'SKIPPED_DUPLICATE', duplicateMessage);
        await this.moveFile(fullPath, archiveDir);
        await appendImportLog({
          category: 'erp_import',
          provider: 'INTERDATA',
          operation: 'upload_import',
            status: 'skipped_duplicate_file',
          fileName: originalName,
          details: { reason: 'hash_already_imported', hash: sha256 },
        });
        return {
          processedFiles: 1,
          insertedSales: 0,
          skippedDuplicates: 0,
          invalidRows: 0,
          invalidSaved: 0,
          errors: 0,
          alreadyImported: true,
          fileHash: sha256,
          message: duplicateMessage,
          reconciliationDateFrom: null,
          reconciliationDateTo: null,
        };
      }

      const parsed = parseWorkbook(buffer, originalName, this.getSourceName());
      const reconciliationDateFrom = this.extractMinSaleDate(parsed.sales);
      const reconciliationDateTo = reconciliationDateFrom
        ? this.toDateOnlyString(new Date())
        : null;
      this.logger.log(
        `Parse concluido: linhas=${parsed.meta.parsedRows} invalidas=${parsed.meta.invalidRows}`,
      );
      if (uploadId) {
        this.progressService.update({
          uploadId,
          percent: 0,
          stage: 'insert',
          message: `Processando 0/${parsed.sales.length}`,
        });
      }
      const result = await this.saveSales(parsed.sales, uploadId);
      await this.insertReviewSales(parsed.invalidSales, 'invalid');
      this.logger.log(`Insert concluido: inseridas=${result.inserted} duplicadas=${result.duplicates}`);
      await this.recordFile(fileInfo, sha256, 'PROCESSED', null);
      await this.recordImportedFile(originalName, sha256, 'INTERDATA');
      await this.moveFile(fullPath, archiveDir);
      await appendImportLog({
        category: 'erp_import',
        provider: 'INTERDATA',
        operation: 'upload_import',
        status: this.buildProcessedStatus(result.duplicates),
        fileName: originalName,
        details: {
          inserted: result.inserted,
          duplicates: result.duplicates,
          invalidRows: parsed.meta.invalidRows,
          invalidSaved: parsed.invalidSales.length,
          hash: sha256,
        },
      });
      if (reconciliationDateFrom && reconciliationDateTo) {
        await this.canonDuplicateCleanupService.cleanupAfterImport(
          'INTERDATA',
          reconciliationDateFrom,
          reconciliationDateTo,
        );
      }

      return {
        processedFiles: 1,
        insertedSales: result.inserted,
        skippedDuplicates: result.duplicates,
        invalidRows: parsed.meta.invalidRows,
        invalidSaved: parsed.invalidSales.length,
        errors: 0,
        alreadyImported: false,
        fileHash: sha256,
        reconciliationDateFrom,
        reconciliationDateTo,
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao importar';
      this.logger.error(`Falha ao importar upload ${originalName}: ${message}`);
      await this.safeMoveToError(fullPath, errorDir);
      await appendImportLog({
        category: 'erp_import',
        provider: 'INTERDATA',
        operation: 'upload_import',
        status: 'error',
        fileName: originalName,
        details: { error: message },
      });
      throw error;
    }
  }

  async listSales(options: {
    dateFrom?: string;
    dateTo?: string;
    page?: number;
    limit?: number;
    status?: string;
    acquirer?: string;
    search?: string;
    paymentType?: string;
    brand?: string;
    bucket?: 'valid' | 'invalid' | 'duplicate';
    sortBy?: 'datetime' | 'amount';
    sortDir?: 'asc' | 'desc';
    verboseEnabled?: boolean;
  }) {
    const page = options.page ?? 1;
    const limit = options.limit ?? 50;
    const offset = (page - 1) * limit;
    const conditions: string[] = [];
    const params: unknown[] = [];
    const verboseEnabled = Boolean(options.verboseEnabled);

    if (options.dateFrom) {
      conditions.push('SALE_DATETIME >= ?');
      params.push(this.toStartOfDay(options.dateFrom));
    }
    if (options.dateTo) {
      conditions.push('SALE_DATETIME <= ?');
      params.push(this.toEndOfDay(options.dateTo));
    }

    const statusCondition = this.buildStatusCondition(options.status);
    if (statusCondition) {
      conditions.push(statusCondition.sql);
      params.push(...statusCondition.params);
    }

    if (options.paymentType) {
      const payment = options.paymentType.toUpperCase();
      const needle =
        payment.includes('CARTAO') || payment === 'CARD'
          ? '%CARD%'
          : payment.includes('PIX')
            ? '%PIX%'
            : `%${payment}%`;
      conditions.push(
        '((UPPER(COALESCE(CANON_METHOD_GROUP, \'\')) LIKE ? OR UPPER(COALESCE(CANON_METHOD, \'\')) LIKE ?) OR ((CANON_METHOD_GROUP IS NULL OR TRIM(CANON_METHOD_GROUP) = \'\') AND (CANON_METHOD IS NULL OR TRIM(CANON_METHOD) = \'\') AND (UPPER(COALESCE(PAYMENT_TYPE, \'\')) LIKE ? OR UPPER(COALESCE(CARD_MODE, \'\')) LIKE ?)))',
      );
      params.push(needle, needle, needle, needle);
    }

    if (options.brand) {
      const needle = `%${options.brand.toUpperCase()}%`;
      conditions.push(
        '(UPPER(COALESCE(CANON_METHOD, \'\')) LIKE ? OR UPPER(COALESCE(CANON_BRAND, \'\')) LIKE ? OR UPPER(COALESCE(CARD_BRAND_RAW, \'\')) LIKE ?)',
      );
      params.push(needle, needle, needle);
    }

    if (options.search) {
      const needle = `%${options.search.toUpperCase()}%`;
      conditions.push(
        '(' +
          [
            'UPPER(COALESCE(SALE_NO, \'\')) LIKE ?',
            'UPPER(COALESCE(AUTH_NSU, \'\')) LIKE ?',
            'UPPER(COALESCE(CARD_BRAND_RAW, \'\')) LIKE ?',
            'UPPER(COALESCE(PAYMENT_TYPE, \'\')) LIKE ?',
            'UPPER(COALESCE(CARD_MODE, \'\')) LIKE ?',
            'UPPER(COALESCE(CANON_METHOD_GROUP, \'\')) LIKE ?',
            'UPPER(COALESCE(CANON_METHOD, \'\')) LIKE ?',
            'UPPER(COALESCE(CANON_BRAND, \'\')) LIKE ?',
            'UPPER(COALESCE(SOURCE, \'\')) LIKE ?',
            'UPPER(COALESCE(STATUS_RAW, \'\')) LIKE ?',
          ].join(' OR ') +
          ')',
      );
      params.push(
        needle,
        needle,
        needle,
        needle,
        needle,
        needle,
        needle,
        needle,
        needle,
        needle,
      );
    }

    const where = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';
    const table = this.getSalesTable(options.bucket);
    const isValidBucket = table === 'T_INTERDATA_SALES';
    const pendingOnlyClause = isValidBucket
      ? `NOT EXISTS (
          SELECT 1
          FROM T_RECONCILIATION r
          WHERE r.INTERDATA_ID = T_INTERDATA_SALES.ID
            AND COALESCE(r.IS_ACTIVE, 1) = 1
        )`
      : '';
    const effectiveWhere = pendingOnlyClause
      ? where
        ? `${where} AND ${pendingOnlyClause}`
        : `WHERE ${pendingOnlyClause}`
      : where;
    const orderBy = this.buildOrderBy(options.sortBy, options.sortDir, {
      datetime: 'SALE_DATETIME',
      amount: 'GROSS_AMOUNT',
    });
    const sql =
      `SELECT FIRST ${limit} SKIP ${offset} * FROM ${table} ${effectiveWhere} ORDER BY ${orderBy}`;
    if (process.env.DEBUG === 'true' || verboseEnabled) {
      this.logger.log('[Interdata] listSales options', {
        dateFrom: options.dateFrom,
        dateTo: options.dateTo,
        status: options.status,
        search: options.search,
        sortBy: options.sortBy,
        sortDir: options.sortDir,
      });
      this.logger.log('[Interdata] listSales SQL', { sql, params });
    }
    return this.dbService.query(sql, params);
  }

  private buildOrderBy(
    sortBy: 'datetime' | 'amount' | undefined,
    sortDir: 'asc' | 'desc' | undefined,
    columns: { datetime: string; amount: string },
  ) {
    if (!sortBy) {
      return `${columns.datetime} DESC, ID DESC`;
    }
    if (sortBy === 'amount') {
      return `${columns.amount} ${sortDir === 'desc' ? 'DESC' : 'ASC'}, ID DESC`;
    }
    return `${columns.datetime} ${sortDir === 'asc' ? 'ASC' : 'DESC'}, ID DESC`;
  }

  private buildStoredRowHash(baseHash: string, duplicateOrdinal: number): string {
    if (!duplicateOrdinal) {
      return baseHash;
    }
    return createHash('sha256').update(`${baseHash}:dup:${duplicateOrdinal}`).digest('hex');
  }

  private async resolveAvailableRowHash(baseHash: string, startOrdinal = 0): Promise<string> {
    let ordinal = startOrdinal;
    for (;;) {
      const candidate = this.buildStoredRowHash(baseHash, ordinal);
      const rows = await this.dbService.query<{ ID: number }>(
        'SELECT FIRST 1 ID FROM T_INTERDATA_SALES WHERE ROW_HASH = ?',
        [candidate],
      );
      if (!rows.length) {
        return candidate;
      }
      ordinal += 1;
    }
  }

  private extractMinSaleDate(sales: NormalizedSale[]): string | null {
    let minDate: Date | null = null;
    for (const sale of sales) {
      if (!(sale.saleDatetime instanceof Date) || Number.isNaN(sale.saleDatetime.getTime())) {
        continue;
      }
      if (!minDate || sale.saleDatetime.getTime() < minDate.getTime()) {
        minDate = sale.saleDatetime;
      }
    }
    return minDate ? this.toDateOnlyString(minDate) : null;
  }

  private minDateString(left: string | null, right: string | null): string | null {
    if (!left) {
      return right;
    }
    if (!right) {
      return left;
    }
    return left <= right ? left : right;
  }

  private toDateOnlyString(date: Date): string {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
  }

  async clearSales() {
    await this.dbService.execute('DELETE FROM T_INTERDATA_SALES');
    await this.dbService.execute('DELETE FROM T_INTERDATA_FILES');
    await this.dbService.execute('DELETE FROM T_INTERDATA_SALES_INVALID');
    await this.dbService.execute('DELETE FROM T_INTERDATA_SALES_DUPLICATE');
  }

  private async saveSales(
    sales: NormalizedSale[],
    uploadId?: string,
  ): Promise<{ inserted: number; duplicates: number }> {
    let inserted = 0;
    let duplicates = 0;
    let loggedSamples = 0;
    const total = sales.length;
    const reportEvery = total > 0 ? Math.max(1, Math.floor(total / 20)) : 1;
    for (const sale of sales) {
      const normalizedSaleNo = this.normalizeSaleNo(sale.saleNo);
      let duplicateOrdinal = 0;

      const sql =
        'INSERT INTO T_INTERDATA_SALES (SALE_NO, SOURCE, SALE_DATETIME, AUTH_NSU, CARD_BRAND_RAW, PAYMENT_TYPE, CARD_MODE, INSTALLMENTS, GROSS_AMOUNT, FEES_AMOUNT, NET_AMOUNT, STATUS_RAW, IS_CANCELLED, ROW_HASH, CREATED_AT, CANON_SALE_DATE, CANON_METHOD, CANON_METHOD_GROUP, CANON_BRAND, CANON_TERMINAL_NO, CANON_GROSS_AMOUNT, CANON_INSTALLMENT_NO, CANON_INSTALLMENT_TOTAL) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)';

      try {
        const exists = normalizedSaleNo
          ? await this.dbService.query<{ ID: number }>(
              'SELECT FIRST 1 ID FROM T_INTERDATA_SALES WHERE (SOURCE = ? AND SALE_NO = ?) OR ROW_HASH = ?',
              [sale.source, normalizedSaleNo, sale.rowHash],
            )
          : await this.dbService.query<{ ID: number }>(
              'SELECT FIRST 1 ID FROM T_INTERDATA_SALES WHERE SOURCE = ? AND SALE_DATETIME = ? AND GROSS_AMOUNT = ? AND UPPER(COALESCE(CARD_BRAND_RAW, \'\')) = ?',
              [
                sale.source,
                sale.saleDatetime,
                sale.grossAmount,
                (sale.cardBrandRaw ?? '').toUpperCase(),
              ],
            );
        if (exists.length) {
          duplicates += 1;
          duplicateOrdinal = duplicates;
          const reason = normalizedSaleNo ? 'sale_no' : 'datahora_valor_bandeira';
          this.logger.warn(
            `Duplicado detectado (${reason}): saleNo=${normalizedSaleNo ?? 'NULL'} data=${sale.saleDatetime?.toISOString() ?? 'NULL'} valor=${sale.grossAmount ?? 'NULL'} bandeira=${sale.cardBrandRaw ?? 'NULL'} hash=${sale.rowHash}`,
          );
          await this.insertReviewSale(sale, 'duplicate');
        }

        const canon = canonizeInterdata({
          SALE_DATETIME: sale.saleDatetime ?? null,
          GROSS_AMOUNT: sale.grossAmount ?? null,
          AUTH_NSU: sale.authNsu ?? null,
          INSTALLMENTS: sale.installments ?? null,
          CARD_BRAND_RAW: sale.cardBrandRaw ?? null,
        });
        if (process.env.DEBUG === 'true' && loggedSamples < 3) {
          this.logger.debug(
            {
              rawCardBrand: sale.cardBrandRaw,
              rawPaymentType: sale.paymentType,
              canon,
            },
            'Interdata canon sample',
          );
          loggedSamples += 1;
        }

        const storedRowHash = await this.resolveAvailableRowHash(sale.rowHash, duplicateOrdinal);
        const storedSaleNo = duplicateOrdinal > 0 ? null : normalizedSaleNo;
        await this.dbService.execute(sql, [
          storedSaleNo,
          sale.source,
          sale.saleDatetime,
          sale.authNsu ?? null,
          sale.cardBrandRaw ?? null,
          sale.paymentType,
          sale.cardMode,
          sale.installments ?? null,
          sale.grossAmount,
          sale.feesAmount ?? null,
          sale.netAmount ?? null,
          sale.statusRaw ?? null,
          sale.isCancelled,
          storedRowHash,
          new Date(),
          canon.CANON_SALE_DATE,
          canon.CANON_METHOD,
          canon.CANON_METHOD_GROUP,
          canon.CANON_BRAND,
          canon.CANON_TERMINAL_NO,
          canon.CANON_GROSS_AMOUNT,
          canon.CANON_INSTALLMENT_NO,
          canon.CANON_INSTALLMENT_TOTAL,
        ]);
        inserted += 1;
      } catch (error) {
        const message = error instanceof Error ? error.message : '';
        if (message.includes('UNIQUE') || message.includes('unique')) {
          duplicates += 1;
          this.logger.warn(
            `Duplicado detectado (unique): saleNo=${normalizedSaleNo ?? 'NULL'} data=${sale.saleDatetime?.toISOString() ?? 'NULL'} valor=${sale.grossAmount ?? 'NULL'} bandeira=${sale.cardBrandRaw ?? 'NULL'} hash=${sale.rowHash}`,
          );
          await this.insertReviewSale(sale, 'duplicate');
          const canon = canonizeInterdata({
            SALE_DATETIME: sale.saleDatetime ?? null,
            GROSS_AMOUNT: sale.grossAmount ?? null,
            AUTH_NSU: sale.authNsu ?? null,
            INSTALLMENTS: sale.installments ?? null,
            CARD_BRAND_RAW: sale.cardBrandRaw ?? null,
          });
          await this.dbService.execute(sql, [
            null,
            sale.source,
            sale.saleDatetime,
            sale.authNsu ?? null,
            sale.cardBrandRaw ?? null,
            sale.paymentType,
            sale.cardMode,
            sale.installments ?? null,
            sale.grossAmount,
            sale.feesAmount ?? null,
            sale.netAmount ?? null,
            sale.statusRaw ?? null,
            sale.isCancelled,
            await this.resolveAvailableRowHash(sale.rowHash, duplicates),
            new Date(),
            canon.CANON_SALE_DATE,
            canon.CANON_METHOD,
            canon.CANON_METHOD_GROUP,
            canon.CANON_BRAND,
            canon.CANON_TERMINAL_NO,
            canon.CANON_GROSS_AMOUNT,
            canon.CANON_INSTALLMENT_NO,
            canon.CANON_INSTALLMENT_TOTAL,
          ]);
          inserted += 1;
          continue;
        }
        throw error;
      }

      if (uploadId) {
        const processed = inserted + duplicates;
        if (processed % reportEvery === 0 || processed === total) {
          const percent = total ? Math.round((processed / total) * 100) : 100;
          this.progressService.update({
            uploadId,
            percent,
            stage: 'insert',
            message: `Processando ${processed}/${total}`,
          });
        }
      }
    }
    return { inserted, duplicates };
  }

  async approveReviewSale(bucket: 'invalid' | 'duplicate', id: number) {
    const table = this.getReviewTable(bucket);
    const rows = await this.dbService.query<Record<string, any>>(
      `SELECT FIRST 1 * FROM ${table} WHERE ID = ?`,
      [id],
    );
    const row = rows[0];
    if (!row) {
      throw new Error('Registro nao encontrado');
    }
    const canon = canonizeInterdata({
      SALE_DATETIME: row.SALE_DATETIME ?? null,
      GROSS_AMOUNT: row.GROSS_AMOUNT ?? null,
      AUTH_NSU: row.AUTH_NSU ?? null,
      INSTALLMENTS: row.INSTALLMENTS ?? null,
      CARD_BRAND_RAW: row.CARD_BRAND_RAW ?? null,
    });
    const normalizedSaleNo =
      row.SALE_NO && String(row.SALE_NO).trim() !== '' && String(row.SALE_NO).trim() !== '0'
        ? row.SALE_NO
        : null;
    const insertSql =
      'INSERT INTO T_INTERDATA_SALES (SALE_NO, SOURCE, SALE_DATETIME, AUTH_NSU, CARD_BRAND_RAW, PAYMENT_TYPE, CARD_MODE, INSTALLMENTS, GROSS_AMOUNT, FEES_AMOUNT, NET_AMOUNT, STATUS_RAW, IS_CANCELLED, ROW_HASH, CREATED_AT, CANON_SALE_DATE, CANON_METHOD, CANON_METHOD_GROUP, CANON_BRAND, CANON_TERMINAL_NO, CANON_GROSS_AMOUNT, CANON_INSTALLMENT_NO, CANON_INSTALLMENT_TOTAL) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)';

    const affected = await this.dbService.execute(insertSql, [
      normalizedSaleNo,
      row.SOURCE ?? null,
      row.SALE_DATETIME ?? null,
      row.AUTH_NSU ?? null,
      row.CARD_BRAND_RAW ?? null,
      row.PAYMENT_TYPE ?? null,
      row.CARD_MODE ?? null,
      row.INSTALLMENTS ?? null,
      row.GROSS_AMOUNT ?? null,
      row.FEES_AMOUNT ?? null,
      row.NET_AMOUNT ?? null,
      row.STATUS_RAW ?? null,
      row.IS_CANCELLED ?? null,
      row.ROW_HASH ?? null,
      row.CREATED_AT ?? new Date(),
      canon.CANON_SALE_DATE,
      canon.CANON_METHOD,
      canon.CANON_METHOD_GROUP,
      canon.CANON_BRAND,
      canon.CANON_TERMINAL_NO,
      canon.CANON_GROSS_AMOUNT,
      canon.CANON_INSTALLMENT_NO,
      canon.CANON_INSTALLMENT_TOTAL,
    ]);
    await this.dbService.execute(`DELETE FROM ${table} WHERE ID = ?`, [id]);
    return affected;
  }

  private async insertReviewSales(sales: NormalizedSale[], bucket: 'invalid' | 'duplicate') {
    if (!sales.length) {
      return;
    }
    for (const sale of sales) {
      await this.insertReviewSale(sale, bucket);
    }
  }

  private async insertReviewSale(sale: NormalizedSale, bucket: 'invalid' | 'duplicate') {
    const table = this.getReviewTable(bucket);
    if (bucket === 'invalid') {
      this.logger.warn(
        `Invalido: motivo=${sale.invalidReason ?? 'DESCONHECIDO'} saleNo=${sale.saleNo || 'NULL'} data=${sale.saleDatetime?.toISOString() ?? 'NULL'} valor=${sale.grossAmount ?? 'NULL'} bandeira=${sale.cardBrandRaw ?? 'NULL'} hash=${sale.rowHash}`,
      );
    }
    const sql =
      `INSERT INTO ${table} (SALE_NO, SOURCE, SALE_DATETIME, AUTH_NSU, CARD_BRAND_RAW, PAYMENT_TYPE, CARD_MODE, INSTALLMENTS, GROSS_AMOUNT, FEES_AMOUNT, NET_AMOUNT, STATUS_RAW, IS_CANCELLED, ROW_HASH, CREATED_AT)` +
      ' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)';

    await this.dbService.execute(sql, [
      sale.saleNo || null,
      sale.source,
      sale.saleDatetime,
      sale.authNsu ?? null,
      sale.cardBrandRaw ?? null,
      sale.paymentType,
      sale.cardMode,
      sale.installments ?? null,
      sale.grossAmount ?? null,
      sale.feesAmount ?? null,
      sale.netAmount ?? null,
      sale.statusRaw ?? null,
      sale.isCancelled,
      sale.rowHash,
      new Date(),
    ]);
  }

  private getSalesTable(bucket?: 'valid' | 'invalid' | 'duplicate'): string {
    if (bucket === 'invalid') {
      return this.getReviewTable('invalid');
    }
    if (bucket === 'duplicate') {
      return this.getReviewTable('duplicate');
    }
    return 'T_INTERDATA_SALES';
  }

  private getReviewTable(bucket: 'invalid' | 'duplicate'): string {
    return bucket === 'invalid' ? 'T_INTERDATA_SALES_INVALID' : 'T_INTERDATA_SALES_DUPLICATE';
  }

  private normalizeSaleNo(value: string) {
    const normalized = value?.trim();
    if (!normalized || normalized === '0') {
      return null;
    }
    return normalized;
  }

  private buildStatusCondition(
    status?: string,
  ): { sql: string; params: string[] } | null {
    const raw = Array.isArray(status) ? status[0] : status;
    const normalized = String(raw ?? '').trim().toUpperCase();
    if (!normalized) {
      return null;
    }

    const make = (patterns: string[]) => ({
      sql: `(${patterns.map(() => 'UPPER(STATUS_RAW) LIKE ?').join(' OR ')})`,
      params: patterns.map((pattern) => `%${pattern}%`),
    });

    switch (normalized) {
      case 'PENDENTE':
        return make(['PEND', 'PENDEN', 'AGUARD', 'EM_ABERTO', 'ABERTO']);
      case 'DIVERGENTE':
        return make(['DIVERG', 'DIFER', 'INCONSIST', 'ERRO']);
      case 'NAO_LOCALIZADO':
        return make(['NAO_LOC', 'N LOC', 'CANCEL', 'ESTORN', 'NEGAD', 'RECUS']);
      case 'AUTORIZADA':
        return make(['APROV', 'APROVAD', 'AUTORIZ', 'OK', 'SUCESSO', 'CONFIRM', 'CAPTUR']);
      default:
        return null;
    }
  }

  private toStartOfDay(dateText: string): Date {
    const [year, month, day] = dateText.split('-').map(Number);
    return new Date(year, month - 1, day, 0, 0, 0, 0);
  }

  private toEndOfDay(dateText: string): Date {
    const [year, month, day] = dateText.split('-').map(Number);
    return new Date(year, month - 1, day, 23, 59, 59, 999);
  }

  private async isFileDuplicate(file: InterdataFileInfo, sha256: string): Promise<boolean> {
    const rows = await this.dbService.query<{ ID: number }>(
      'SELECT FIRST 1 ID FROM T_INTERDATA_FILES WHERE SHA256 = ? OR (FILENAME = ? AND SIZE = ? AND MTIME = ?)',
      [sha256, file.filename, file.size, new Date(file.mtimeMs)],
    );
    return rows.length > 0;
  }

  private async isImportedFileHash(hash: string): Promise<boolean> {
    try {
      const rows = await this.dbService.query<{ ID: number }>(
        'SELECT FIRST 1 ID FROM T_IMPORTED_FILES WHERE HASH = ?',
        [hash],
      );
      return rows.length > 0;
    } catch (error) {
      this.logger.warn(
        `T_IMPORTED_FILES indisponivel para checagem de hash: ${
          error instanceof Error ? error.message : 'erro desconhecido'
        }`,
      );
      return false;
    }
  }

  private async recordImportedFile(
    filename: string,
    hash: string,
    acquirer: string,
  ): Promise<void> {
    try {
      await this.dbService.execute(
        'INSERT INTO T_IMPORTED_FILES (FILENAME, HASH, ACQUIRER, IMPORTED_AT) VALUES (?, ?, ?, ?)',
        [filename, hash, acquirer, new Date()],
      );
    } catch (error) {
      this.logger.warn(
        `Falha ao registrar T_IMPORTED_FILES filename=${filename}: ${
          error instanceof Error ? error.message : 'erro desconhecido'
        }`,
      );
    }
  }

  private async recordFile(
    file: InterdataFileInfo,
    sha256: string,
    status: InterdataFileStatus,
    errorMessage: string | null,
  ) {
    const processedAt = new Date();
    const mtime = new Date(file.mtimeMs);
    if (sha256) {
      const sql =
        'UPDATE OR INSERT INTO T_INTERDATA_FILES (FILENAME, SHA256, SIZE, MTIME, STATUS, PROCESSED_AT, ERROR_MESSAGE) VALUES (?, ?, ?, ?, ?, ?, ?) MATCHING (SHA256)';
      await this.dbService.execute(sql, [
        file.filename,
        sha256,
        file.size,
        mtime,
        status,
        processedAt,
        errorMessage,
      ]);
      return;
    }

    const sql =
      'UPDATE OR INSERT INTO T_INTERDATA_FILES (FILENAME, SIZE, MTIME, STATUS, PROCESSED_AT, ERROR_MESSAGE) VALUES (?, ?, ?, ?, ?, ?) MATCHING (FILENAME, MTIME)';
    await this.dbService.execute(sql, [
      file.filename,
      file.size,
      mtime,
      status,
      processedAt,
      errorMessage,
    ]);
  }

  private async moveFile(fromPath: string, targetDir: string) {
    const baseName = path.basename(fromPath);
    let targetPath = path.join(targetDir, baseName);

    try {
      await fs.rename(fromPath, targetPath);
    } catch (error) {
      const timestamp = Date.now();
      targetPath = path.join(targetDir, `${timestamp}-${baseName}`);
      await fs.rename(fromPath, targetPath);
    }
  }

  private async safeMoveToError(fromPath: string, errorDir: string) {
    try {
      await this.moveFile(fromPath, errorDir);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao mover arquivo';
      this.logger.error(`Falha ao mover para errorDir: ${message}`);
    }
  }

  private async ensureDir(dirPath: string) {
    await fs.mkdir(dirPath, { recursive: true });
  }

  private matchesGlob(filename: string, glob: string): boolean {
    if (!glob || glob === '*' || glob === '*.*') {
      return true;
    }
    const escaped = glob.replace(/[-/\\^$+?.()|[\]{}]/g, '\\$&');
    const regex = new RegExp(`^${escaped.replace(/\*/g, '.*').replace(/\?/g, '.')}$`, 'i');
    return regex.test(filename);
  }

  private async readRecentImportLogEntries(
    limit: number,
    predicate: (entry: Record<string, any>) => boolean,
  ): Promise<Array<Record<string, any>>> {
    const baseDir = this.configService.get<string>('IMPORT_LOG_DIR')?.trim() || './data/logs/imports';
    const resolvedBaseDir = path.resolve(process.cwd(), baseDir);
    try {
      const files = (await fs.readdir(resolvedBaseDir))
        .filter((file) => file.endsWith('.jsonl'))
        .sort()
        .reverse();
      const collected: Array<Record<string, any>> = [];
      for (const file of files) {
        if (collected.length >= limit) {
          break;
        }
        const fullPath = path.join(resolvedBaseDir, file);
        const content = await fs.readFile(fullPath, 'utf8');
        const lines = content.split(/\r?\n/).filter((line) => line.trim()).reverse();
        for (const line of lines) {
          if (collected.length >= limit) {
            break;
          }
          try {
            const parsed = JSON.parse(line) as Record<string, any>;
            if (predicate(parsed)) {
              collected.push(parsed);
            }
          } catch {
            continue;
          }
        }
      }
      return collected;
    } catch {
      return [];
    }
  }

  private extractImportLogMessage(entry: Record<string, any>): string | null {
    const details = entry.details && typeof entry.details === 'object' ? entry.details : null;
    if (details && typeof details.error === 'string') {
      return details.error;
    }
    if (details && typeof details.reason === 'string' && details.reason === 'hash_already_imported') {
      return 'Arquivo ja importado anteriormente';
    }
    if (entry.status === 'ok') {
      return 'Importacao concluida';
    }
    if (entry.status === 'skipped') {
      return 'Importacao ignorada';
    }
    return null;
  }
}
