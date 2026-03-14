import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { promises as fs } from 'fs';
import { createReadStream } from 'fs';
import * as path from 'path';
import { createHash } from 'crypto';
import { DbService } from '../../db/db.service';
import {
  Cielo04AdjustmentBlock,
  EdiAdjustment,
  EdiAdjustmentItem,
  EdiFileInfo,
  EdiFileStatus,
  EdiFileType,
  EdiTransaction,
} from './cielo-sftp-edi.types';
import { parseCielo04File, parseCielo16File, parseEdiFile } from './cielo-sftp-edi.parser';
import { CieloSftpClient } from './cielo-sftp.client';
import { canonizeCielo } from '../../modules/reconciliation/canon/canonize';
import { CanonDuplicateCleanupService } from '../../modules/reconciliation/canon-duplicate-cleanup.service';

@Injectable()
export class CieloSftpEdiService {
  private readonly logger = new Logger(CieloSftpEdiService.name);
  private dbControlEnabled: boolean | null = null;
  private dbControlWarned = false;
  private insertColumnsLogged = false;
  private cieloSalesColumnsCache: Set<string> | null = null;
  private tableColumnsCache = new Map<string, Set<string>>();

  constructor(
    private readonly configService: ConfigService,
    private readonly dbService: DbService,
    private readonly sftpClient: CieloSftpClient,
    private readonly canonDuplicateCleanupService: CanonDuplicateCleanupService,
  ) {}

  private buildStoredRowHash(baseHash: string, duplicateOrdinal: number): string {
    if (!duplicateOrdinal) {
      return baseHash;
    }
    return createHash('sha256').update(`${baseHash}:dup:${duplicateOrdinal}`).digest('hex');
  }

  getMode(): string {
    return this.configService.get<string>('CIELO_EDI_MODE') ?? 'local';
  }

  getDirectories() {
    return {
      localDir: this.configService.get<string>('CIELO_EDI_LOCAL_DIR') ?? './data/cielo/edi',
      archiveDir: this.configService.get<string>('CIELO_EDI_ARCHIVE_DIR') ?? './data/cielo/archive',
      errorDir: this.configService.get<string>('CIELO_EDI_ERROR_DIR') ?? './data/cielo/error',
    };
  }

  async listLocalFiles(): Promise<EdiFileInfo[]> {
    const { localDir } = this.getDirectories();
    const glob = this.configService.get<string>('CIELO_EDI_FILE_GLOB') ?? '*.*';
    await this.ensureDir(localDir);
    const entries = await fs.readdir(localDir);
    const results: EdiFileInfo[] = [];

    for (const entry of entries) {
      if (!this.matchesGlob(entry, glob)) {
        continue;
      }
      const fullPath = path.join(localDir, entry);
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

  async syncFromSftp(): Promise<{ downloaded: number; files: string[] }> {
    const mode = this.getMode();
    if (mode !== 'sftp') {
      throw new Error('CIELO_EDI_MODE diferente de sftp');
    }

    const { localDir } = this.getDirectories();
    const glob = this.configService.get<string>('CIELO_EDI_FILE_GLOB') ?? '*.*';
    await this.ensureDir(localDir);
    const existingFiles = await this.listLocalFiles();
    const existingNames = new Set(existingFiles.map((file) => file.filename));
    const downloaded = await this.sftpClient.downloadNewFiles(localDir, existingNames, glob);
    return { downloaded: downloaded.length, files: downloaded };
  }

  async scanLocal(): Promise<{
    processed: number;
    errors: number;
    skipped: number;
    unknown: number;
  }> {
    const { localDir, archiveDir, errorDir } = this.getDirectories();
    const duplicateDir = path.join(archiveDir, 'duplicate');
    await this.ensureDir(localDir);
    await this.ensureDir(archiveDir);
    await this.ensureDir(errorDir);
    await this.ensureDir(duplicateDir);

    const files = await this.listLocalFiles();
    if (files.length === 0) {
      this.logger.log('Nenhum arquivo EDI encontrado para processar');
      return { processed: 0, errors: 0, skipped: 0, unknown: 0 };
    }
    let processed = 0;
    let errors = 0;
    let skipped = 0;
    let unknown = 0;
    const verboseEnabled = this.isVerboseEnabled();

    for (const file of files) {
      const fullPath = path.join(localDir, file.filename);
      try {
        if (this.isTempFile(file.filename)) {
          continue;
        }
        if (!this.isValidFilename(file.filename)) {
          unknown += 1;
          await this.recordFile(file, '', 'UNKNOWN', 'ERROR', 'Nome de arquivo invalido', null);
          await this.moveFile(fullPath, errorDir);
          continue;
        }

        const sha256 = await this.hashFileSha256(fullPath);

        const content = await fs.readFile(fullPath, 'utf8');
        const type = this.detectType(file.filename, content);
        const lineCount = content.split(/\r?\n/).filter((line) => line.trim() !== '').length;

        this.logger.log(
          `Processando EDI: ${file.filename} type=${type} lines=${lineCount} sha=${sha256}`,
        );

        if (type === 'UNKNOWN') {
          unknown += 1;
          await this.recordFile(file, sha256, type, 'ERROR', 'Tipo nao identificado', lineCount);
          await this.moveFile(fullPath, errorDir);
          continue;
        }

        let transactions: EdiTransaction[] = [];
        let adjustments: EdiAdjustment[] = [];
        let adjustmentBlocks: Cielo04AdjustmentBlock[] = [];
        let headerDate: string | null = null;
        let trailerLine: string | null = null;
        if (type === 'CIELO16') {
          const parsed16 = parseCielo16File(content, file.filename, verboseEnabled);
          transactions = parsed16.transactions;
          headerDate = parsed16.headerDate;
          trailerLine = parsed16.trailerLine ?? null;
          if (verboseEnabled && parsed16.headerLine) {
            this.logger.debug(`CIELO16 header: ${parsed16.headerLine.slice(0, 120)}`);
          }
        } else if (type === 'CIELO04') {
          const parsed04 = parseCielo04File(content, file.filename, verboseEnabled);
          transactions = parsed04.transactions;
          adjustmentBlocks = parsed04.adjustmentBlocks;
          headerDate = parsed04.headerDate;
        } else {
          const parsed = parseEdiFile(type, content, file.filename, verboseEnabled);
          transactions = parsed.transactions ?? [];
          adjustments = parsed.adjustments ?? [];
          headerDate = parsed.headerDate ?? null;
        }
        if (headerDate) {
          const firstSaleDatetime =
            transactions.find((entry) => entry.saleDatetime)?.saleDatetime ?? null;
          this.logger.log(
            `EDI headerDate=${headerDate} firstSaleDatetime=${firstSaleDatetime ?? 'null'} file=${file.filename}`,
          );
        }
        const dateRange = this.getSaleDateRange(transactions);
        const fileBaseDate = this.extractFileBaseDate(file.filename);
        const warningMessage = this.buildFutureSaleDateWarning(
          fileBaseDate,
          dateRange.maxSaleDate,
          type,
        );
        const fileStatus = warningMessage ? 'WARNING' : 'PROCESSED';

        if (verboseEnabled) {
          this.logger.log(
            `EDI verbose: file=${file.filename} fileDate=${this.formatLogValue(fileBaseDate)} minSaleDate=${this.formatLogValue(dateRange.minSaleDate)} maxSaleDate=${this.formatLogValue(dateRange.maxSaleDate)} qtdE=${transactions.length} qtdD=${adjustments.length}`,
          );
        }

        if (type === 'CIELO03') {
          const { inserted, skippedDup, errors: importErrors } = await this.importEdiTransactions(
            type,
            transactions,
            file.filename,
          );
          this.logger.log(
            `Arquivo ${file.filename}: E=${transactions.length} inserted=${inserted} skippedDup=${skippedDup} errors=${importErrors}`,
          );
        } else if (type === 'CIELO04') {
          const adjustmentResult = await this.importCielo04AdjustmentBlocks(
            adjustmentBlocks,
            sha256,
            file.filename,
            headerDate,
          );
          this.logger.log(
            `Arquivo ${file.filename}: ajustes: D=${adjustmentResult.totalAdjustments} inserted=${adjustmentResult.insertedAdjustments} skippedDup=${adjustmentResult.skippedAdjustments} errors=${adjustmentResult.adjustmentErrors}`,
          );
          this.logger.log(
            `Arquivo ${file.filename}: itens: E=${adjustmentResult.totalItems} inserted=${adjustmentResult.insertedItems} skippedDup=${adjustmentResult.skippedItems} errors=${adjustmentResult.itemErrors}`,
          );
        } else if (type === 'CIELO16') {
          const { inserted, skippedDup, errors: importErrors } = await this.importEdiTransactions(
            type,
            transactions,
            file.filename,
          );
          this.logger.log(
            `Arquivo ${file.filename}: CIELO16 detalhes: 8=${transactions.length} inserted=${inserted} skippedDup=${skippedDup} errors=${importErrors}`,
          );
          if (verboseEnabled && trailerLine) {
            this.logger.debug(`CIELO16 trailer: ${trailerLine.slice(0, 120)}`);
          }
        }
        if (transactions.length && dateRange.minSaleDate && dateRange.maxSaleDate) {
          await this.canonDuplicateCleanupService.cleanupAfterImport(
            'CIELO',
            this.toDateOnlyString(dateRange.minSaleDate),
            this.toDateOnlyString(dateRange.maxSaleDate),
          );
        }
        await this.recordFile(file, sha256, type, fileStatus, warningMessage, lineCount);
        await this.moveFile(fullPath, archiveDir);
        processed += 1;
      } catch (error) {
        errors += 1;
        const message = error instanceof Error ? error.message : 'Erro desconhecido';
        this.logger.error(`Falha ao processar ${file.filename}: ${message}`);
        try {
          await this.recordFile(file, '', 'UNKNOWN', 'ERROR', message, null);
        } catch (dbError) {
          const dbMessage = dbError instanceof Error ? dbError.message : 'Erro ao registrar';
          this.logger.error(`Falha ao registrar T_EDI_FILES: ${dbMessage}`);
        }
        await this.safeMoveToError(fullPath, errorDir);
      }
    }

    return { processed, errors, skipped, unknown };
  }

  public isVerboseEnabled(): boolean {
    const raw = this.configService.get<string>('CIELO_EDI_VERBOSE');
    if (!raw) {
      return false;
    }
    const normalized = raw.trim().toLowerCase();
    return normalized === 'true' || normalized === '1' || normalized === 'yes';
  }

  private extractFileBaseDate(filename: string): Date | null {
    const match = filename.match(/_(\d{8})_/);
    const fallback = !match ? filename.match(/(\d{8})/) : null;
    const raw = match?.[1] ?? fallback?.[1] ?? '';
    if (!raw) {
      return null;
    }
    const year = Number(raw.slice(0, 4));
    const month = Number(raw.slice(4, 6));
    const day = Number(raw.slice(6, 8));
    if (!year || !month || !day) {
      return null;
    }
    return new Date(year, month - 1, day, 0, 0, 0, 0);
  }

  private getSaleDateRange(transactions: EdiTransaction[]): {
    minSaleDate: string | null;
    maxSaleDate: string | null;
  } {
    let minSaleDate: string | null = null;
    let maxSaleDate: string | null = null;
    for (const record of transactions) {
      const saleDate = record.saleDatetime?.trim() ?? '';
      if (!this.isSaleDatetimeString(saleDate)) {
        continue;
      }
      if (!minSaleDate || saleDate < minSaleDate) {
        minSaleDate = saleDate;
      }
      if (!maxSaleDate || saleDate > maxSaleDate) {
        maxSaleDate = saleDate;
      }
    }
    return { minSaleDate, maxSaleDate };
  }

  private buildFutureSaleDateWarning(
    fileBaseDate: Date | null,
    maxSaleDate: string | null,
    type: EdiFileType,
  ): string | null {
    if (!fileBaseDate || !maxSaleDate) {
      return null;
    }
    const limit = new Date(fileBaseDate);
    limit.setDate(limit.getDate() + 15);
    const limitText = this.formatTimestampFromDate(limit);
    if (maxSaleDate <= limitText) {
      return null;
    }
    return `Venda com data muito futura detectada: maxSale=${maxSaleDate} fileDate=${this.formatTimestampFromDate(fileBaseDate)} type=${type}`;
  }

  private formatLogValue(value: string | Date | null): string {
    if (!value) {
      return 'null';
    }
    if (value instanceof Date) {
      return this.formatTimestampFromDate(value);
    }
    return value;
  }

  private formatTimestampFromDate(value: Date): string {
    const pad = (input: number) => String(input).padStart(2, '0');
    const year = value.getFullYear();
    const month = pad(value.getMonth() + 1);
    const day = pad(value.getDate());
    const hours = pad(value.getHours());
    const minutes = pad(value.getMinutes());
    const seconds = pad(value.getSeconds());
    return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
  }

  private isSaleDatetimeString(value: string): boolean {
    if (value.length !== 19) {
      return false;
    }
    const parts = value.split(' ');
    if (parts.length !== 2) {
      return false;
    }
    const [datePart, timePart] = parts;
    if (!/^\d{4}-\d{2}-\d{2}$/.test(datePart)) {
      return false;
    }
    if (!/^\d{2}:\d{2}:\d{2}$/.test(timePart)) {
      return false;
    }
    return true;
  }

  private toFbTimestamp(value: Date | null): string | null {
    if (!value) {
      return null;
    }
    const iso = value.toISOString();
    return iso.slice(0, 19).replace('T', ' ');
  }

  private toFbDate(value: unknown): string | null {
    if (!value) {
      return null;
    }
    if (value instanceof Date) {
      return value.toISOString().slice(0, 10);
    }
    const text = String(value).trim();
    if (/^\d{4}-\d{2}-\d{2}/.test(text)) {
      return text.slice(0, 10);
    }
    return null;
  }

  private async getTableColumns(tableName: string): Promise<Set<string>> {
    const upperName = tableName.toUpperCase();
    const cached = this.tableColumnsCache.get(upperName);
    if (cached) {
      return cached;
    }
    const rows = await this.dbService.query<{ FIELD_NAME: string }>(
      "SELECT TRIM(rf.RDB$FIELD_NAME) as FIELD_NAME FROM RDB$RELATION_FIELDS rf WHERE rf.RDB$RELATION_NAME = ?",
      [upperName],
    );
    const set = new Set<string>(
      rows.map((row) => String(row.FIELD_NAME).trim().toUpperCase()),
    );
    this.tableColumnsCache.set(upperName, set);
    if (upperName === 'T_CIELO_SALES') {
      this.cieloSalesColumnsCache = set;
    }
    return set;
  }

  private async getCieloSalesColumns(): Promise<Set<string>> {
    if (this.cieloSalesColumnsCache) {
      return this.cieloSalesColumnsCache;
    }
    return this.getTableColumns('T_CIELO_SALES');
  }

  private canonicalizeCieloColumn(column: string, available?: Set<string>): string {
    const upper = column.toUpperCase();
    if (upper === 'E_BRANCH') {
      if (available?.has('E_AGENCY_NO')) {
        return 'E_AGENCY_NO';
      }
      if (available?.has('E_BRANCH_NO')) {
        return 'E_BRANCH_NO';
      }
    }
    const aliasMap: Record<string, string> = {
      E_LIQUIDATION_BRAND: 'E_SETTLEMENT_BRAND',
      E_LIQUIDATION_TYPE: 'E_SETTLEMENT_TYPE',
      E_ENTRY_TYPE: 'E_ENTRY_TYPE_CODE',
      E_CHAVE_UR: 'E_UR_KEY',
      E_NEGOTIATION_CODE: 'E_RECEIVED_TRANS_CODE',
      E_ORDER_REFERENCE: 'E_ORDER_CODE',
      E_TERMINAL_LOGIC_NO: 'E_LOGICAL_TERMINAL_NO',
      E_BANK: 'E_BANK_NO',
      E_BRANCH: 'E_AGENCY_NO',
      E_ACCOUNT: 'E_ACCOUNT_NO',
      E_ACCOUNT_DIGIT: 'E_ACCOUNT_DV',
      E_BATCH_NUMBER: 'E_BATCH_NO',
      E_REJECTION_REASON: 'E_REJECT_REASON_CODE',
      E_IND_CUSTOMER_INSTALLMENT: 'E_FLAG_CLIENT_INSTALLMENT',
      E_IND_RECEIVABLE_NEGOTIATION: 'E_FLAG_RECEIVABLES_NEG',
      E_IND_REJECTED: 'E_FLAG_REJECTED',
    };
    return aliasMap[upper] ?? upper;
  }

  private getCieloTransactionColumns(): string[] {
    return [
      'SALE_DATETIME',
      'ESTABLISHMENT_NO',
      'PAYMENT_METHOD',
      'BRAND',
      'GROSS_AMOUNT',
      'FEE_AMOUNT',
      'NET_AMOUNT',
      'STATUS',
      'ENTRY_TYPE',
      'REASON',
      'ENTRY_DATE',
      'SETTLEMENT_DATE',
      'AUTH_CODE',
      'NSU_DOC',
      'SALE_CODE',
      'TID',
      'PIX_ID',
      'TX_ID',
      'PIX_PAYMENT_ID',
      'MACHINE_NUMBER',
      'ROW_HASH',
      'E_RECORD_TYPE',
      'E_SUBMIT_ESTABLISHMENT',
      'E_LIQUIDATION_BRAND',
      'E_LIQUIDATION_TYPE',
      'E_INSTALLMENT_NO',
      'E_INSTALLMENT_TOTAL',
      'E_AUTH_CODE',
      'E_ENTRY_TYPE',
      'E_CHAVE_UR',
      'E_NEGOTIATION_CODE',
      'E_ADJUSTMENT_CODE',
      'E_PAYMENT_METHOD_CODE',
      'E_IND_PROMO',
      'E_IND_DCC',
      'E_IND_MIN_COMMISSION',
      'E_IND_RA_TC',
      'E_IND_ZERO_FEE',
      'E_IND_REJECTED',
      'E_IND_LATE_SALE',
      'E_CARD_BIN',
      'E_CARD_LAST4',
      'E_NSU_DOC',
      'E_INVOICE_NO',
      'E_TID',
      'E_ORDER_REFERENCE',
      'E_MDR_RATE',
      'E_RA_RATE',
      'E_SALE_RATE',
      'E_TOTAL_AMOUNT_SIGN',
      'E_TOTAL_AMOUNT',
      'E_GROSS_AMOUNT_SIGN',
      'E_GROSS_AMOUNT',
      'E_NET_AMOUNT_SIGN',
      'E_NET_AMOUNT',
      'E_COMMISSION_SIGN',
      'E_COMMISSION_AMOUNT',
      'E_MIN_COMMISSION_SIGN',
      'E_MIN_COMMISSION_AMOUNT',
      'E_ENTRY_SIGN',
      'E_ENTRY_AMOUNT',
      'E_MDR_FEE_SIGN',
      'E_MDR_FEE_AMOUNT',
      'E_FAST_RECEIVE_SIGN',
      'E_FAST_RECEIVE_AMOUNT',
      'E_CASHOUT_SIGN',
      'E_CASHOUT_AMOUNT',
      'E_SHIPMENT_FEE_SIGN',
      'E_SHIPMENT_FEE_AMOUNT',
      'E_PENDING_SIGN',
      'E_PENDING_AMOUNT',
      'E_DEBT_TOTAL_SIGN',
      'E_DEBT_TOTAL_AMOUNT',
      'E_CHARGED_SIGN',
      'E_CHARGED_AMOUNT',
      'E_ADMIN_FEE_SIGN',
      'E_ADMIN_FEE_AMOUNT',
      'E_PROMO_SIGN',
      'E_PROMO_AMOUNT',
      'E_DCC_SIGN',
      'E_DCC_AMOUNT',
      'E_TIME_HHMMSS',
      'E_CARD_GROUP',
      'E_RECEIVER_DOCUMENT',
      'E_AUTH_BRAND',
      'E_SALE_UNIQUE_CODE',
      'E_SALE_ORIGINAL_CODE',
      'E_NEGOTIATION_EFFECT_ID',
      'E_SALES_CHANNEL',
      'E_TERMINAL_LOGIC_NO',
      'E_ORIGINAL_ENTRY_TYPE',
      'E_TRANSACTION_TYPE',
      'E_CIELO_USAGE_1',
      'E_PRICING_MODEL_CODE',
      'E_AUTH_DATE',
      'E_CAPTURE_DATE',
      'E_ENTRY_DATE',
      'E_ORIGINAL_ENTRY_DATE',
      'E_BATCH_NUMBER',
      'E_PROCESSED_TRANSACTION_NO',
      'E_REJECTION_REASON',
      'E_SETTLEMENT_BLOCK',
      'E_IND_CUSTOMER_INSTALLMENT',
      'E_BANK',
      'E_BRANCH',
      'E_ACCOUNT',
      'E_ACCOUNT_DIGIT',
      'E_ARN',
      'E_IND_RECEIVABLE_NEGOTIATION',
      'E_CAPTURE_TYPE',
      'E_NEGOTIATOR_DOCUMENT',
      'E_CIELO_USAGE_2',
      'E_FILE_HEADER_DATE',
      'E_RAW_LINE',
      'CANON_SALE_DATE',
      'CANON_METHOD',
      'CANON_METHOD_GROUP',
      'CANON_BRAND',
      'CANON_TERMINAL_NO',
      'CANON_AUTH_CODE',
      'CANON_NSU',
      'CANON_GROSS_AMOUNT',
      'CANON_FEE_AMOUNT',
      'CANON_NET_AMOUNT',
      'CANON_PERC_TAXA',
      'CANON_INSTALLMENT_NO',
      'CANON_INSTALLMENT_TOTAL',
    ];
  }

  private getCieloAdjustmentColumns(): string[] {
    return [
      'ADJUSTMENT_DATETIME',
      'ESTABLISHMENT_NO',
      'PAYMENT_METHOD',
      'BRAND',
      'NSU_DOC',
      'AUTH_CODE',
      'ENTRY_TYPE',
      'REASON',
      'GROSS_AMOUNT',
      'FEE_AMOUNT',
      'NET_AMOUNT',
      'SETTLEMENT_DATE',
      'FILE_SHA256',
      'ROW_HASH',
      'FILE_NAME',
      'FILE_SEQ',
      'FILE_DATE',
      'LINE_NO',
      'EC',
      'ADJ_TYPE_CODE',
      'ADJ_REASON_CODE',
      'ADJ_GROSS_SIGN',
      'ADJ_GROSS',
      'ADJ_FEE_SIGN',
      'ADJ_FEE',
      'ADJ_NET_SIGN',
      'ADJ_NET',
      'REFERENCE_DATE',
      'UR_KEY',
      'NSU_ORIGINAL',
      'BLOCK_HASH',
    ];
  }

  private getCieloAdjustmentItemColumns(): string[] {
    return [
      'AJUSTE_ID',
      'SALE_ID',
      'FILE_NAME',
      'LINE_NO',
      'EC',
      'NSU_CIELO',
      'AUTH_CODE',
      'SALE_DATE',
      'SALE_TIME',
      'BRAND_CODE',
      'PRODUCT_CODE',
      'CARD_BIN',
      'CARD_LAST4',
      'UR_KEY',
      'GROSS',
      'NET',
      'FEE',
      'RAW_LINE',
      'ROW_HASH',
    ];
  }

  private decodePaymentMethod(code?: string | null): string | null {
    const normalized = code?.trim();
    if (!normalized) {
      return null;
    }
    if (normalized === '010' || normalized === '040' || normalized === '041') {
      return 'DEBITO';
    }
    if (normalized === '011') {
      return 'CREDITO_A_VISTA';
    }
    if (normalized === '111') {
      return 'CREDITO_PARCELADO';
    }
    if (normalized === '031' || normalized === '071') {
      return 'VOUCHER';
    }
    return null;
  }

  private decodeSettlementType(code?: string | null): string | null {
    const normalized = code?.trim();
    if (!normalized) {
      return null;
    }
    switch (normalized) {
      case '001':
        return 'CREDITO';
      case '002':
        return 'DEBITO';
      case '003':
        return 'VOUCHER';
      case '004':
        return 'PRE_PAGO';
      case '005':
        return 'CREDITO_PARCELADO';
      default:
        return null;
    }
  }

  private decodeStatus(rejectedFlag?: string | null): string | null {
    const rejected = rejectedFlag?.trim().toUpperCase();
    if (rejected === 'S') {
      return 'REJEITADA';
    }
    if (rejected === 'N') {
      return 'APROVADA';
    }
    return null;
  }

  private parseDateDDMMAAAA(raw: string): string | null {
    const trimmed = raw.trim();
    if (trimmed.length !== 8) {
      return null;
    }
    for (const char of trimmed) {
      if (char < '0' || char > '9') {
        return null;
      }
    }
    const day = trimmed.slice(0, 2);
    const month = trimmed.slice(2, 4);
    const year = trimmed.slice(4, 8);
    const monthNum = Number(month);
    const dayNum = Number(day);
    if (monthNum < 1 || monthNum > 12 || dayNum < 1 || dayNum > 31) {
      return null;
    }
    return `${year}-${month}-${day}`;
  }

  private parseTimeHHMMSS(raw: string): string | null {
    const trimmed = raw.trim();
    if (trimmed.length !== 6) {
      return null;
    }
    for (const char of trimmed) {
      if (char < '0' || char > '9') {
        return null;
      }
    }
    const hh = Number(trimmed.slice(0, 2));
    const mm = Number(trimmed.slice(2, 4));
    const ss = Number(trimmed.slice(4, 6));
    if (hh < 0 || hh > 23 || mm < 0 || mm > 59 || ss < 0 || ss > 59) {
      return null;
    }
    return trimmed;
  }

  private normalizeDbDate(value: unknown): string | null {
    if (!value) {
      return null;
    }
    if (value instanceof Date) {
      return this.toFbDate(value);
    }
    const text = String(value).trim();
    if (/^\d{4}-\d{2}-\d{2}/.test(text)) {
      return text.slice(0, 10);
    }
    return null;
  }

  private computeSaleFieldsFromRawLine(
    rawLine: string,
    headerDate: string | null,
  ): {
    saleDatetime: string | null;
    rejectedFlag: string | null;
    rejectReason: string | null;
    status: string | null;
  } {
    const authDate = this.parseDateDDMMAAAA(rawLine.slice(565, 573));
    const captureDate = this.parseDateDDMMAAAA(rawLine.slice(573, 581));
    const entryDate = this.parseDateDDMMAAAA(rawLine.slice(581, 589));
    const timeRaw = this.parseTimeHHMMSS(rawLine.slice(470, 476)) ?? '000000';
    const saleDate = authDate || captureDate || entryDate || headerDate;
    const hh = timeRaw.slice(0, 2);
    const mi = timeRaw.slice(2, 4);
    const ss = timeRaw.slice(4, 6);
    const saleDatetime = saleDate ? `${saleDate} ${hh}:${mi}:${ss}` : null;
    const rejectedFlag = rawLine.slice(163, 164).trim().toUpperCase() || null;
    const rejectReason =
      rejectedFlag === 'S' ? rawLine.slice(626, 629).trim() || null : null;
    const status = this.decodeStatus(rejectedFlag);
    return { saleDatetime, rejectedFlag, rejectReason, status };
  }

  private getCieloColumnValue(
    column: string,
    record: EdiTransaction,
    verboseEnabled: boolean,
  ): unknown {
    switch (column) {
      case 'SALE_DATETIME':
        return record.saleDatetime ?? null;
      case 'ESTABLISHMENT_NO':
        return record.establishmentNo;
      case 'PAYMENT_METHOD': {
        if (record.paymentMethod) {
          return record.paymentMethod;
        }
        let decoded = this.decodePaymentMethod(record.ePaymentMethodCode);
        if (!decoded) {
          decoded = this.decodeSettlementType(record.eLiquidationType);
          if (!decoded && verboseEnabled && record.eLiquidationType?.trim()) {
            this.logger.debug(
              `Unknown settlement type code: ${record.eLiquidationType}`,
            );
          }
        }
        if (!decoded && verboseEnabled && record.ePaymentMethodCode?.trim()) {
          this.logger.debug(
            `Unknown payment method code: ${record.ePaymentMethodCode}`,
          );
        }
        return decoded;
      }
      case 'BRAND':
        return record.brand;
      case 'GROSS_AMOUNT':
        return record.grossAmount;
      case 'FEE_AMOUNT':
        return record.feeAmount;
      case 'NET_AMOUNT':
        return record.netAmount;
      case 'STATUS': {
        if (record.status) {
          return record.status;
        }
        const decoded = this.decodeStatus(record.eIndicatorRejected);
        if (!decoded && verboseEnabled && record.eIndicatorRejected?.trim()) {
          this.logger.debug(`Unknown reject flag: ${record.eIndicatorRejected}`);
        }
        return decoded;
      }
      case 'ENTRY_TYPE':
        return record.entryType;
      case 'REASON':
        return record.reason;
      case 'ENTRY_DATE':
        return this.toFbDate(record.entryDate);
      case 'SETTLEMENT_DATE':
        return this.toFbDate(record.settlementDate);
      case 'AUTH_CODE':
        return record.authCode;
      case 'NSU_DOC':
        return record.nsuDoc;
      case 'SALE_CODE':
        return record.saleCode;
      case 'TID':
        return record.tid;
      case 'MACHINE_NUMBER':
        return record.machineNumber;
      case 'ROW_HASH':
        return record.rowHash;
      case 'E_RECORD_TYPE':
        return record.eRecordType;
      case 'E_SUBMIT_ESTABLISHMENT':
        return record.eSubmitEstablishment;
      case 'E_LIQUIDATION_BRAND':
        return record.eLiquidationBrand;
      case 'E_LIQUIDATION_TYPE':
        return record.eLiquidationType;
      case 'E_INSTALLMENT_NO':
        return record.eInstallmentNo;
      case 'E_INSTALLMENT_TOTAL':
        return record.eInstallmentTotal;
      case 'E_AUTH_CODE':
        return record.eAuthCode;
      case 'E_ENTRY_TYPE':
        return record.eEntryType;
      case 'E_CHAVE_UR':
        return record.eChaveUr;
      case 'E_NEGOTIATION_CODE':
        return record.eNegotiationCode;
      case 'E_ADJUSTMENT_CODE':
        return record.eAdjustmentCode;
      case 'E_PAYMENT_METHOD_CODE':
        return record.ePaymentMethodCode;
      case 'E_IND_PROMO':
        return record.eIndicatorPromo;
      case 'E_IND_DCC':
        return record.eIndicatorDcc;
      case 'E_IND_MIN_COMMISSION':
        return record.eIndicatorMinCommission;
      case 'E_IND_RA_TC':
        return record.eIndicatorRaTc;
      case 'E_IND_ZERO_FEE':
        return record.eIndicatorZeroFee;
      case 'E_IND_REJECTED':
        return record.eIndicatorRejected;
      case 'E_IND_LATE_SALE':
        return record.eIndicatorLateSale;
      case 'E_CARD_BIN':
        return record.eCardBin;
      case 'E_CARD_LAST4':
        return record.eCardLast4;
      case 'E_NSU_DOC':
        return record.eNsuDoc;
      case 'E_INVOICE_NO':
        return record.eInvoiceNo;
      case 'E_TID':
        return record.eTid;
      case 'E_ORDER_REFERENCE':
        return record.eOrderReference;
      case 'E_MDR_RATE':
        return record.eMdrRate;
      case 'E_RA_RATE':
        return record.eRaRate;
      case 'E_SALE_RATE':
        return record.eSaleRate;
      case 'E_TOTAL_AMOUNT_SIGN':
        return record.eTotalAmountSign;
      case 'E_TOTAL_AMOUNT':
        return record.eTotalAmount;
      case 'E_GROSS_AMOUNT_SIGN':
        return record.eGrossAmountSign;
      case 'E_GROSS_AMOUNT':
        return record.eGrossAmount;
      case 'E_NET_AMOUNT_SIGN':
        return record.eNetAmountSign;
      case 'E_NET_AMOUNT':
        return record.eNetAmount;
      case 'E_COMMISSION_SIGN':
        return record.eCommissionSign;
      case 'E_COMMISSION_AMOUNT':
        return record.eCommissionAmount;
      case 'E_MIN_COMMISSION_SIGN':
        return record.eMinCommissionSign;
      case 'E_MIN_COMMISSION_AMOUNT':
        return record.eMinCommissionAmount;
      case 'E_ENTRY_SIGN':
        return record.eEntrySign;
      case 'E_ENTRY_AMOUNT':
        return record.eEntryAmount;
      case 'E_MDR_FEE_SIGN':
        return record.eMdrFeeSign;
      case 'E_MDR_FEE_AMOUNT':
        return record.eMdrFeeAmount;
      case 'E_FAST_RECEIVE_SIGN':
        return record.eFastReceiveSign;
      case 'E_FAST_RECEIVE_AMOUNT':
        return record.eFastReceiveAmount;
      case 'E_CASHOUT_SIGN':
        return record.eCashoutSign;
      case 'E_CASHOUT_AMOUNT':
        return record.eCashoutAmount;
      case 'E_SHIPMENT_FEE_SIGN':
        return record.eShipmentFeeSign;
      case 'E_SHIPMENT_FEE_AMOUNT':
        return record.eShipmentFeeAmount;
      case 'E_PENDING_SIGN':
        return record.ePendingSign;
      case 'E_PENDING_AMOUNT':
        return record.ePendingAmount;
      case 'E_DEBT_TOTAL_SIGN':
        return record.eDebtTotalSign;
      case 'E_DEBT_TOTAL_AMOUNT':
        return record.eDebtTotalAmount;
      case 'E_CHARGED_SIGN':
        return record.eChargedSign;
      case 'E_CHARGED_AMOUNT':
        return record.eChargedAmount;
      case 'E_ADMIN_FEE_SIGN':
        return record.eAdminFeeSign;
      case 'E_ADMIN_FEE_AMOUNT':
        return record.eAdminFeeAmount;
      case 'E_PROMO_SIGN':
        return record.ePromoSign;
      case 'E_PROMO_AMOUNT':
        return record.ePromoAmount;
      case 'E_DCC_SIGN':
        return record.eDccSign;
      case 'E_DCC_AMOUNT':
        return record.eDccAmount;
      case 'E_TIME_HHMMSS':
        return record.eTimeHhmmss;
      case 'E_CARD_GROUP':
        return record.eCardGroup;
      case 'E_RECEIVER_DOCUMENT':
        return record.eReceiverDocument;
      case 'E_AUTH_BRAND':
        return record.eAuthBrand;
      case 'E_SALE_UNIQUE_CODE':
        return record.eSaleUniqueCode;
      case 'E_SALE_ORIGINAL_CODE':
        return record.eSaleOriginalCode;
      case 'E_NEGOTIATION_EFFECT_ID':
        return record.eNegotiationEffectId;
      case 'E_SALES_CHANNEL':
        return record.eSalesChannel;
      case 'E_TERMINAL_LOGIC_NO':
        return record.eTerminalLogicNo;
      case 'E_ORIGINAL_ENTRY_TYPE':
        return record.eOriginalEntryType;
      case 'E_TRANSACTION_TYPE':
        return record.eTransactionType;
      case 'E_CIELO_USAGE_1':
        return record.eCieloUsage1;
      case 'E_PRICING_MODEL_CODE':
        return record.ePricingModelCode;
      case 'E_AUTH_DATE':
        return record.eAuthDate;
      case 'E_CAPTURE_DATE':
        return record.eCaptureDate;
      case 'E_ENTRY_DATE':
        return record.eEntryDate;
      case 'E_ORIGINAL_ENTRY_DATE':
        return record.eOriginalEntryDate;
      case 'E_BATCH_NUMBER':
        return record.eBatchNumber;
      case 'E_PROCESSED_TRANSACTION_NO':
        return record.eProcessedTransactionNo;
      case 'E_REJECTION_REASON':
        return record.eRejectionReason;
      case 'E_SETTLEMENT_BLOCK':
        return record.eSettlementBlock;
      case 'E_IND_CUSTOMER_INSTALLMENT':
        return record.eIndicatorCustomerInstallment;
      case 'E_BANK':
        return record.eBank;
      case 'E_BRANCH':
        return record.eBranch;
      case 'E_ACCOUNT':
        return record.eAccount;
      case 'E_ACCOUNT_DIGIT':
        return record.eAccountDigit;
      case 'E_ARN':
        return record.eArn;
      case 'E_IND_RECEIVABLE_NEGOTIATION':
        return record.eIndicatorReceivableNegotiation;
      case 'E_CAPTURE_TYPE':
        return record.eCaptureType;
      case 'E_NEGOTIATOR_DOCUMENT':
        return record.eNegotiatorDocument;
      case 'E_CIELO_USAGE_2':
        return record.eCieloUsage2;
      case 'E_FILE_HEADER_DATE':
        return record.eFileHeaderDate;
      case 'E_RAW_LINE':
        return record.eRawLine;
      case 'PIX_ID':
        return record.pixId ?? null;
      case 'TX_ID':
        return record.txId ?? null;
      case 'PIX_PAYMENT_ID':
        return record.pixPaymentId ?? null;
      default:
        return null;
    }
  }

  private getAdjustmentColumnValue(
    column: string,
    record: EdiAdjustment,
    fileSha256: string | null,
    fileName: string,
  ): unknown {
    switch (column) {
      case 'ADJUSTMENT_DATETIME':
        return this.toFbTimestamp(record.adjustmentDatetime);
      case 'ESTABLISHMENT_NO':
        return record.establishmentNo;
      case 'PAYMENT_METHOD':
        return record.paymentMethod;
      case 'BRAND':
        return record.brand;
      case 'NSU_DOC':
        return record.nsuDoc;
      case 'AUTH_CODE':
        return record.authCode;
      case 'ENTRY_TYPE':
        return record.entryType;
      case 'REASON':
        return record.reason;
      case 'GROSS_AMOUNT':
        return record.grossAmount;
      case 'FEE_AMOUNT':
        return record.feeAmount;
      case 'NET_AMOUNT':
        return record.netAmount;
      case 'SETTLEMENT_DATE':
        return this.toFbDate(record.settlementDate ?? null);
      case 'FILE_SHA256':
        return fileSha256 || null;
      case 'ROW_HASH':
        return record.rowHash;
      case 'FILE_NAME':
        return record.fileName ?? fileName;
      case 'FILE_SEQ':
        return record.fileSeq ?? null;
      case 'FILE_DATE':
        return this.toFbDate(record.fileDate ?? null);
      case 'LINE_NO':
        return record.lineNo ?? null;
      case 'EC':
        return record.ec ?? record.establishmentNo;
      case 'ADJ_TYPE_CODE':
        return record.adjTypeCode ?? null;
      case 'ADJ_REASON_CODE':
        return record.adjReasonCode ?? null;
      case 'ADJ_GROSS_SIGN':
        return record.adjGrossSign ?? null;
      case 'ADJ_GROSS':
        return record.adjGross ?? null;
      case 'ADJ_FEE_SIGN':
        return record.adjFeeSign ?? null;
      case 'ADJ_FEE':
        return record.adjFee ?? null;
      case 'ADJ_NET_SIGN':
        return record.adjNetSign ?? null;
      case 'ADJ_NET':
        return record.adjNet ?? null;
      case 'REFERENCE_DATE':
        return this.toFbDate(record.referenceDate ?? null);
      case 'UR_KEY':
        return record.urKey ?? null;
      case 'NSU_ORIGINAL':
        return record.nsuOriginal ?? null;
      case 'BLOCK_HASH':
        return record.blockHash ?? null;
      default:
        return null;
    }
  }

  private getAdjustmentItemColumnValue(
    column: string,
    record: EdiAdjustmentItem,
  ): unknown {
    switch (column) {
      case 'AJUSTE_ID':
        return record.ajusteId ?? null;
      case 'SALE_ID':
        return record.saleId ?? null;
      case 'FILE_NAME':
        return record.fileName ?? null;
      case 'LINE_NO':
        return record.lineNo ?? null;
      case 'EC':
        return record.ec ?? null;
      case 'NSU_CIELO':
        return record.nsuCielo ?? null;
      case 'AUTH_CODE':
        return record.authCode ?? null;
      case 'SALE_DATE':
        return this.toFbDate(record.saleDate ?? null);
      case 'SALE_TIME':
        return record.saleTime ?? null;
      case 'BRAND_CODE':
        return record.brandCode ?? null;
      case 'PRODUCT_CODE':
        return record.productCode ?? null;
      case 'CARD_BIN':
        return record.cardBin ?? null;
      case 'CARD_LAST4':
        return record.cardLast4 ?? null;
      case 'UR_KEY':
        return record.urKey ?? null;
      case 'GROSS':
        return record.gross ?? null;
      case 'NET':
        return record.net ?? null;
      case 'FEE':
        return record.fee ?? null;
      case 'RAW_LINE':
        return record.rawLine ?? null;
      case 'ROW_HASH':
        return record.rowHash;
      default:
        return null;
    }
  }

  private detectType(filename: string, content: string): EdiFileType {
    const name = filename.toUpperCase();
    const prefixes: EdiFileType[] = ['CIELO03', 'CIELO04', 'CIELO09', 'CIELO15', 'CIELO16'];
    for (const prefix of prefixes) {
      if (name.startsWith(prefix)) {
        return prefix;
      }
    }

    const firstLine = content.split(/\r?\n/)[0]?.toUpperCase() ?? '';
    for (const prefix of prefixes) {
      if (firstLine.startsWith(prefix)) {
        return prefix;
      }
    }

    return 'UNKNOWN';
  }

  private toDateOnlyString(date: Date | string): string {
    if (typeof date === 'string') {
      return date.slice(0, 10);
    }
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
  }

  private async importEdiTransactions(
    type: EdiFileType,
    transactions: EdiTransaction[],
    fileName: string,
  ): Promise<{ inserted: number; skippedDup: number; errors: number }> {
    if (type === 'CIELO04') {
      const message = `CIELO04 nao pode inserir em T_CIELO_SALES: file=${fileName}`;
      this.logger.error(message);
      throw new Error(message);
    }
    const selectSql = 'SELECT 1 FROM T_CIELO_SALES WHERE ROW_HASH = ?';
    const logicalColumns = this.getCieloTransactionColumns();
    const availableColumns = await this.getCieloSalesColumns();
    const canonicalColumns = logicalColumns.map((column) =>
      this.canonicalizeCieloColumn(column, availableColumns),
    );
    const orderedUniqueColumns: string[] = [];
    const seen = new Set<string>();
    for (const column of canonicalColumns) {
      if (seen.has(column)) {
        continue;
      }
      seen.add(column);
      orderedUniqueColumns.push(column);
    }
    const filteredColumns = orderedUniqueColumns.filter((column) =>
      availableColumns.has(column),
    );
    const skippedColumns = orderedUniqueColumns.filter(
      (column) => !availableColumns.has(column),
    );
    if (skippedColumns.length) {
      this.logger.debug(`Skipping unknown columns: ${skippedColumns.join(', ')}`);
    }

    const verboseEnabled = this.isVerboseEnabled();

    const dateColumns = new Set([
      'ENTRY_DATE',
      'SETTLEMENT_DATE',
      'E_AUTH_DATE',
      'E_CAPTURE_DATE',
      'E_ENTRY_DATE',
      'E_ORIGINAL_ENTRY_DATE',
      'E_FILE_HEADER_DATE',
    ]);
    const insertValues = filteredColumns.map((column) => {
      if (column === 'SALE_DATETIME') {
        return 'CAST(? AS TIMESTAMP)';
      }
      if (dateColumns.has(column)) {
        return 'CAST(? AS DATE)';
      }
      return '?';
    });
    const insertSql = `INSERT INTO T_CIELO_SALES (${filteredColumns.join(', ')}) VALUES (${insertValues.join(', ')})`;
    if (verboseEnabled && !this.insertColumnsLogged) {
      this.logger.debug(`CIELO EDI columnsUsed: ${filteredColumns.join(', ')}`);
      this.logger.debug(`CIELO EDI insertSql: ${insertSql}`);
      this.insertColumnsLogged = true;
    }
    let inserted = 0;
    let skippedDup = 0;
    let errors = 0;
    let loggedParams = false;
    let loggedSamples = 0;
    let loggedCanonSamples = 0;

    try {
      await this.dbService.transaction(async (transaction) => {
        for (const record of transactions) {
          try {
            let storedRowHash = record.rowHash;
            const existing = await this.dbService.queryTx<{ exists: number }>(
              transaction,
              selectSql,
              [record.rowHash],
            );
            if (existing.length > 0) {
              skippedDup += 1;
              storedRowHash = this.buildStoredRowHash(record.rowHash, skippedDup);
            }
            if (!record.saleDatetime) {
              this.logger.warn(
                `SALE_DATETIME nulo no EDI Cielo: file=${fileName} rowHash=${record.rowHash}`,
              );
            }
            this.logger.debug(
              {
                saleDatetime: record.saleDatetime,
                fbSaleDatetime: record.saleDatetime ?? null,
              },
              'Cielo parsed datetime',
            );
            if (verboseEnabled && loggedSamples < 3 && record.eRawLine && type === 'CIELO03') {
              const raw = record.eRawLine;
              const rejectedFlag = raw.slice(163, 164);
              const rejectReason = raw.slice(626, 629);
              const authDateRaw = raw.slice(565, 573);
              const captureDateRaw = raw.slice(573, 581);
              const entryDateRaw = raw.slice(581, 589);
              const timeRaw = raw.slice(470, 476);
              const settlementBrandRaw = raw.slice(11, 14);
              const settlementTypeRaw = raw.slice(14, 17);
              const derivedPayment = this.decodePaymentMethod(record.ePaymentMethodCode)
                ?? this.decodeSettlementType(record.eLiquidationType);
              this.logger.debug(
                {
                  filename: fileName,
                  rawRejectedFlag: rejectedFlag,
                  rawRejectReason: rejectReason,
                  rawAuthDate: authDateRaw,
                  rawCaptureDate: captureDateRaw,
                  rawEntryDate: entryDateRaw,
                  rawTime: timeRaw,
                  rawSettlementBrand: settlementBrandRaw,
                  rawSettlementType: settlementTypeRaw,
                  saleDatetime: record.saleDatetime,
                  status: this.decodeStatus(record.eIndicatorRejected),
                  brand: record.brand,
                  paymentMethod: derivedPayment,
                },
                'CIELO EDI sample record',
              );
              loggedSamples += 1;
            }
            const valuesByColumn = new Map<string, unknown>();
            const filteredSet = new Set(filteredColumns);
            for (const logicalColumn of logicalColumns) {
              const canonical = this.canonicalizeCieloColumn(
                logicalColumn,
                availableColumns,
              );
              if (!filteredSet.has(canonical) || valuesByColumn.has(canonical)) {
                continue;
              }
              valuesByColumn.set(
                canonical,
                this.getCieloColumnValue(logicalColumn, record, verboseEnabled),
              );
            }
            const canon = canonizeCielo({
              SALE_DATETIME: record.saleDatetime ?? null,
              GROSS_AMOUNT: record.grossAmount ?? null,
              FEE_AMOUNT: record.feeAmount ?? null,
              NET_AMOUNT: record.netAmount ?? null,
              MACHINE_NUMBER: record.machineNumber ?? null,
              E_LOGICAL_TERMINAL_NO: record.eTerminalLogicNo ?? null,
              AUTH_CODE: record.authCode ?? record.eAuthCode ?? null,
              NSU_DOC: record.nsuDoc ?? record.eNsuDoc ?? null,
              E_INSTALLMENT_TOTAL: record.eInstallmentTotal ?? null,
              E_INSTALLMENT_NO: record.eInstallmentNo ?? null,
              PAYMENT_METHOD: record.paymentMethod ?? null,
              ENTRY_TYPE: record.entryType ?? null,
              BRAND: record.brand ?? record.eAuthBrand ?? null,
            });
            const canonMap: Record<string, unknown> = {
              CANON_SALE_DATE: canon.CANON_SALE_DATE,
              CANON_METHOD: canon.CANON_METHOD,
              CANON_METHOD_GROUP: canon.CANON_METHOD_GROUP,
              CANON_BRAND: canon.CANON_BRAND,
              CANON_TERMINAL_NO: canon.CANON_TERMINAL_NO,
              CANON_AUTH_CODE: canon.CANON_AUTH_CODE,
              CANON_NSU: canon.CANON_NSU,
              CANON_GROSS_AMOUNT: canon.CANON_GROSS_AMOUNT,
              CANON_FEE_AMOUNT: canon.CANON_FEE_AMOUNT,
              CANON_NET_AMOUNT: canon.CANON_NET_AMOUNT,
              CANON_PERC_TAXA: canon.CANON_PERC_TAXA,
              CANON_INSTALLMENT_NO: canon.CANON_INSTALLMENT_NO,
              CANON_INSTALLMENT_TOTAL: canon.CANON_INSTALLMENT_TOTAL,
            };
            for (const [key, value] of Object.entries(canonMap)) {
              if (filteredSet.has(key)) {
                valuesByColumn.set(key, value ?? null);
              }
            }
            if (filteredSet.has('ROW_HASH')) {
              valuesByColumn.set('ROW_HASH', storedRowHash);
            }
            if (verboseEnabled && loggedCanonSamples < 3) {
              this.logger.debug(
                {
                  rawPaymentMethod: record.paymentMethod,
                  rawEntryType: record.entryType,
                  rawBrand: record.brand,
                  canon,
                },
                'CIELO EDI canon sample',
              );
              loggedCanonSamples += 1;
            }
            const values = filteredColumns.map((column) =>
              valuesByColumn.has(column) ? valuesByColumn.get(column) : null,
            );
            if (verboseEnabled && !loggedParams) {
              this.logger.debug({ params: values }, 'CIELO EDI insert params');
              loggedParams = true;
            }
            await this.dbService.executeTx(transaction, insertSql, values);
            inserted += 1;
          } catch (error) {
            errors += 1;
            const message = error instanceof Error ? error.message : 'Erro desconhecido';
            this.logger.error(
              `Falha ao inserir transacao Cielo (${fileName}): ${message}`,
            );
          }
        }
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro desconhecido';
      this.logger.error(`Falha ao importar transacoes Cielo (${fileName}): ${message}`);
      throw error;
    }

    return { inserted, skippedDup, errors };
  }

  private async importCielo04AdjustmentBlocks(
    blocks: Cielo04AdjustmentBlock[],
    fileSha256: string,
    fileName: string,
    headerDate: string | null,
  ): Promise<{
    totalAdjustments: number;
    insertedAdjustments: number;
    skippedAdjustments: number;
    adjustmentErrors: number;
    totalItems: number;
    insertedItems: number;
    skippedItems: number;
    itemErrors: number;
  }> {
    const verboseEnabled = this.isVerboseEnabled();
    const availableAdjustColumns = await this.getTableColumns('T_EDI_CIELO_AJUSTES');
    const availableItemColumns = await this.getTableColumns('T_EDI_CIELO_AJUSTES_ITENS');
    const logicalAdjustColumns = this.getCieloAdjustmentColumns();
    const logicalItemColumns = this.getCieloAdjustmentItemColumns();
    const adjustColumns = logicalAdjustColumns.filter((column) =>
      availableAdjustColumns.has(column),
    );
    const itemColumns = logicalItemColumns.filter((column) =>
      availableItemColumns.has(column),
    );

    const adjustmentDateColumns = new Set([
      'SETTLEMENT_DATE',
      'FILE_DATE',
      'REFERENCE_DATE',
    ]);
    const itemDateColumns = new Set(['SALE_DATE']);

    const adjustInsertValues = adjustColumns.map((column) => {
      if (column === 'ADJUSTMENT_DATETIME') {
        return 'CAST(? AS TIMESTAMP)';
      }
      if (adjustmentDateColumns.has(column)) {
        return 'CAST(? AS DATE)';
      }
      return '?';
    });
    const adjustInsertSql = `INSERT INTO T_EDI_CIELO_AJUSTES (${adjustColumns.join(', ')}) VALUES (${adjustInsertValues.join(', ')})`;

    const itemInsertValues = itemColumns.map((column) => {
      if (itemDateColumns.has(column)) {
        return 'CAST(? AS DATE)';
      }
      if (column === 'SALE_TIME') {
        return 'CAST(? AS TIME)';
      }
      return '?';
    });
    const itemInsertSql = `INSERT INTO T_EDI_CIELO_AJUSTES_ITENS (${itemColumns.join(', ')}) VALUES (${itemInsertValues.join(', ')})`;

    const selectAdjustSql = 'SELECT ID FROM T_EDI_CIELO_AJUSTES WHERE ROW_HASH = ?';
    const selectItemSql = 'SELECT 1 FROM T_EDI_CIELO_AJUSTES_ITENS WHERE ROW_HASH = ?';

    let insertedAdjustments = 0;
    let skippedAdjustments = 0;
    let adjustmentErrors = 0;
    let insertedItems = 0;
    let skippedItems = 0;
    let itemErrors = 0;
    let totalItems = 0;
    const totalAdjustments = blocks.length;

    await this.dbService.transaction(async (transaction) => {
      for (const block of blocks) {
        const record = block.adjustment;
        totalItems += block.items.length;
        record.fileName = record.fileName ?? fileName;
        record.fileDate = record.fileDate ?? headerDate ?? null;
        if (!record.blockHash && record.rawLine) {
          record.blockHash = createHash('sha256')
            .update(`CIELO04D|${fileSha256}|D|${record.rawLine.trimEnd()}`)
            .digest('hex');
        }
        let ajusteId: number | null = null;
        try {
          if (record.adjNet !== null && record.adjNet !== undefined && block.items.length) {
            const itemsNetSum = block.items.reduce((sum, item) => {
              return typeof item.net === 'number' ? sum + item.net : sum;
            }, 0);
            if (Math.abs(itemsNetSum - record.adjNet) > 0.01) {
              const message = `CIELO04 ajuste net diff: file=${fileName} line=${record.lineNo} rowHash=${record.rowHash} adjNet=${record.adjNet} itensNet=${itemsNetSum}`;
              this.logger.error(message);
              throw new Error(message);
            }
          }
          const existing = await this.dbService.queryTx<{ ID: number }>(
            transaction,
            selectAdjustSql,
            [record.rowHash],
          );
          if (existing.length > 0) {
            ajusteId = existing[0].ID ?? null;
            skippedAdjustments += 1;
          } else {
            const adjustValues = adjustColumns.map((column) =>
              this.getAdjustmentColumnValue(column, record, fileSha256, fileName),
            );
            await this.dbService.executeTx(transaction, adjustInsertSql, adjustValues);
            const inserted = await this.dbService.queryTx<{ ID: number }>(
              transaction,
              selectAdjustSql,
              [record.rowHash],
            );
            ajusteId = inserted[0]?.ID ?? null;
            insertedAdjustments += 1;
          }
        } catch (error) {
          adjustmentErrors += 1;
          const message = error instanceof Error ? error.message : 'Erro desconhecido';
          this.logger.error(
            `Falha ao inserir ajuste CIELO04 (${fileName}): ${message}`,
          );
        }

        if (!ajusteId) {
          this.logger.error(
            `AJUSTE_ID ausente para itens CIELO04: file=${fileName} rowHash=${record.rowHash}`,
          );
          continue;
        }

        for (const item of block.items) {
          try {
            const existingItem = await this.dbService.queryTx<{ exists: number }>(
              transaction,
              selectItemSql,
              [item.rowHash],
            );
            if (existingItem.length > 0) {
              skippedItems += 1;
              continue;
            }
            item.ajusteId = ajusteId;
            item.fileName = item.fileName ?? fileName;
            const itemValues = itemColumns.map((column) =>
              this.getAdjustmentItemColumnValue(column, item),
            );
            await this.dbService.executeTx(transaction, itemInsertSql, itemValues);
            insertedItems += 1;
          } catch (error) {
            itemErrors += 1;
            const message = error instanceof Error ? error.message : 'Erro desconhecido';
            this.logger.error(
              `Falha ao inserir item CIELO04 (${fileName}): ${message}`,
            );
          }
        }
      }
    });

    return {
      totalAdjustments,
      insertedAdjustments,
      skippedAdjustments,
      adjustmentErrors,
      totalItems,
      insertedItems,
      skippedItems,
      itemErrors,
    };
  }

  private async importEdiAdjustments(
    type: EdiFileType,
    adjustments: EdiAdjustment[],
    fileSha256: string,
    fileName: string,
  ): Promise<{ inserted: number; skippedDup: number; errors: number }> {
    const selectSql = 'SELECT 1 FROM T_EDI_CIELO_AJUSTES WHERE ROW_HASH = ?';
    const insertSql =
      'INSERT INTO T_EDI_CIELO_AJUSTES (ADJUSTMENT_DATETIME, ESTABLISHMENT_NO, PAYMENT_METHOD, BRAND, NSU_DOC, AUTH_CODE, ENTRY_TYPE, REASON, GROSS_AMOUNT, FEE_AMOUNT, NET_AMOUNT, SETTLEMENT_DATE, FILE_SHA256, ROW_HASH) VALUES (CAST(? AS TIMESTAMP), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS DATE), ?, ?)';

    let inserted = 0;
    let skippedDup = 0;
    let errors = 0;

    try {
      await this.dbService.transaction(async (transaction) => {
        for (const record of adjustments) {
          try {
            const existing = await this.dbService.queryTx<{ exists: number }>(
              transaction,
              selectSql,
              [record.rowHash],
            );
            if (existing.length > 0) {
              skippedDup += 1;
              continue;
            }
            await this.dbService.executeTx(transaction, insertSql, [
              this.toFbTimestamp(record.adjustmentDatetime),
              record.establishmentNo,
              record.paymentMethod,
              record.brand,
              record.nsuDoc,
              record.authCode,
              record.entryType,
              record.reason,
              record.grossAmount,
              record.feeAmount,
              record.netAmount,
              this.toFbDate(record.settlementDate),
              fileSha256 || null,
              record.rowHash,
            ]);
            inserted += 1;
          } catch (error) {
            errors += 1;
            const message = error instanceof Error ? error.message : 'Erro desconhecido';
            this.logger.error(
              `Falha ao inserir ajuste Cielo (${fileName}): ${message}`,
            );
          }
        }
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro desconhecido';
      this.logger.error(`Falha ao importar ajustes Cielo (${fileName}): ${message}`);
      throw error;
    }

    return { inserted, skippedDup, errors };
  }

  async reprocessSales(from: string, to: string) {
    const rows = await this.dbService.query<{
      ID: number;
      E_RAW_LINE: string | null;
      E_FILE_HEADER_DATE: unknown;
    }>(
      'SELECT ID, E_RAW_LINE, E_FILE_HEADER_DATE FROM T_CIELO_SALES WHERE E_FILE_HEADER_DATE BETWEEN CAST(? AS DATE) AND CAST(? AS DATE) AND E_RAW_LINE IS NOT NULL',
      [from, to],
    );
    const availableColumns = await this.getCieloSalesColumns();
    const updateColumns = [
      'SALE_DATETIME',
      'STATUS',
      'E_FLAG_REJECTED',
      'E_REJECT_REASON_CODE',
    ].filter((column) => availableColumns.has(column));
    if (!updateColumns.length) {
      return { updated: 0, skipped: rows.length };
    }
    const setClauses = updateColumns.map((column) =>
      column === 'SALE_DATETIME' ? 'SALE_DATETIME = CAST(? AS TIMESTAMP)' : `${column} = ?`,
    );
    const updateSql = `UPDATE T_CIELO_SALES SET ${setClauses.join(', ')} WHERE ID = ?`;
    let updated = 0;
    let skipped = 0;

    for (const row of rows) {
      if (!row.E_RAW_LINE) {
        skipped += 1;
        continue;
      }
      const headerDate = this.normalizeDbDate(row.E_FILE_HEADER_DATE);
      const computed = this.computeSaleFieldsFromRawLine(row.E_RAW_LINE, headerDate);
      const params: Array<string | number | null> = updateColumns.map((column) => {
        switch (column) {
          case 'SALE_DATETIME':
            return computed.saleDatetime;
          case 'STATUS':
            return computed.status;
          case 'E_FLAG_REJECTED':
            return computed.rejectedFlag;
          case 'E_REJECT_REASON_CODE':
            return computed.rejectReason;
          default:
            return null;
        }
      });
      params.push(row.ID);
      await this.dbService.execute(updateSql, params);
      updated += 1;
    }

    return { updated, skipped };
  }

  async matchCielo04AdjustmentItems(filters: {
    fileName: string;
    dryRun?: boolean;
  }): Promise<{
    total: number;
    ok: number;
    not_found: number;
    ambiguous: number;
    updated: number;
  }> {
    const items = await this.dbService.query<{
      ID: number;
      EC: string | null;
      NSU_CIELO: string | null;
      SALE_DATE: unknown;
      MATCH_STATUS: string | null;
      CIELO_SALE_ID: number | null;
    }>(
      'SELECT i.ID, i.EC, i.NSU_CIELO, i.SALE_DATE, i.MATCH_STATUS, i.CIELO_SALE_ID ' +
        'FROM T_EDI_CIELO_AJUSTES_ITENS i JOIN T_EDI_CIELO_AJUSTES a ON a.ID = i.AJUSTE_ID ' +
        'WHERE a.FILE_NAME = ?',
      [filters.fileName],
    );

    let ok = 0;
    let notFound = 0;
    let ambiguous = 0;
    let updated = 0;
    const dryRun = Boolean(filters.dryRun);

    for (const item of items) {
      const ec = item.EC?.trim() || '';
      const nsu = item.NSU_CIELO?.trim() || '';
      const saleDate = this.toFbDate(item.SALE_DATE ?? null);
      let matches: Array<{ ID: number }> = [];

      if (ec && nsu && saleDate) {
        matches = await this.dbService.query<{ ID: number }>(
          'SELECT ID FROM T_CIELO_SALES WHERE TRIM(ESTABLISHMENT_NO) = ? AND CAST(SALE_DATETIME AS DATE) = ? AND TRIM(NSU_DOC) = ?',
          [ec, saleDate, nsu],
        );
      }

      if (matches.length === 1) {
        ok += 1;
        const saleId = matches[0].ID;
        const alreadyOk = item.MATCH_STATUS === 'OK' && item.CIELO_SALE_ID === saleId;
        if (!dryRun && !alreadyOk) {
          await this.dbService.execute(
            'UPDATE T_EDI_CIELO_AJUSTES_ITENS SET CIELO_SALE_ID = ?, MATCH_STATUS = ?, MATCH_REASON = ?, MATCHED_AT = CAST(? AS TIMESTAMP) WHERE ID = ?',
            [saleId, 'OK', null, this.toFbTimestamp(new Date()), item.ID],
          );
          updated += 1;
        }
        continue;
      }

      if (matches.length === 0) {
        notFound += 1;
        const alreadyNotFound =
          item.MATCH_STATUS === 'NOT_FOUND' && item.CIELO_SALE_ID === null;
        if (!dryRun && !alreadyNotFound) {
          await this.dbService.execute(
            'UPDATE T_EDI_CIELO_AJUSTES_ITENS SET CIELO_SALE_ID = ?, MATCH_STATUS = ?, MATCH_REASON = ?, MATCHED_AT = CAST(? AS TIMESTAMP) WHERE ID = ?',
            [null, 'NOT_FOUND', 'no sale for EC+DATE+NSU', this.toFbTimestamp(new Date()), item.ID],
          );
          updated += 1;
        }
        continue;
      }

      ambiguous += 1;
      const alreadyAmbiguous =
        item.MATCH_STATUS === 'AMBIGUOUS' && item.CIELO_SALE_ID === null;
      if (!dryRun && !alreadyAmbiguous) {
        await this.dbService.execute(
          'UPDATE T_EDI_CIELO_AJUSTES_ITENS SET CIELO_SALE_ID = ?, MATCH_STATUS = ?, MATCH_REASON = ?, MATCHED_AT = CAST(? AS TIMESTAMP) WHERE ID = ?',
          [null, 'AMBIGUOUS', 'multiple sales for EC+DATE+NSU', this.toFbTimestamp(new Date()), item.ID],
        );
        updated += 1;
      }
    }

    const result = {
      total: items.length,
      ok,
      not_found: notFound,
      ambiguous,
      updated,
    };
    this.logger.log(
      `CIELO04 match file=${filters.fileName} total=${result.total} ok=${result.ok} notFound=${result.not_found} ambiguous=${result.ambiguous} updated=${result.updated} dryRun=${dryRun}`,
    );
    if (dryRun) {
      this.logger.warn(`CIELO04 match dryRun=true (nao persistido) file=${filters.fileName}`);
    }
    return result;
  }

  async applyCielo04AdjustmentItems(filters: {
    fileName: string;
    dryRun?: boolean;
  }): Promise<{
    totalMatchedOk: number;
    applied: number;
    skippedAlreadyApplied: number;
    errors: number;
  }> {
    const rows = await this.dbService.query<{
      ID: number;
      CIELO_SALE_ID: number | null;
      GROSS: number | null;
      FEE: number | null;
      NET: number | null;
    }>(
      'SELECT i.ID, i.CIELO_SALE_ID, i.GROSS, i.FEE, i.NET ' +
        'FROM T_EDI_CIELO_AJUSTES_ITENS i JOIN T_EDI_CIELO_AJUSTES a ON a.ID = i.AJUSTE_ID ' +
        'WHERE a.FILE_NAME = ? AND i.MATCH_STATUS = ? AND i.CIELO_SALE_ID IS NOT NULL',
      [filters.fileName, 'OK'],
    );

    const dryRun = Boolean(filters.dryRun);
    if (rows.length === 0) {
      const message = `CIELO04 apply sem itens OK: execute match dryRun=false antes de apply (file=${filters.fileName})`;
      this.logger.error(message);
      throw new Error(message);
    }
    const auditColumns = await this.getTableColumns('T_CIELO_AJUSTES_APPLIED');
    let applied = 0;
    let skippedAlreadyApplied = 0;
    let errors = 0;

    const addAuditValue = (
      data: Record<string, unknown>,
      names: string[],
      value: unknown,
    ) => {
      for (const name of names) {
        if (auditColumns.has(name)) {
          data[name] = value;
          return;
        }
      }
    };

    for (const item of rows) {
      const existing = await this.dbService.query<{ exists: number }>(
        'SELECT 1 FROM T_CIELO_AJUSTES_APPLIED WHERE AJUSTE_ITEM_ID = ?',
        [item.ID],
      );
      if (existing.length > 0) {
        skippedAlreadyApplied += 1;
        continue;
      }

      if (dryRun) {
        applied += 1;
        continue;
      }

      try {
        await this.dbService.transaction(async (tx) => {
          const current = await this.dbService.queryTx<{
            GROSS_AMOUNT: number | null;
            FEE_AMOUNT: number | null;
            NET_AMOUNT: number | null;
          }>(
            tx,
            'SELECT GROSS_AMOUNT, FEE_AMOUNT, NET_AMOUNT FROM T_CIELO_SALES WHERE ID = ?',
            [item.CIELO_SALE_ID],
          );
          if (!current.length) {
            throw new Error(`CIELO_SALE_ID inexistente: ${item.CIELO_SALE_ID}`);
          }
          const before = current[0];

          await this.dbService.executeTx(
            tx,
            'UPDATE T_CIELO_SALES SET GROSS_AMOUNT = ?, FEE_AMOUNT = ?, NET_AMOUNT = ? WHERE ID = ?',
            [item.GROSS, item.FEE, item.NET, item.CIELO_SALE_ID],
          );

          const auditData: Record<string, unknown> = {};
          addAuditValue(auditData, ['AJUSTE_ITEM_ID'], item.ID);
          addAuditValue(auditData, ['CIELO_SALE_ID'], item.CIELO_SALE_ID);
          addAuditValue(auditData, ['FILE_NAME'], filters.fileName);
          addAuditValue(auditData, ['BEFORE_GROSS_AMOUNT', 'BEFORE_GROSS'], before.GROSS_AMOUNT);
          addAuditValue(auditData, ['BEFORE_FEE_AMOUNT', 'BEFORE_FEE'], before.FEE_AMOUNT);
          addAuditValue(auditData, ['BEFORE_NET_AMOUNT', 'BEFORE_NET'], before.NET_AMOUNT);
          addAuditValue(auditData, ['AFTER_GROSS_AMOUNT', 'AFTER_GROSS', 'GROSS_AMOUNT'], item.GROSS);
          addAuditValue(auditData, ['AFTER_FEE_AMOUNT', 'AFTER_FEE', 'FEE_AMOUNT'], item.FEE);
          addAuditValue(auditData, ['AFTER_NET_AMOUNT', 'AFTER_NET', 'NET_AMOUNT'], item.NET);
          addAuditValue(auditData, ['CREATED_AT', 'APPLIED_AT'], this.toFbTimestamp(new Date()));

          if (!auditData.AJUSTE_ITEM_ID) {
            throw new Error('T_CIELO_AJUSTES_APPLIED sem AJUSTE_ITEM_ID');
          }

          const columns = Object.keys(auditData);
          const values = columns.map((col) => auditData[col]);
          const insertSql =
            `INSERT INTO T_CIELO_AJUSTES_APPLIED (${columns.join(', ')}) VALUES (${columns.map(() => '?').join(', ')})`;
          await this.dbService.executeTx(tx, insertSql, values);
        });
        applied += 1;
      } catch (error) {
        errors += 1;
        const message = error instanceof Error ? error.message : 'Erro desconhecido';
        this.logger.error(`Falha ao aplicar ajuste CIELO04 (${filters.fileName}): ${message}`);
      }
    }

    const result = {
      totalMatchedOk: rows.length,
      applied,
      skippedAlreadyApplied,
      errors,
    };
    this.logger.log(
      `CIELO04 apply file=${filters.fileName} totalOk=${result.totalMatchedOk} applied=${result.applied} skipped=${result.skippedAlreadyApplied} errors=${result.errors} dryRun=${dryRun}`,
    );
    if (dryRun) {
      this.logger.warn(`CIELO04 apply dryRun=true (nao persistido) file=${filters.fileName}`);
    }
    return result;
  }

  private async recordFile(
    file: EdiFileInfo,
    sha256: string,
    type: EdiFileType,
    status: EdiFileStatus,
    errorMessage: string | null,
    lineCount: number | null,
  ) {
    if (this.dbControlEnabled === false) {
      return;
    }
    const updateSql =
      'UPDATE T_EDI_FILES SET EDI_TYPE = ?, FILE_NAME = ?, LINE_COUNT = ?, STATUS = ?, MESSAGE = ? WHERE PROVIDER = ? AND FILE_SHA256 = ?';
    const insertSql =
      'INSERT INTO T_EDI_FILES (PROVIDER, EDI_TYPE, FILE_NAME, FILE_SHA256, LINE_COUNT, STATUS, MESSAGE, CREATED_AT) VALUES (?, ?, ?, ?, ?, ?, ?, CAST(? AS TIMESTAMP))';
    const lineCountValue = typeof lineCount === 'number' ? lineCount : null;

    try {
      if (sha256) {
        const existing = await this.dbService.query<{ id: number }>(
          'SELECT ID FROM T_EDI_FILES WHERE PROVIDER = ? AND FILE_SHA256 = ?',
          ['CIELO', sha256],
        );
        if (existing.length > 0) {
          await this.dbService.execute(updateSql, [
            type,
            file.filename,
            lineCountValue,
            status,
            errorMessage,
            'CIELO',
            sha256,
          ]);
        } else {
          await this.dbService.execute(insertSql, [
            'CIELO',
            type,
            file.filename,
            sha256,
            lineCountValue,
            status,
            errorMessage,
            this.toFbTimestamp(new Date()),
          ]);
        }
      } else {
        await this.dbService.execute(insertSql, [
          'CIELO',
          type,
          file.filename,
          null,
          lineCountValue,
          status,
          errorMessage,
          this.toFbTimestamp(new Date()),
        ]);
      }
      this.dbControlEnabled = true;
    } catch (error) {
      if (this.isTableMissingError(error)) {
        this.dbControlEnabled = false;
        if (!this.dbControlWarned) {
          this.logger.warn('Tabela T_EDI_FILES ausente, usando fallback local');
          this.dbControlWarned = true;
        }
        return;
      }
      const message = error instanceof Error ? error.message : 'Erro desconhecido';
      this.logger.error(`Falha ao registrar T_EDI_FILES: ${message}`);
    }
  }

  private isValidFilename(filename: string): boolean {
    return /^CIELO(0(3|4)D|16D)_.*\.TXT$/i.test(filename);
  }

  private isTempFile(filename: string): boolean {
    const lower = filename.toLowerCase();
    return lower.startsWith('~') || lower.endsWith('.part') || lower.endsWith('.tmp');
  }

  private isTableMissingError(error: unknown): boolean {
    const message = error instanceof Error ? error.message : String(error);
    const code = (error as { code?: number }).code;
    return (
      code === -204 ||
      message.includes('Table unknown') ||
      (message.includes('T_EDI_FILES') && message.includes('-204'))
    );
  }

  private async hashFileSha256(filePath: string): Promise<string> {
    return new Promise((resolve, reject) => {
      const hash = createHash('sha256');
      const stream = createReadStream(filePath);
      stream.on('data', (chunk) => hash.update(chunk));
      stream.on('error', (error) => reject(error));
      stream.on('end', () => resolve(hash.digest('hex')));
    });
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
    } catch (moveError) {
      const moveMessage = moveError instanceof Error ? moveError.message : 'Erro ao mover';
      this.logger.error(`Falha ao mover para errorDir: ${moveMessage}`);
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
}
