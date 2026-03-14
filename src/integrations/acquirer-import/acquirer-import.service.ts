import { Injectable, Logger } from '@nestjs/common';
import { createHash } from 'crypto';
import { DbService } from '../../db/db.service';
import {
  hashRow,
  normalizeHeader,
  normalizeText,
  parseCsvLine,
  parseDateTime,
  parseDateTimeParts,
  parseMoneyFlexible,
  parsePercent,
} from './acquirer-import.utils';
import { canonizeCielo, canonizeSicredi, canonizeSipag } from '../../modules/reconciliation/canon/canonize';
import { appendImportLog, ImportLogStatus } from '../../common/import-log';
import { CanonDuplicateCleanupService } from '../../modules/reconciliation/canon-duplicate-cleanup.service';

type ImportResult = {
  inserted: number;
  duplicates: number;
  invalidRows: number;
  alreadyImported?: boolean;
  fileHash?: string;
  message?: string;
};

type SicrediEdiKind = 'S' | 'P' | 'R';

type UnifiedAcquirerSale = {
  acquirer: 'CIELO' | 'SIPAG' | 'SICREDI';
  id: number;
  saleDatetime: Date | string | null;
  grossAmount: number;
  mdrAmount: number | null;
  netAmount: number | null;
  authCode: string | null;
  nsu: string | null;
  terminal: string | null;
  pdv: string | null;
  brand: string | null;
  status: string | null;
  raw?: any;
};

@Injectable()
export class AcquirerImportService {
  private readonly logger = new Logger(AcquirerImportService.name);
  private readonly relationExistsCache = new Map<string, boolean>();
  private readonly sipagCanonColumnsCache = new Map<string, boolean>();

  constructor(
    private readonly dbService: DbService,
    private readonly canonDuplicateCleanupService: CanonDuplicateCleanupService,
  ) {}

  private buildStoredRowHash(baseHash: string, duplicateOrdinal: number): string {
    if (!duplicateOrdinal) {
      return baseHash;
    }
    return createHash('sha256').update(`${baseHash}:dup:${duplicateOrdinal}`).digest('hex');
  }

  private buildAlreadyImportedResult(hash: string): ImportResult {
    return {
      inserted: 0,
      duplicates: 0,
      invalidRows: 0,
      alreadyImported: true,
      fileHash: hash,
      message: `Arquivo ja importado anteriormente (hash=${hash})`,
    };
  }

  private buildProcessedStatus(duplicates: number): ImportLogStatus {
    return duplicates > 0 ? 'processed_with_duplicates' : 'processed';
  }

  private toDateOnlyString(date: Date): string {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
  }

  private updateDateWindow(
    currentMin: string | null,
    currentMax: string | null,
    date: Date | null,
  ): { min: string | null; max: string | null } {
    if (!date) {
      return { min: currentMin, max: currentMax };
    }
    const value = this.toDateOnlyString(date);
    return {
      min: currentMin && currentMin < value ? currentMin : value,
      max: currentMax && currentMax > value ? currentMax : value,
    };
  }

  async importCielo(buffer: Buffer, fileName = 'upload.csv'): Promise<ImportResult> {
    const sha256 = createHash('sha256').update(buffer).digest('hex');
    if (await this.isImportedFileHash(sha256)) {
      await appendImportLog({
        category: 'acquirer_import',
        provider: 'CIELO',
        operation: 'import_file',
        status: 'skipped_duplicate_file',
        fileName,
        details: { reason: 'hash_already_imported', hash: sha256 },
      });
      return this.buildAlreadyImportedResult(sha256);
    }
    const pickLines = (content: string) =>
      content.replace(/^\uFEFF/, '').split(/\r?\n/).filter((line) => line.trim());
    const findHeaderIndex = (rows: string[]) =>
      rows.findIndex((line) =>
        normalizeHeader(line).startsWith('data da venda hora da venda estabelecimento forma de pagamento'),
      );

    let lines = pickLines(buffer.toString('utf8'));
    let headerIndex = findHeaderIndex(lines);
    if (headerIndex < 0) {
      lines = pickLines(buffer.toString('latin1'));
      headerIndex = findHeaderIndex(lines);
    }
    if (headerIndex < 0) {
      await appendImportLog({
        category: 'acquirer_import',
        provider: 'CIELO',
        operation: 'import_file',
        status: 'unknown_format',
        fileName,
        details: { error: 'Cabecalho Cielo nao encontrado', hash: sha256 },
      });
      throw new Error('Cabecalho Cielo nao encontrado');
    }

    let inserted = 0;
    let duplicates = 0;
    let invalidRows = 0;
    let loggedSamples = 0;
    let dateFrom: string | null = null;
    let dateTo: string | null = null;
    for (let i = headerIndex + 1; i < lines.length; i += 1) {
      const row = parseCsvLine(lines[i]);
      if (row.length < 10) {
        continue;
      }
      const saleDate = parseDateTimeParts(row[0], row[1]);
      const establishment = normalizeText(row[2]);
      const paymentMethod = normalizeText(row[3]);
      const brand = normalizeText(row[4]);
      const gross = parseMoneyFlexible(row[5]);
      const fee = parseMoneyFlexible(row[6]);
      const net = parseMoneyFlexible(row[7]);
      const status = normalizeText(row[8]);
      const entryType = normalizeText(row[9]);
      const reason = normalizeText(row[10]);
      const entryDate = parseDateTime(row[11]);
      const settlementDate = parseDateTime(row[12]);
      const authCode = normalizeText(row[13]);
      const nsuDoc = normalizeText(row[14]);
      const saleCode = normalizeText(row[15]);
      const tid = normalizeText(row[16]);
      const cardOrigin = normalizeText(row[17]);
      const pixId = normalizeText(row[18]);
      const txId = normalizeText(row[19]);
      const pixPaymentId = normalizeText(row[20]);
      const cardNumber = normalizeText(row[21]);
      const orderNumber = normalizeText(row[22]);
      const invoiceNumber = normalizeText(row[23]);
      const batchNumber = normalizeText(row[24]);
      const channel = normalizeText(row[25]);
      const modality = normalizeText(row[26]);
      const captureType = normalizeText(row[27]);
      const machineNumber = normalizeText(row[28]);
      const totalFees = parsePercent(row[29]);
      const mdrRate = parsePercent(row[30]);
      const termRate = parsePercent(row[31]);
      const mdrAmount = parseMoneyFlexible(row[32]);
      const termAmount = parseMoneyFlexible(row[33]);
      const cashAmount = parseMoneyFlexible(row[34]);
      const changeAmount = parseMoneyFlexible(row[35]);
      const originValue = normalizeText(row[36]);
      const originDocument = normalizeText(row[37]);
      const originInstitution = normalizeText(row[38]);
      const destValue = normalizeText(row[39]);
      const destDocument = normalizeText(row[40]);
      const destInstitution = normalizeText(row[41]);

      if (!saleDate || gross === null || net === null) {
        invalidRows += 1;
        continue;
      }
      ({ min: dateFrom, max: dateTo } = this.updateDateWindow(dateFrom, dateTo, saleDate));

      const rowHash = hashRow([saleDate.toISOString(), saleCode, nsuDoc, gross, net, tid]);
      const exists = await this.dbService.query<{ ID: number }>(
        'SELECT FIRST 1 ID FROM T_CIELO_SALES WHERE ROW_HASH = ?',
        [rowHash],
      );
      let storedRowHash = rowHash;
      if (exists.length) {
        duplicates += 1;
        storedRowHash = this.buildStoredRowHash(rowHash, duplicates);
      }

      const canon = canonizeCielo({
        SALE_DATETIME: saleDate,
        GROSS_AMOUNT: gross,
        FEE_AMOUNT: fee,
        NET_AMOUNT: net,
        MACHINE_NUMBER: machineNumber,
        E_LOGICAL_TERMINAL_NO: null,
        AUTH_CODE: authCode,
        NSU_DOC: nsuDoc,
        E_INSTALLMENT_TOTAL: null,
        E_INSTALLMENT_NO: null,
        PAYMENT_METHOD: paymentMethod,
        ENTRY_TYPE: entryType,
        BRAND: brand,
      });
      if (process.env.DEBUG === 'true' && loggedSamples < 3) {
        this.logger.debug(
          {
            rawPaymentMethod: paymentMethod,
            rawBrand: brand,
            canon,
          },
          'Cielo import canon sample',
        );
        loggedSamples += 1;
      }

      await this.dbService.execute(
        'INSERT INTO T_CIELO_SALES (SALE_DATETIME, ESTABLISHMENT_NO, PAYMENT_METHOD, BRAND, GROSS_AMOUNT, FEE_AMOUNT, NET_AMOUNT, STATUS, ENTRY_TYPE, REASON, ENTRY_DATE, SETTLEMENT_DATE, AUTH_CODE, NSU_DOC, SALE_CODE, TID, CARD_ORIGIN, PIX_ID, TX_ID, PIX_PAYMENT_ID, CARD_NUMBER, ORDER_NUMBER, INVOICE_NUMBER, BATCH_NUMBER, SALES_CHANNEL, MODALITY, CAPTURE_TYPE, MACHINE_NUMBER, TOTAL_FEES, MDR_RATE, TERM_RATE, MDR_AMOUNT, TERM_AMOUNT, CASH_AMOUNT, CHANGE_AMOUNT, ORIGIN_VALUE, ORIGIN_DOCUMENT, ORIGIN_INSTITUTION, DEST_VALUE, DEST_DOCUMENT, DEST_INSTITUTION, ROW_HASH, CREATED_AT, CANON_SALE_DATE, CANON_METHOD, CANON_METHOD_GROUP, CANON_BRAND, CANON_TERMINAL_NO, CANON_AUTH_CODE, CANON_NSU, CANON_GROSS_AMOUNT, CANON_FEE_AMOUNT, CANON_NET_AMOUNT, CANON_PERC_TAXA, CANON_INSTALLMENT_NO, CANON_INSTALLMENT_TOTAL) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        [
          saleDate,
          establishment || null,
          paymentMethod,
          brand,
          gross,
          fee,
          net,
          status || null,
          entryType || null,
          reason || null,
          entryDate,
          settlementDate,
          authCode || null,
          nsuDoc || null,
          saleCode || null,
          tid || null,
          cardOrigin || null,
          pixId || null,
          txId || null,
          pixPaymentId || null,
          cardNumber || null,
          orderNumber || null,
          invoiceNumber || null,
          batchNumber || null,
          channel || null,
          modality || null,
          captureType || null,
          machineNumber || null,
          totalFees,
          mdrRate,
          termRate,
          mdrAmount,
          termAmount,
          cashAmount,
          changeAmount,
          originValue || null,
          originDocument || null,
          originInstitution || null,
          destValue || null,
          destDocument || null,
          destInstitution || null,
          storedRowHash,
          new Date(),
          canon.CANON_SALE_DATE,
          canon.CANON_METHOD,
          canon.CANON_METHOD_GROUP,
          canon.CANON_BRAND,
          canon.CANON_TERMINAL_NO,
          canon.CANON_AUTH_CODE,
          canon.CANON_NSU,
          canon.CANON_GROSS_AMOUNT,
          canon.CANON_FEE_AMOUNT,
          canon.CANON_NET_AMOUNT,
          canon.CANON_PERC_TAXA,
          canon.CANON_INSTALLMENT_NO,
          canon.CANON_INSTALLMENT_TOTAL,
        ],
      );
      inserted += 1;
    }

    await this.recordImportedFile(fileName, sha256, 'CIELO');
    await appendImportLog({
      category: 'acquirer_import',
      provider: 'CIELO',
      operation: 'import_file',
      status: this.buildProcessedStatus(duplicates),
      fileName,
      details: { inserted, duplicates, invalidRows, hash: sha256 },
    });
    if (dateFrom && dateTo) {
      await this.canonDuplicateCleanupService.cleanupAfterImport('CIELO', dateFrom, dateTo);
    }
    this.logger.log(`Cielo import concluido: inseridas=${inserted} duplicadas=${duplicates}`);
    return { inserted, duplicates, invalidRows };
  }

  async importSipag(buffer: Buffer, fileName = 'upload.csv'): Promise<ImportResult> {
    const sha256 = createHash('sha256').update(buffer).digest('hex');
    if (await this.isImportedFileHash(sha256)) {
      await appendImportLog({
        category: 'acquirer_import',
        provider: 'SIPAG',
        operation: 'import_file',
        status: 'skipped_duplicate_file',
        fileName,
        details: { reason: 'hash_already_imported', hash: sha256 },
      });
      return this.buildAlreadyImportedResult(sha256);
    }
    const pickLines = (content: string) =>
      content.replace(/^\uFEFF/, '').split(/\r?\n/).filter((line) => line.trim());
    const findHeaderIndex = (rows: string[]) =>
      rows.findIndex((line) =>
        normalizeHeader(line).startsWith('n do estabelecimento data da transacao') ||
        normalizeHeader(line).startsWith('cod cliente data da autorizacao'),
      );

    let lines = pickLines(buffer.toString('utf8'));
    let headerIndex = findHeaderIndex(lines);
    if (headerIndex < 0) {
      lines = pickLines(buffer.toString('latin1'));
      headerIndex = findHeaderIndex(lines);
    }
    if (headerIndex < 0) {
      const ediResult = await this.tryImportSipagEdi(lines, fileName, sha256);
      if (ediResult) {
        return ediResult;
      }
      await appendImportLog({
        category: 'acquirer_import',
        provider: 'SIPAG',
        operation: 'import_file',
        status: 'unknown_format',
        fileName,
        details: { error: 'Cabecalho Sipag nao encontrado', hash: sha256 },
      });
      throw new Error('Cabecalho Sipag nao encontrado');
    }

    let inserted = 0;
    let duplicates = 0;
    let invalidRows = 0;
    let dateFrom: string | null = null;
    let dateTo: string | null = null;
    const header = parseCsvLine(lines[headerIndex]);
    const normalizedHeader = header.map((cell) => normalizeHeader(cell));
    const findIndex = (candidates: string[]) =>
      normalizedHeader.findIndex((value) => candidates.some((candidate) => value === candidate));
    const findFirstIndex = (candidate: string) => normalizedHeader.findIndex((value) => value === candidate);
    const findLastIndex = (candidate: string) =>
      [...normalizedHeader].reverse().findIndex((value) => value === candidate) >= 0
        ? normalizedHeader.length -
          1 -
          [...normalizedHeader].reverse().findIndex((value) => value === candidate)
        : -1;
    const hasNewLayout = normalizedHeader.some((value) => value === 'cod cliente');

    for (let i = headerIndex + 1; i < lines.length; i += 1) {
      const row = parseCsvLine(lines[i]);
      if (row.length < header.length) {
        continue;
      }

      let establishment = '';
      let saleDateTime: Date | null = null;
      let transactionNo = '';
      let saleId = '';
      let brand = '';
      let paymentMethod = '';
      let plan = '';
      let installmentNo: number | null = null;
      let installmentTotal: number | null = null;
      let authNo = '';
      let cardType = '';
      let cardNumber = '';
      let terminalNo = '';
      let captureType = '';
      let creditDebit = '';
      let cancelIndicator = '';
      let summaryNo = '';
      let settlementDate: Date | null = null;
      let yourNumber = '';
      let paymentOrderNo = '';
      let status = '';
      let gross: number | null = null;
      let fee: number | null = null;
      let net: number | null = null;
      let planTotal: number | null = null;

      if (hasNewLayout) {
        const idxEst = findIndex(['cod cliente', 'estabelecimento', 'estabelecimento(s)']);
        const idxDate = findIndex(['data da autorizacao', 'data autorizacao', 'data da transacao']);
        const idxStatus = findIndex(['status']);
        const idxAuth = findIndex(['autorizacao', 'autorizacao no', 'autorizacao numero']);
        const idxType = findIndex(['tipo transacao', 'tipo transacaoo', 'tipo transacao']);
        const idxCard = findIndex(['n do cartao', 'numero do cartao', 'nº do cartao']);
        const idxDoc = findIndex(['doc']);
        const idxVoucherFirst = findFirstIndex('comprovante');
        const idxVoucherLast = findLastIndex('comprovante');
        const idxValue = findIndex(['valor']);

        establishment = normalizeText(row[idxEst >= 0 ? idxEst : 0]);
        saleDateTime = parseDateTime(row[idxDate >= 0 ? idxDate : 1]);
        status = this.normalizeSipagStatus(row[idxStatus >= 0 ? idxStatus : 2]) ?? '';
        authNo = normalizeText(row[idxAuth >= 0 ? idxAuth : 3]);
        paymentMethod = normalizeText(row[idxType >= 0 ? idxType : 4]);
        terminalNo = normalizeText(row[idxVoucherFirst >= 0 ? idxVoucherFirst : 5]);
        cardNumber = normalizeText(row[idxCard >= 0 ? idxCard : 6]);
        yourNumber = normalizeText(row[idxDoc >= 0 ? idxDoc : 7]);
        transactionNo = normalizeText(row[idxVoucherLast >= 0 ? idxVoucherLast : 8]);
        gross = parseMoneyFlexible(row[idxValue >= 0 ? idxValue : row.length - 1]);
      } else {
        establishment = normalizeText(row[0]);
        saleDateTime = parseDateTime(row[1]);
        transactionNo = normalizeText(row[2]);
        saleId = normalizeText(row[3]);
        brand = normalizeText(row[4]);
        paymentMethod = normalizeText(row[5]);
        plan = normalizeText(row[6]);
        installmentNo = Number(row[7]);
        installmentTotal = Number(row[8]);
        authNo = normalizeText(row[9]);
        cardType = normalizeText(row[10]);
        cardNumber = normalizeText(row[11]);
        terminalNo = normalizeText(row[12]);
        captureType = normalizeText(row[13]);
        creditDebit = normalizeText(row[14]);
        cancelIndicator = normalizeText(row[15]);
        summaryNo = normalizeText(row[16]);
        settlementDate = parseDateTime(row[17]);
        yourNumber = normalizeText(row[18]);
        paymentOrderNo = normalizeText(row[19]);
        status = this.normalizeSipagStatus(row[20]) ?? '';
        gross = parseMoneyFlexible(row[21]);
        fee = parseMoneyFlexible(row[22]);
        net = parseMoneyFlexible(row[23]);
        planTotal = parseMoneyFlexible(row[24]);
      }

      if (!saleDateTime || gross === null) {
        invalidRows += 1;
        continue;
      }
      ({ min: dateFrom, max: dateTo } = this.updateDateWindow(dateFrom, dateTo, saleDateTime));

      const rowHash = hashRow([
        establishment,
        saleDateTime.toISOString(),
        transactionNo,
        saleId,
        gross,
        net ?? '',
      ]);

      const exists = await this.dbService.query<{ ID: number }>(
        'SELECT FIRST 1 ID FROM T_SIPAG_SALES WHERE ROW_HASH = ?',
        [rowHash],
      );
      let storedRowHash = rowHash;
      if (exists.length) {
        duplicates += 1;
        storedRowHash = this.buildStoredRowHash(rowHash, duplicates);
      }

      const normalizedInstallmentNo =
        installmentNo !== null && !Number.isNaN(installmentNo) ? installmentNo : null;
      const normalizedInstallmentTotal =
        installmentTotal !== null && !Number.isNaN(installmentTotal) ? installmentTotal : null;
      const canon = canonizeSipag({
        SALE_DATETIME: saleDateTime,
        GROSS_AMOUNT: gross,
        FEE_AMOUNT: fee,
        NET_AMOUNT: net,
        PAYMENT_METHOD: paymentMethod,
        CREDIT_DEBIT_IND: creditDebit,
        PLAN_DESC: plan,
        CARD_TYPE: cardType,
        BRAND: brand,
        TERMINAL_NO: terminalNo || null,
        AUTH_NO: authNo || null,
        TRANSACTION_NO: transactionNo || null,
        SALE_ID: saleId || null,
        INSTALLMENT_NO: normalizedInstallmentNo,
        INSTALLMENT_TOTAL: normalizedInstallmentTotal,
      });
      await this.insertSipagSale({
        establishmentNo: establishment,
        saleDateTime,
        transactionNo,
        saleId,
        brand,
        paymentMethod,
        plan,
        installmentNo: normalizedInstallmentNo,
        installmentTotal: normalizedInstallmentTotal,
        authNo: authNo || null,
        cardType: cardType || null,
        cardNumber: cardNumber || null,
        terminalNo: terminalNo || null,
        captureType: captureType || null,
        creditDebit: creditDebit || null,
        cancelIndicator: cancelIndicator || null,
        summaryNo: summaryNo || null,
        settlementDate,
        yourNumber: yourNumber || null,
        paymentOrderNo: paymentOrderNo || null,
        status: status || null,
        gross,
        fee,
        net,
        planTotal,
        storedRowHash,
        canon,
      });
      inserted += 1;
    }

    await this.recordImportedFile(fileName, sha256, 'SIPAG');
    await appendImportLog({
      category: 'acquirer_import',
      provider: 'SIPAG',
      operation: 'import_file',
      status: this.buildProcessedStatus(duplicates),
      fileName,
      details: { inserted, duplicates, invalidRows, hash: sha256 },
    });
    if (dateFrom && dateTo) {
      await this.canonDuplicateCleanupService.cleanupAfterImport('SIPAG', dateFrom, dateTo);
    }
    this.logger.log(`Sipag import concluido: inseridas=${inserted} duplicadas=${duplicates}`);
    return { inserted, duplicates, invalidRows };
  }

  private async tryImportSipagEdi(
    lines: string[],
    fileName: string,
    sha256: string,
  ): Promise<ImportResult | null> {
    if (!lines.length) {
      return null;
    }

    const first = parseCsvLine(lines[0], ',');
    if (normalizeText(first[0]) !== '000' || normalizeHeader(first[2]) !== 'sipag') {
      return null;
    }

    let inserted = 0;
    let duplicates = 0;
    let invalidRows = 0;
    let paymentsInserted = 0;
    let dateFrom: string | null = null;
    let dateTo: string | null = null;
    let fallbackStatus = '';

    for (const line of lines) {
      const row = parseCsvLine(line, ',');
      if (!row.length) {
        continue;
      }
      const recordType = normalizeText(row[0]);
      if (recordType === '010' || recordType === '020') {
        fallbackStatus = this.normalizeSipagStatus(row[8]) ?? '';
        continue;
      }

      if (recordType === '021' || recordType === '023' || recordType === '025') {
        const payment = this.parseSipagEdiPayment(row, recordType, fallbackStatus);
        if (payment) {
          const insertedPayment = await this.insertSipagPaymentIfAvailable(payment, line, fileName);
          if (insertedPayment) {
            paymentsInserted += 1;
          }
        }
      }

      if (recordType !== '001' && recordType !== '011' && recordType !== '013') {
        continue;
      }

      const parsed =
        recordType === '001'
          ? this.parseSipagEdi001(row, fallbackStatus)
          : this.parseSipagEdi011(row, fallbackStatus, recordType as '011' | '013');

      if (!parsed.saleDateTime || parsed.gross === null) {
        invalidRows += 1;
        continue;
      }
      ({ min: dateFrom, max: dateTo } = this.updateDateWindow(dateFrom, dateTo, parsed.saleDateTime));

      const rowHash = hashRow([
        parsed.establishment,
        parsed.saleDateTime.toISOString(),
        parsed.transactionNo,
        parsed.saleId,
        parsed.gross,
        parsed.net ?? '',
      ]);

      const exists = await this.dbService.query<{ ID: number }>(
        'SELECT FIRST 1 ID FROM T_SIPAG_SALES WHERE ROW_HASH = ?',
        [rowHash],
      );
      let storedRowHash = rowHash;
      if (exists.length) {
        duplicates += 1;
        storedRowHash = this.buildStoredRowHash(rowHash, duplicates);
      }

      const normalizedInstallmentNo =
        parsed.installmentNo !== null && !Number.isNaN(parsed.installmentNo)
          ? parsed.installmentNo
          : null;
      const normalizedInstallmentTotal =
        parsed.installmentTotal !== null && !Number.isNaN(parsed.installmentTotal)
          ? parsed.installmentTotal
          : null;
      const canon = canonizeSipag({
        SALE_DATETIME: parsed.saleDateTime,
        GROSS_AMOUNT: parsed.gross,
        FEE_AMOUNT: parsed.fee,
        NET_AMOUNT: parsed.net,
        PAYMENT_METHOD: parsed.paymentMethod,
        CREDIT_DEBIT_IND: parsed.creditDebit,
        PLAN_DESC: parsed.plan,
        CARD_TYPE: parsed.cardType,
        BRAND: parsed.brand,
        TERMINAL_NO: parsed.terminalNo,
        AUTH_NO: parsed.authNo,
        TRANSACTION_NO: parsed.transactionNo,
        SALE_ID: parsed.saleId,
        INSTALLMENT_NO: normalizedInstallmentNo,
        INSTALLMENT_TOTAL: normalizedInstallmentTotal,
      });
      await this.insertSipagSale({
        establishmentNo: parsed.establishment || null,
        saleDateTime: parsed.saleDateTime,
        transactionNo: parsed.transactionNo || null,
        saleId: parsed.saleId || null,
        brand: parsed.brand || null,
        paymentMethod: parsed.paymentMethod || null,
        plan: parsed.plan || null,
        installmentNo: normalizedInstallmentNo,
        installmentTotal: normalizedInstallmentTotal,
        authNo: parsed.authNo || null,
        cardType: parsed.cardType || null,
        cardNumber: parsed.cardNumber || null,
        terminalNo: parsed.terminalNo || null,
        captureType: parsed.captureType || null,
        creditDebit: parsed.creditDebit || null,
        cancelIndicator: parsed.cancelIndicator || null,
        summaryNo: parsed.summaryNo || null,
        settlementDate: parsed.settlementDate,
        yourNumber: parsed.yourNumber || null,
        paymentOrderNo: parsed.paymentOrderNo || null,
        status: parsed.status || null,
        gross: parsed.gross,
        fee: parsed.fee,
        net: parsed.net,
        planTotal: parsed.planTotal,
        storedRowHash,
        canon,
      });
      inserted += 1;
    }

    if (inserted === 0 && duplicates === 0 && invalidRows === 0 && paymentsInserted === 0) {
      return null;
    }

    await this.recordImportedFile(fileName, sha256, 'SIPAG');
    await appendImportLog({
      category: 'acquirer_import',
      provider: 'SIPAG',
      operation: 'import_file',
      status: this.buildProcessedStatus(duplicates),
      fileName,
      details: { layout: 'EDI', inserted, duplicates, invalidRows, paymentsInserted, hash: sha256 },
    });
    if (dateFrom && dateTo) {
      await this.canonDuplicateCleanupService.cleanupAfterImport('SIPAG', dateFrom, dateTo);
    }
    this.logger.log(
      `Sipag import EDI concluido: inseridas=${inserted} duplicadas=${duplicates} invalidas=${invalidRows} pagamentos=${paymentsInserted}`,
    );
    return { inserted, duplicates, invalidRows };
  }

  private parseSipagEdi011(
    row: string[],
    fallbackStatus: string,
    recordType: '011' | '013',
  ) {
    const brand = normalizeText(row[5]);
    const paymentMethod = normalizeText(row[6]);
    const cardType = normalizeText(row[16]);
    const creditDebit = this.resolveSipagEdiCreditDebit(recordType, brand, paymentMethod, cardType);
    return {
      establishment: normalizeText(row[1]),
      saleDateTime: this.parseSipagEdiDateTime(row[2], row[11]),
      transactionNo: normalizeText(row[4]),
      saleId: normalizeText(row[35] ?? row[4]),
      brand,
      paymentMethod,
      plan: null as string | null,
      installmentNo: null as number | null,
      installmentTotal: this.parseSipagEdiInteger(row[15]),
      authNo: normalizeText(row[10]),
      cardType,
      cardNumber: normalizeText(row[7]),
      terminalNo: normalizeText(row[12]),
      captureType: normalizeText(row[13]),
      creditDebit,
      cancelIndicator: null as string | null,
      summaryNo: normalizeText(row[3]),
      settlementDate: this.parseSipagEdiDateTime(row[21]),
      yourNumber: normalizeText(row[37] ?? row[8]),
      paymentOrderNo: normalizeText(row[3]),
      status: this.normalizeSipagStatus(fallbackStatus) || null,
      gross: this.parseSipagEdiCents(row[17]),
      fee: this.parseSipagEdiCents(row[18]),
      net: this.parseSipagEdiCents(row[20]),
      planTotal: null as number | null,
    };
  }

  private parseSipagEdi001(row: string[], fallbackStatus: string) {
    return {
      establishment: normalizeText(row[4] || row[1]),
      saleDateTime: this.parseSipagEdiDateTime(row[8], row[9]),
      transactionNo: normalizeText(row[20] || row[5]),
      saleId: normalizeText(row[21] || row[5]),
      brand: normalizeText(row[13] || 'PIX'),
      paymentMethod: normalizeText(row[13] || 'PIX'),
      plan: null as string | null,
      installmentNo: 1,
      installmentTotal: 1,
      authNo: normalizeText(row[14]),
      cardType: 'PIX',
      cardNumber: null as string | null,
      terminalNo: normalizeText(row[22]),
      captureType: null as string | null,
      creditDebit: 'PIX',
      cancelIndicator: null as string | null,
      summaryNo: normalizeText(row[1]),
      settlementDate: this.parseSipagEdiDateTime(row[10], row[11]),
      yourNumber: normalizeText(row[6] || row[20]),
      paymentOrderNo: normalizeText(row[3]),
      status: this.normalizeSipagStatus(row[7]) || this.normalizeSipagStatus(fallbackStatus) || null,
      gross: this.parseSipagEdiCents(row[12]),
      fee: null as number | null,
      net: this.parseSipagEdiCents(row[12]),
      planTotal: null as number | null,
    };
  }

  private resolveSipagEdiCreditDebit(
    recordType: '011' | '013',
    brand: string,
    paymentMethod: string,
    cardType: string,
  ): 'PIX' | 'DEBIT' | 'CREDIT' {
    const normalized = [brand, paymentMethod, cardType]
      .map((value) =>
        normalizeText(value)
          .normalize('NFD')
          .replace(/[\u0300-\u036f]/g, '')
          .toUpperCase(),
      )
      .join(' ');

    if (normalized.includes('PIX')) {
      return 'PIX';
    }
    if (normalized.includes('DEBIT')) {
      return 'DEBIT';
    }
    if (normalized.includes('CRED')) {
      return 'CREDIT';
    }
    return recordType === '011' ? 'DEBIT' : 'CREDIT';
  }

  private parseSipagEdiPayment(
    row: string[],
    recordType: '021' | '023' | '025',
    fallbackStatus: string,
  ) {
    const paymentDate = this.parseSipagEdiDateTime(row[2] || row[24] || row[3]);
    const saleDateTime = this.parseSipagEdiDateTime(row[20], row[21]);
    const gross = this.parseSipagEdiCents(row[31]);
    const fee = this.parseSipagEdiCents(row[32]);
    const net = this.parseSipagEdiCents(row[33]);
    return {
      recordType,
      establishmentNo: normalizeText(row[3]),
      salesSummaryNo: normalizeText(row[6]),
      salesReceiptNo: normalizeText(row[29] || row[18]),
      authCode: normalizeText(row[28]),
      idUr: normalizeText(row[38]),
      paymentDate,
      saleDateTime,
      brand: normalizeText(row[1]),
      paymentMethod: normalizeText(row[30]),
      paymentStatus: this.normalizeSipagStatus(row[16]) || this.normalizeSipagStatus(fallbackStatus) || null,
      grossAmount: gross,
      feeAmount: fee,
      netAmount: net,
      planTotal: this.parseSipagEdiCents(row[35]),
    };
  }

  private async insertSipagPaymentIfAvailable(
    payment: {
      recordType: string;
      establishmentNo: string;
      salesSummaryNo: string;
      salesReceiptNo: string;
      authCode: string;
      idUr: string;
      paymentDate: Date | null;
      saleDateTime: Date | null;
      brand: string;
      paymentMethod: string;
      paymentStatus: string | null;
      grossAmount: number | null;
      feeAmount: number | null;
      netAmount: number | null;
      planTotal: number | null;
    },
    rawLine: string,
    fileName: string,
  ): Promise<boolean> {
    if (!(await this.relationExists('T_SIPAG_PAYMENTS'))) {
      return false;
    }

    const rowHash = hashRow([
      payment.recordType,
      payment.establishmentNo,
      payment.salesSummaryNo,
      payment.salesReceiptNo,
      payment.authCode,
      payment.idUr,
      payment.paymentDate ? payment.paymentDate.toISOString() : '',
      payment.grossAmount ?? '',
      payment.netAmount ?? '',
    ]);

    const exists = await this.dbService.query<{ ID: number }>(
      'SELECT FIRST 1 ID FROM T_SIPAG_PAYMENTS WHERE ROW_HASH = ?',
      [rowHash],
    );
    if (exists.length) {
      return false;
    }

    await this.dbService.execute(
      'INSERT INTO T_SIPAG_PAYMENTS (SOURCE_FILE, RECORD_TYPE, ESTABLISHMENT_NO, SALES_SUMMARY_NO, SALES_RECEIPT_NO, AUTH_CODE, ID_UR, PAYMENT_DATE, SALE_DATETIME, BRAND, PAYMENT_METHOD, PAYMENT_STATUS, GROSS_AMOUNT, FEE_AMOUNT, NET_AMOUNT, PLAN_TOTAL, ROW_HASH, RAW_LINE, CREATED_AT) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
      [
        fileName,
        payment.recordType,
        payment.establishmentNo || null,
        payment.salesSummaryNo || null,
        payment.salesReceiptNo || null,
        payment.authCode || null,
        payment.idUr || null,
        payment.paymentDate ? new Date(payment.paymentDate.getFullYear(), payment.paymentDate.getMonth(), payment.paymentDate.getDate()) : null,
        payment.saleDateTime,
        payment.brand || null,
        payment.paymentMethod || null,
        payment.paymentStatus || null,
        payment.grossAmount,
        payment.feeAmount,
        payment.netAmount,
        payment.planTotal,
        rowHash,
        rawLine,
        new Date(),
      ],
    );
    return true;
  }

  private async insertSipagSale(input: {
    establishmentNo: string | null;
    saleDateTime: Date;
    transactionNo: string | null;
    saleId: string | null;
    brand: string | null;
    paymentMethod: string | null;
    plan: string | null;
    installmentNo: number | null;
    installmentTotal: number | null;
    authNo: string | null;
    cardType: string | null;
    cardNumber: string | null;
    terminalNo: string | null;
    captureType: string | null;
    creditDebit: string | null;
    cancelIndicator: string | null;
    summaryNo: string | null;
    settlementDate: Date | null;
    yourNumber: string | null;
    paymentOrderNo: string | null;
    status: string | null;
    gross: number;
    fee: number | null;
    net: number | null;
    planTotal: number | null;
    storedRowHash: string;
    canon: {
      CANON_SALE_DATE: string | null;
      CANON_METHOD: string | null;
      CANON_METHOD_GROUP: string | null;
      CANON_BRAND: string | null;
      CANON_TERMINAL_NO: string | null;
      CANON_AUTH_CODE: string | null;
      CANON_NSU: string | null;
      CANON_GROSS_AMOUNT: number | null;
      CANON_FEE_AMOUNT: number | null;
      CANON_NET_AMOUNT: number | null;
      CANON_PERC_TAXA: number | null;
      CANON_INSTALLMENT_NO: number | null;
      CANON_INSTALLMENT_TOTAL: number | null;
    };
  }): Promise<void> {
    const fit = (value: string | null, max: number): string | null => {
      if (!value) {
        return null;
      }
      const text = String(value).trim();
      return text.length > max ? text.slice(0, max) : text;
    };

    const canonSafe = {
      CANON_SALE_DATE: input.canon.CANON_SALE_DATE,
      CANON_METHOD: fit(input.canon.CANON_METHOD, 16),
      CANON_METHOD_GROUP: fit(input.canon.CANON_METHOD_GROUP, 8),
      CANON_BRAND: fit(input.canon.CANON_BRAND, 32),
      CANON_TERMINAL_NO: fit(input.canon.CANON_TERMINAL_NO, 32),
      CANON_AUTH_CODE: fit(input.canon.CANON_AUTH_CODE, 32),
      CANON_NSU: fit(input.canon.CANON_NSU, 32),
      CANON_GROSS_AMOUNT: input.canon.CANON_GROSS_AMOUNT,
      CANON_FEE_AMOUNT: input.canon.CANON_FEE_AMOUNT,
      CANON_NET_AMOUNT: input.canon.CANON_NET_AMOUNT,
      CANON_PERC_TAXA: input.canon.CANON_PERC_TAXA,
      CANON_INSTALLMENT_NO: input.canon.CANON_INSTALLMENT_NO,
      CANON_INSTALLMENT_TOTAL: input.canon.CANON_INSTALLMENT_TOTAL,
    };

    const baseValues = [
      input.establishmentNo,
      input.saleDateTime,
      input.transactionNo,
      input.saleId,
      input.brand,
      input.paymentMethod,
      input.plan,
      input.installmentNo,
      input.installmentTotal,
      input.authNo,
      input.cardType,
      input.cardNumber,
      input.terminalNo,
      input.captureType,
      input.creditDebit,
      input.cancelIndicator,
      input.summaryNo,
      input.settlementDate,
      input.yourNumber,
      input.paymentOrderNo,
      input.status,
      input.gross,
      input.fee,
      input.net,
      input.planTotal,
      input.storedRowHash,
      new Date(),
    ];

    const hasBaseCanon = await this.hasSipagCanonColumns();
    if (hasBaseCanon && (await this.hasSipagCanonFinancialColumns())) {
      await this.dbService.execute(
        'INSERT INTO T_SIPAG_SALES (ESTABLISHMENT_NO, SALE_DATETIME, TRANSACTION_NO, SALE_ID, BRAND, PAYMENT_METHOD, PLAN_DESC, INSTALLMENT_NO, INSTALLMENT_TOTAL, AUTH_NO, CARD_TYPE, CARD_NUMBER, TERMINAL_NO, CAPTURE_TYPE, CREDIT_DEBIT_IND, CANCEL_IND, SUMMARY_NO, SETTLEMENT_DATE, YOUR_NUMBER, PAYMENT_ORDER_NO, STATUS, GROSS_AMOUNT, FEE_AMOUNT, NET_AMOUNT, PLAN_TOTAL, ROW_HASH, CREATED_AT, CANON_SALE_DATE, CANON_METHOD, CANON_METHOD_GROUP, CANON_BRAND, CANON_TERMINAL_NO, CANON_AUTH_CODE, CANON_NSU, CANON_GROSS_AMOUNT, CANON_FEE_AMOUNT, CANON_NET_AMOUNT, CANON_PERC_TAXA, CANON_INSTALLMENT_NO, CANON_INSTALLMENT_TOTAL) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        [
          ...baseValues,
          canonSafe.CANON_SALE_DATE,
          canonSafe.CANON_METHOD,
          canonSafe.CANON_METHOD_GROUP,
          canonSafe.CANON_BRAND,
          canonSafe.CANON_TERMINAL_NO,
          canonSafe.CANON_AUTH_CODE,
          canonSafe.CANON_NSU,
          canonSafe.CANON_GROSS_AMOUNT,
          canonSafe.CANON_FEE_AMOUNT,
          canonSafe.CANON_NET_AMOUNT,
          canonSafe.CANON_PERC_TAXA,
          canonSafe.CANON_INSTALLMENT_NO,
          canonSafe.CANON_INSTALLMENT_TOTAL,
        ],
      );
      return;
    }

    if (hasBaseCanon) {
      await this.dbService.execute(
        'INSERT INTO T_SIPAG_SALES (ESTABLISHMENT_NO, SALE_DATETIME, TRANSACTION_NO, SALE_ID, BRAND, PAYMENT_METHOD, PLAN_DESC, INSTALLMENT_NO, INSTALLMENT_TOTAL, AUTH_NO, CARD_TYPE, CARD_NUMBER, TERMINAL_NO, CAPTURE_TYPE, CREDIT_DEBIT_IND, CANCEL_IND, SUMMARY_NO, SETTLEMENT_DATE, YOUR_NUMBER, PAYMENT_ORDER_NO, STATUS, GROSS_AMOUNT, FEE_AMOUNT, NET_AMOUNT, PLAN_TOTAL, ROW_HASH, CREATED_AT, CANON_SALE_DATE, CANON_METHOD, CANON_METHOD_GROUP, CANON_BRAND, CANON_TERMINAL_NO, CANON_AUTH_CODE, CANON_NSU, CANON_GROSS_AMOUNT, CANON_INSTALLMENT_NO, CANON_INSTALLMENT_TOTAL) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        [
          ...baseValues,
          canonSafe.CANON_SALE_DATE,
          canonSafe.CANON_METHOD,
          canonSafe.CANON_METHOD_GROUP,
          canonSafe.CANON_BRAND,
          canonSafe.CANON_TERMINAL_NO,
          canonSafe.CANON_AUTH_CODE,
          canonSafe.CANON_NSU,
          canonSafe.CANON_GROSS_AMOUNT,
          canonSafe.CANON_INSTALLMENT_NO,
          canonSafe.CANON_INSTALLMENT_TOTAL,
        ],
      );
      return;
    }

    await this.dbService.execute(
      'INSERT INTO T_SIPAG_SALES (ESTABLISHMENT_NO, SALE_DATETIME, TRANSACTION_NO, SALE_ID, BRAND, PAYMENT_METHOD, PLAN_DESC, INSTALLMENT_NO, INSTALLMENT_TOTAL, AUTH_NO, CARD_TYPE, CARD_NUMBER, TERMINAL_NO, CAPTURE_TYPE, CREDIT_DEBIT_IND, CANCEL_IND, SUMMARY_NO, SETTLEMENT_DATE, YOUR_NUMBER, PAYMENT_ORDER_NO, STATUS, GROSS_AMOUNT, FEE_AMOUNT, NET_AMOUNT, PLAN_TOTAL, ROW_HASH, CREATED_AT) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
      baseValues,
    );
  }

  private async hasSipagCanonColumns(): Promise<boolean> {
    const cacheKey = 'T_SIPAG_SALES';
    if (this.sipagCanonColumnsCache.has(cacheKey)) {
      return this.sipagCanonColumnsCache.get(cacheKey) === true;
    }
    const requiredColumns = [
      'CANON_SALE_DATE',
      'CANON_METHOD',
      'CANON_METHOD_GROUP',
      'CANON_BRAND',
      'CANON_TERMINAL_NO',
      'CANON_AUTH_CODE',
      'CANON_NSU',
      'CANON_GROSS_AMOUNT',
      'CANON_INSTALLMENT_NO',
      'CANON_INSTALLMENT_TOTAL',
    ];

    for (const column of requiredColumns) {
      if (!(await this.columnExists('T_SIPAG_SALES', column))) {
        this.sipagCanonColumnsCache.set(cacheKey, false);
        return false;
      }
    }

    this.sipagCanonColumnsCache.set(cacheKey, true);
    return true;
  }

  private async hasSipagCanonFinancialColumns(): Promise<boolean> {
    const cacheKey = 'T_SIPAG_SALES:FINANCIAL';
    if (this.sipagCanonColumnsCache.has(cacheKey)) {
      return this.sipagCanonColumnsCache.get(cacheKey) === true;
    }
    const requiredColumns = [
      'CANON_FEE_AMOUNT',
      'CANON_NET_AMOUNT',
      'CANON_PERC_TAXA',
    ];

    for (const column of requiredColumns) {
      if (!(await this.columnExists('T_SIPAG_SALES', column))) {
        this.sipagCanonColumnsCache.set(cacheKey, false);
        return false;
      }
    }

    this.sipagCanonColumnsCache.set(cacheKey, true);
    return true;
  }

  private async hasSicrediCanonColumns(): Promise<boolean> {
    const cacheKey = 'T_SICREDI_SALES';
    if (this.sipagCanonColumnsCache.has(cacheKey)) {
      return this.sipagCanonColumnsCache.get(cacheKey) === true;
    }
    const requiredColumns = [
      'CANON_SALE_DATE',
      'CANON_METHOD',
      'CANON_METHOD_GROUP',
      'CANON_BRAND',
      'CANON_TERMINAL_NO',
      'CANON_AUTH_CODE',
      'CANON_NSU',
      'CANON_GROSS_AMOUNT',
      'CANON_INSTALLMENT_NO',
      'CANON_INSTALLMENT_TOTAL',
    ];

    for (const column of requiredColumns) {
      if (!(await this.columnExists('T_SICREDI_SALES', column))) {
        this.sipagCanonColumnsCache.set(cacheKey, false);
        return false;
      }
    }

    this.sipagCanonColumnsCache.set(cacheKey, true);
    return true;
  }

  private async hasSicrediCanonFinancialColumns(): Promise<boolean> {
    const cacheKey = 'T_SICREDI_SALES:FINANCIAL';
    if (this.sipagCanonColumnsCache.has(cacheKey)) {
      return this.sipagCanonColumnsCache.get(cacheKey) === true;
    }
    const requiredColumns = [
      'CANON_FEE_AMOUNT',
      'CANON_NET_AMOUNT',
      'CANON_PERC_TAXA',
    ];

    for (const column of requiredColumns) {
      if (!(await this.columnExists('T_SICREDI_SALES', column))) {
        this.sipagCanonColumnsCache.set(cacheKey, false);
        return false;
      }
    }

    this.sipagCanonColumnsCache.set(cacheKey, true);
    return true;
  }

  private async relationExists(name: string): Promise<boolean> {
    const relationName = name.toUpperCase();
    if (this.relationExistsCache.has(relationName)) {
      return this.relationExistsCache.get(relationName) === true;
    }
    const rows = await this.dbService.query<{ TOTAL: number }>(
      'SELECT COUNT(*) AS TOTAL FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = ?',
      [relationName],
    );
    const exists = (rows[0]?.TOTAL ?? 0) > 0;
    this.relationExistsCache.set(relationName, exists);
    return exists;
  }

  private async columnExists(relationName: string, columnName: string): Promise<boolean> {
    const rows = await this.dbService.query<{ TOTAL: number }>(
      'SELECT COUNT(*) AS TOTAL FROM RDB$RELATION_FIELDS WHERE RDB$RELATION_NAME = ? AND RDB$FIELD_NAME = ?',
      [relationName.toUpperCase(), columnName.toUpperCase()],
    );
    return (rows[0]?.TOTAL ?? 0) > 0;
  }

  private parseSipagEdiDateTime(dateValue?: string, timeValue?: string): Date | null {
    const dateText = normalizeText(dateValue);
    if (!dateText || !/^\d{8}$/.test(dateText)) {
      return null;
    }
    const day = Number(dateText.slice(0, 2));
    const month = Number(dateText.slice(2, 4));
    const year = Number(dateText.slice(4, 8));

    let hour = 0;
    let minute = 0;
    let second = 0;
    const timeText = normalizeText(timeValue);
    if (timeText) {
      const match = timeText.match(/^(\d{2}):(\d{2})(?::(\d{2}))?$/);
      if (match) {
        hour = Number(match[1]);
        minute = Number(match[2]);
        second = match[3] ? Number(match[3]) : 0;
      } else {
        const compact = timeText.replace(/\D/g, '');
        if (compact.length === 6) {
          hour = Number(compact.slice(0, 2));
          minute = Number(compact.slice(2, 4));
          second = Number(compact.slice(4, 6));
        } else if (compact.length === 4) {
          hour = Number(compact.slice(0, 2));
          minute = Number(compact.slice(2, 4));
        }
      }
    }

    const parsed = new Date(year, month - 1, day, hour, minute, second);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  }

  private parseSipagEdiCents(value?: string): number | null {
    const digits = normalizeText(value).replace(/\D/g, '');
    if (!digits) {
      return null;
    }
    const parsed = Number(digits);
    if (!Number.isFinite(parsed)) {
      return null;
    }
    return Number((parsed / 100).toFixed(2));
  }

  private parseSipagEdiInteger(value?: string): number | null {
    const digits = normalizeText(value).replace(/\D/g, '');
    if (!digits) {
      return null;
    }
    const parsed = Number(digits);
    return Number.isFinite(parsed) ? parsed : null;
  }

  private normalizeSipagStatus(value?: string | null): string | null {
    const text = normalizeText(value ?? '');
    if (!text) {
      return null;
    }
    const normalized = text
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .toUpperCase();
    if (
      normalized === 'OK' ||
      normalized.includes('OK - SUCESSO') ||
      normalized.includes('OK SUCESSO') ||
      normalized === 'APPROVED'
    ) {
      return 'APROVADA';
    }
    if (normalized.startsWith('UNDONE') || normalized === 'UNAUTHORIZED - 90') {
      return 'CANCELADO';
    }
    return text;
  }

  private normalizeSicrediStatus(value?: string | null): string | null {
    const text = normalizeText(value ?? '');
    if (!text) {
      return null;
    }
    const normalized = text
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .toUpperCase();
    if (
      normalized === 'APPROVED' ||
      normalized === 'SUCESSO' ||
      normalized === 'APROVADA'
    ) {
      return 'Aprovada';
    }
    if (normalized.startsWith('UNDONE') || normalized === 'UNAUTHORIZED - 90') {
      return 'Cancelado';
    }
    return text;
  }

  async importSicredi(buffer: Buffer, fileName = 'upload.csv'): Promise<ImportResult> {
    const sha256 = createHash('sha256').update(buffer).digest('hex');
    if (await this.isImportedFileHash(sha256)) {
      await appendImportLog({
        category: 'acquirer_import',
        provider: 'SICREDI',
        operation: 'import_file',
        status: 'skipped_duplicate_file',
        fileName,
        details: { reason: 'hash_already_imported', hash: sha256 },
      });
      return this.buildAlreadyImportedResult(sha256);
    }
    const pickLines = (content: string) =>
      content.replace(/^\uFEFF/, '').split(/\r?\n/).filter((line) => line.trim());
    const findHeaderIndex = (rows: string[]) =>
      rows.findIndex((line) =>
        normalizeHeader(line).startsWith(
          'data da venda hora da venda codigo de autorizacao',
        ),
      );

    let lines = pickLines(buffer.toString('utf8'));
    let headerIndex = findHeaderIndex(lines);
    if (headerIndex < 0) {
      lines = pickLines(buffer.toString('latin1'));
      headerIndex = findHeaderIndex(lines);
    }
    if (headerIndex < 0) {
      await appendImportLog({
        category: 'acquirer_import',
        provider: 'SICREDI',
        operation: 'import_file',
        status: 'unknown_format',
        fileName,
        details: { error: 'Cabecalho Sicredi nao encontrado', hash: sha256 },
      });
      throw new Error('Cabecalho Sicredi nao encontrado');
    }

    const header = parseCsvLine(lines[headerIndex], ';');
    const normalizedHeader = header.map((cell) => normalizeHeader(cell));
    const findIndex = (candidates: string[]) =>
      normalizedHeader.findIndex((value) =>
        candidates.some((candidate) => value === candidate),
      );
    const findLastIndex = (candidates: string[]) => {
      for (let i = normalizedHeader.length - 1; i >= 0; i -= 1) {
        if (candidates.includes(normalizedHeader[i])) {
          return i;
        }
      }
      return -1;
    };
    const getValue = (row: string[], idx: number) => (idx >= 0 ? row[idx] : '');

    const indexDataVenda = findIndex(['data da venda']);
    const indexHoraVenda = findIndex(['hora da venda']);
    const indexAuthCode = findIndex(['codigo de autorizacao']);
    const indexEstCode = findIndex(['codigo do estabelecimento', 'codigo estabelecimento']);
    const indexEstName = findIndex(['nome do estabelecimento']);
    const indexReceipt = findIndex(['comprovante de venda']);
    const indexOrderNo = findIndex(['pedido']);
    const indexChannel = findIndex(['canal de venda']);
    const indexTerminal = findIndex(['numero do terminal', 'n do terminal']);
    const indexProduct = findIndex(['produto']);
    const indexCardType = findIndex(['tipo de cartao']);
    const indexBrand = findIndex(['bandeira']);
    const indexStatus = findIndex(['status']);
    const indexGross = findIndex(['valor bruto da transacao']);
    const indexMdr = findIndex(['valor da taxa mdr', 'valor da taxa (mdr)']);
    const indexNet = findIndex(['valor liquido da parcela transacao', 'valor liquido da parcela/transacao']);
    const indexOrderId = findIndex(['id do pedido', 'id. do pedido']);
    const indexCardNumber = findIndex(['numero do cartao', 'n do cartao', 'nº do cartao']);
    const indexPrepaid = findIndex(['pre pago', 'pre-pago']);
    const indexExpectedPay = findIndex(['data prevista de pagamento da venda']);
    const indexPayStatus = findIndex(['status do pagamento da venda']);
    const indexPayDate = findIndex(['data de pagamento efetivo da venda']);
    const indexPayCode = findIndex(['codigo de pagamento']);
    const indexCardRef = findIndex(['cod ref cartao', 'cod. ref. cartao', 'cod ref. cartao']);
    const indexCardOrigin = findLastIndex(['tipo de cartao']);

    let inserted = 0;
    let duplicates = 0;
    let invalidRows = 0;
    let dateFrom: string | null = null;
    let dateTo: string | null = null;

    this.logger.log(`Sicredi header detectado (linha ${headerIndex + 1}).`);
    this.logger.log(`Sicredi linhas totais: ${lines.length - headerIndex - 1}`);

    for (let i = headerIndex + 1; i < lines.length; i += 1) {
      let row = parseCsvLine(lines[i], ';');
      if (row.length > header.length) {
        const extra = row.slice(header.length);
        if (extra.every((value) => !normalizeText(value))) {
          row = row.slice(0, header.length);
        }
      }
      if (row.length < header.length) {
        invalidRows += 1;
        continue;
      }

      const saleDate = parseDateTimeParts(getValue(row, indexDataVenda), getValue(row, indexHoraVenda));
      const authCode = normalizeText(getValue(row, indexAuthCode));
      const establishmentCode = normalizeText(getValue(row, indexEstCode));
      const establishmentName = normalizeText(getValue(row, indexEstName));
      const saleReceipt = normalizeText(getValue(row, indexReceipt));
      const orderNo = normalizeText(getValue(row, indexOrderNo));
      const salesChannel = normalizeText(getValue(row, indexChannel));
      const terminalNo = normalizeText(getValue(row, indexTerminal));
      const product = normalizeText(getValue(row, indexProduct));
      const cardType = normalizeText(getValue(row, indexCardType));
      const brand = normalizeText(getValue(row, indexBrand));
      const status = normalizeText(getValue(row, indexStatus));
      const gross = parseMoneyFlexible(getValue(row, indexGross));
      const mdrAmount = parseMoneyFlexible(getValue(row, indexMdr));
      const net = parseMoneyFlexible(getValue(row, indexNet));
      const orderIdDesc = normalizeText(getValue(row, indexOrderId));
      const cardNumber = normalizeText(getValue(row, indexCardNumber));
      const prepaid = normalizeText(getValue(row, indexPrepaid));
      const expectedPayDate = parseDateTime(getValue(row, indexExpectedPay));
      const payStatus = normalizeText(getValue(row, indexPayStatus));
      const payDate = parseDateTime(getValue(row, indexPayDate));
      const paymentCode = normalizeText(getValue(row, indexPayCode));
      const cardRefCode = normalizeText(getValue(row, indexCardRef));
      const cardOrigin = normalizeText(getValue(row, indexCardOrigin));

      if (!saleDate || gross === null) {
        invalidRows += 1;
        continue;
      }
      ({ min: dateFrom, max: dateTo } = this.updateDateWindow(dateFrom, dateTo, saleDate));

      const rowHash = hashRow([
        saleDate.toISOString(),
        authCode,
        saleReceipt,
        terminalNo,
        gross ?? '',
        net ?? '',
        establishmentCode,
      ]);

      const exists = await this.dbService.query<{ ID: number }>(
        'SELECT FIRST 1 ID FROM T_SICREDI_SALES WHERE ROW_HASH = ?',
        [rowHash],
      );
      let storedRowHash = rowHash;
      if (exists.length) {
        duplicates += 1;
        storedRowHash = this.buildStoredRowHash(rowHash, duplicates);
      }
      const fit = (value: string | null, max: number): string | null => {
        if (!value) {
          return null;
        }
        const text = String(value).trim();
        return text.length > max ? text.slice(0, max) : text;
      };
      const canon = canonizeSicredi({
        SALE_DATETIME: saleDate,
        GROSS_AMOUNT: gross,
        MDR_AMOUNT: mdrAmount,
        NET_AMOUNT: net,
        PRODUCT: product,
        CARD_TYPE: cardType,
        BRAND: brand,
        TERMINAL_NO: terminalNo,
        AUTH_CODE: authCode,
        SALE_RECEIPT: saleReceipt,
        PAYMENT_CODE: paymentCode,
        CARD_REF_CODE: cardRefCode,
      });
      const canonSafe = {
        CANON_SALE_DATE: canon.CANON_SALE_DATE,
        CANON_METHOD: fit(canon.CANON_METHOD, 16),
        CANON_METHOD_GROUP: fit(canon.CANON_METHOD_GROUP, 8),
        CANON_BRAND: fit(canon.CANON_BRAND, 32),
        CANON_TERMINAL_NO: fit(canon.CANON_TERMINAL_NO, 32),
        CANON_AUTH_CODE: fit(canon.CANON_AUTH_CODE, 32),
        CANON_NSU: fit(canon.CANON_NSU, 32),
        CANON_GROSS_AMOUNT: canon.CANON_GROSS_AMOUNT,
        CANON_FEE_AMOUNT: canon.CANON_FEE_AMOUNT,
        CANON_NET_AMOUNT: canon.CANON_NET_AMOUNT,
        CANON_PERC_TAXA: canon.CANON_PERC_TAXA,
        CANON_INSTALLMENT_NO: canon.CANON_INSTALLMENT_NO,
        CANON_INSTALLMENT_TOTAL: canon.CANON_INSTALLMENT_TOTAL,
      };

      const baseValues = [
        saleDate,
        authCode || null,
        establishmentCode || null,
        establishmentName || null,
        saleReceipt || null,
        orderNo || null,
        salesChannel || null,
        terminalNo || null,
        product || null,
        cardType || null,
        brand || null,
        status || null,
        gross,
        mdrAmount,
        net,
        orderIdDesc || null,
        cardNumber || null,
        prepaid || null,
        expectedPayDate,
        payStatus || null,
        payDate,
        paymentCode || null,
        cardRefCode || null,
        cardOrigin || null,
        storedRowHash,
        new Date(),
      ];

      const hasBaseCanon = await this.hasSicrediCanonColumns();
      if (hasBaseCanon && (await this.hasSicrediCanonFinancialColumns())) {
        const values = [
          ...baseValues,
          canonSafe.CANON_SALE_DATE,
          canonSafe.CANON_METHOD,
          canonSafe.CANON_METHOD_GROUP,
          canonSafe.CANON_BRAND,
          canonSafe.CANON_TERMINAL_NO,
          canonSafe.CANON_AUTH_CODE,
          canonSafe.CANON_NSU,
          canonSafe.CANON_GROSS_AMOUNT,
          canonSafe.CANON_FEE_AMOUNT,
          canonSafe.CANON_NET_AMOUNT,
          canonSafe.CANON_PERC_TAXA,
          canonSafe.CANON_INSTALLMENT_NO,
          canonSafe.CANON_INSTALLMENT_TOTAL,
        ];
        const placeholders = new Array(values.length).fill('?').join(', ');
        await this.dbService.execute(
          `INSERT INTO T_SICREDI_SALES (SALE_DATETIME, AUTH_CODE, ESTABLISHMENT_CODE, ESTABLISHMENT_NAME, SALE_RECEIPT, ORDER_NO, SALES_CHANNEL, TERMINAL_NO, PRODUCT, CARD_TYPE, BRAND, STATUS, GROSS_AMOUNT, MDR_AMOUNT, NET_AMOUNT, ORDER_ID_DESC, CARD_NUMBER, PREPAID, EXPECTED_PAY_DATE, PAY_STATUS, PAY_DATE, PAYMENT_CODE, CARD_REF_CODE, CARD_ORIGIN, ROW_HASH, CREATED_AT, CANON_SALE_DATE, CANON_METHOD, CANON_METHOD_GROUP, CANON_BRAND, CANON_TERMINAL_NO, CANON_AUTH_CODE, CANON_NSU, CANON_GROSS_AMOUNT, CANON_FEE_AMOUNT, CANON_NET_AMOUNT, CANON_PERC_TAXA, CANON_INSTALLMENT_NO, CANON_INSTALLMENT_TOTAL) VALUES (${placeholders})`,
          values,
        );
      } else if (hasBaseCanon) {
        const values = [
          ...baseValues,
          canonSafe.CANON_SALE_DATE,
          canonSafe.CANON_METHOD,
          canonSafe.CANON_METHOD_GROUP,
          canonSafe.CANON_BRAND,
          canonSafe.CANON_TERMINAL_NO,
          canonSafe.CANON_AUTH_CODE,
          canonSafe.CANON_NSU,
          canonSafe.CANON_GROSS_AMOUNT,
          canonSafe.CANON_INSTALLMENT_NO,
          canonSafe.CANON_INSTALLMENT_TOTAL,
        ];
        const placeholders = new Array(values.length).fill('?').join(', ');
        await this.dbService.execute(
          `INSERT INTO T_SICREDI_SALES (SALE_DATETIME, AUTH_CODE, ESTABLISHMENT_CODE, ESTABLISHMENT_NAME, SALE_RECEIPT, ORDER_NO, SALES_CHANNEL, TERMINAL_NO, PRODUCT, CARD_TYPE, BRAND, STATUS, GROSS_AMOUNT, MDR_AMOUNT, NET_AMOUNT, ORDER_ID_DESC, CARD_NUMBER, PREPAID, EXPECTED_PAY_DATE, PAY_STATUS, PAY_DATE, PAYMENT_CODE, CARD_REF_CODE, CARD_ORIGIN, ROW_HASH, CREATED_AT, CANON_SALE_DATE, CANON_METHOD, CANON_METHOD_GROUP, CANON_BRAND, CANON_TERMINAL_NO, CANON_AUTH_CODE, CANON_NSU, CANON_GROSS_AMOUNT, CANON_INSTALLMENT_NO, CANON_INSTALLMENT_TOTAL) VALUES (${placeholders})`,
          values,
        );
      } else {
        await this.dbService.execute(
          'INSERT INTO T_SICREDI_SALES (SALE_DATETIME, AUTH_CODE, ESTABLISHMENT_CODE, ESTABLISHMENT_NAME, SALE_RECEIPT, ORDER_NO, SALES_CHANNEL, TERMINAL_NO, PRODUCT, CARD_TYPE, BRAND, STATUS, GROSS_AMOUNT, MDR_AMOUNT, NET_AMOUNT, ORDER_ID_DESC, CARD_NUMBER, PREPAID, EXPECTED_PAY_DATE, PAY_STATUS, PAY_DATE, PAYMENT_CODE, CARD_REF_CODE, CARD_ORIGIN, ROW_HASH, CREATED_AT) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
          baseValues,
        );
      }
      inserted += 1;
    }

    this.logger.log(
      `Sicredi import concluido: inseridas=${inserted} duplicadas=${duplicates} invalidas=${invalidRows}`,
    );
    await this.recordImportedFile(fileName, sha256, 'SICREDI');
    await appendImportLog({
      category: 'acquirer_import',
      provider: 'SICREDI',
      operation: 'import_file',
      status: this.buildProcessedStatus(duplicates),
      fileName,
      details: { inserted, duplicates, invalidRows, hash: sha256 },
    });
    if (dateFrom && dateTo) {
      await this.canonDuplicateCleanupService.cleanupAfterImport('SICREDI', dateFrom, dateTo);
    }
    return { inserted, duplicates, invalidRows };
  }

  async importSicrediEdi(buffer: Buffer, fileName = 'upload.json'): Promise<ImportResult> {
    const sha256 = createHash('sha256').update(buffer).digest('hex');
    if (await this.isImportedFileHash(sha256)) {
      await appendImportLog({
        category: 'acquirer_import',
        provider: 'SICREDI',
        operation: 'import_file_edi',
        status: 'skipped_duplicate_file',
        fileName,
        details: { reason: 'hash_already_imported', hash: sha256 },
      });
      return this.buildAlreadyImportedResult(sha256);
    }

    const rawText = buffer.toString('utf8').replace(/^\uFEFF/, '').trim();
    if (!rawText) {
      throw new Error('Arquivo EDI Sicredi vazio');
    }

    let payload: any;
    try {
      payload = JSON.parse(rawText);
    } catch (error) {
      throw new Error(
        `JSON invalido no EDI Sicredi: ${error instanceof Error ? error.message : 'erro desconhecido'}`,
      );
    }

    const kind = this.detectSicrediEdiKind(payload, fileName);
    const result = await this.dbService.transaction<ImportResult>(async (tx) => {
      const fileId = await this.insertSicrediEdiFileTx(tx, {
        fileName,
        fileHash: sha256,
        kind,
        payload,
        rawText,
      });

      if (kind === 'S') {
        return this.importSicrediEdiSalesTx(tx, fileId, payload);
      }
      if (kind === 'P') {
        return this.importSicrediEdiFinanceTx(tx, fileId, payload);
      }
      return this.importSicrediEdiReceivablesTx(tx, fileId, payload);
    });

    await this.recordImportedFile(fileName, sha256, `SICREDI_EDI_${kind}`);
    await appendImportLog({
      category: 'acquirer_import',
      provider: 'SICREDI',
      operation: 'import_file_edi',
      status: this.buildProcessedStatus(result.duplicates),
      fileName,
      details: { kind, inserted: result.inserted, duplicates: result.duplicates, invalidRows: result.invalidRows, hash: sha256 },
    });
    await this.canonDuplicateCleanupService.cleanupAfterImport('SICREDI');
    this.logger.log(
      `Sicredi EDI import concluido tipo=${kind} inseridas=${result.inserted} duplicadas=${result.duplicates} invalidas=${result.invalidRows}`,
    );
    return result;
  }

  private detectSicrediEdiKind(payload: any, fileName: string): SicrediEdiKind {
    const headerDescription = normalizeText(payload?.fileHeader?.fileTypeDescription)
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .toUpperCase();
    if (headerDescription.includes('VENDAS')) {
      return 'S';
    }
    if (headerDescription.includes('FINANCEIRO')) {
      return 'P';
    }
    if (headerDescription.includes('RECEBIVEIS')) {
      return 'R';
    }

    const upperFileName = fileName.toUpperCase();
    if (upperFileName.includes('EDI-S-')) {
      return 'S';
    }
    if (upperFileName.includes('EDI-P-')) {
      return 'P';
    }
    if (upperFileName.includes('EDI-R-')) {
      return 'R';
    }

    throw new Error('Nao foi possivel identificar o tipo do EDI Sicredi');
  }

  private parseSicrediEdiDate(value?: string | null): Date | null {
    const digits = normalizeText(value).replace(/\D/g, '');
    if (!digits || digits.length !== 8) {
      return null;
    }
    const day = Number(digits.slice(0, 2));
    const month = Number(digits.slice(2, 4));
    const year = Number(digits.slice(4, 8));
    const parsed = new Date(year, month - 1, day, 0, 0, 0, 0);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  }

  private parseSicrediEdiDateTime(dateValue?: string | null, timeValue?: string | null): Date | null {
    const date = this.parseSicrediEdiDate(dateValue);
    if (!date) {
      return null;
    }
    const timeText = normalizeText(timeValue);
    if (!timeText) {
      return date;
    }
    const compact = timeText.replace(/\D/g, '');
    let hour = 0;
    let minute = 0;
    let second = 0;
    if (compact.length >= 2) {
      hour = Number(compact.slice(0, 2));
    }
    if (compact.length >= 4) {
      minute = Number(compact.slice(2, 4));
    }
    if (compact.length >= 6) {
      second = Number(compact.slice(4, 6));
    }
    const parsed = new Date(
      date.getFullYear(),
      date.getMonth(),
      date.getDate(),
      hour,
      minute,
      second,
      0,
    );
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  }

  private parseSicrediEdiDecimal(value?: string | null): number | null {
    const text = normalizeText(value);
    if (!text) {
      return null;
    }
    const compact = text.replace(/\s/g, '');
    let normalized = compact;
    const hasDot = compact.includes('.');
    const hasComma = compact.includes(',');

    if (hasDot && hasComma) {
      const lastDot = compact.lastIndexOf('.');
      const lastComma = compact.lastIndexOf(',');
      if (lastComma > lastDot) {
        normalized = compact.replace(/\./g, '').replace(',', '.');
      } else {
        normalized = compact.replace(/,/g, '');
      }
    } else if (hasComma) {
      normalized = compact.replace(',', '.');
    }

    const parsed = Number(normalized);
    return Number.isFinite(parsed) ? Number(parsed.toFixed(4)) : null;
  }

  private trimTo(value: unknown, max: number): string | null {
    const text = normalizeText(value == null ? '' : String(value));
    if (!text) {
      return null;
    }
    return text.length > max ? text.slice(0, max) : text;
  }

  private async insertSicrediEdiFileTx(
    tx: any,
    params: {
      fileName: string;
      fileHash: string;
      kind: SicrediEdiKind;
      payload: any;
      rawText: string;
    },
  ): Promise<number> {
    const header = params.payload?.fileHeader ?? {};
    const client = header.client ?? {};
    const rows = await this.dbService.queryTx<{ ID: number }>(
      tx,
      `INSERT INTO T_SICREDI_EDI_FILES (
        FILE_NAME, FILE_HASH, EDI_KIND, FILE_NUMBER, PROCESSING_DATE, ACQUIRING_NAME,
        FILE_TYPE_DESCRIPTION, PROCESSING_TYPE, FILE_LAYOUT_VERSION, CLIENT_CODE,
        CLIENT_DOCUMENT, CLIENT_NAME, RAW_JSON, CREATED_AT
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      RETURNING ID`,
      [
        params.fileName,
        params.fileHash,
        params.kind,
        this.trimTo(header.fileNumber, 32),
        this.parseSicrediEdiDate(header.processingDate),
        this.trimTo(header.acquiringName, 60),
        this.trimTo(header.fileTypeDescription, 80),
        this.trimTo(header.processingType, 40),
        this.trimTo(header.fileLayoutVersion, 20),
        this.trimTo(client.code, 20),
        this.trimTo(client.document, 20),
        this.trimTo(client.name, 120),
        params.rawText,
        new Date(),
      ],
    );
    return Number(rows[0]?.ID);
  }

  private async existsRowHashTx(tx: any, tableName: string, rowHash: string): Promise<boolean> {
    const rows = await this.dbService.queryTx<{ ID: number }>(
      tx,
      `SELECT FIRST 1 ID FROM ${tableName} WHERE ROW_HASH = ?`,
      [rowHash],
    );
    return rows.length > 0;
  }

  private async findExistingSicrediSaleIdTx(
    tx: any,
    params: {
      saleDate?: Date | null;
      authCode?: string | null;
      saleReceipt?: string | null;
      terminalNo?: string | null;
      grossAmount?: number | null;
      paymentCode?: string | null;
      cardRefCode?: string | null;
    },
  ): Promise<number | null> {
    const conditions: string[] = [];
    const values: unknown[] = [];

    if (params.saleDate) {
      conditions.push('CAST(SALE_DATETIME AS DATE) = ?');
      values.push(params.saleDate);
    }
    if (typeof params.grossAmount === 'number') {
      conditions.push('GROSS_AMOUNT = ?');
      values.push(params.grossAmount);
    }

    const keyParts: string[] = [];
    if (params.authCode) {
      keyParts.push('AUTH_CODE = ?');
      values.push(params.authCode);
    }
    if (params.saleReceipt) {
      keyParts.push('SALE_RECEIPT = ?');
      values.push(params.saleReceipt);
    }
    if (params.paymentCode) {
      keyParts.push('PAYMENT_CODE = ?');
      values.push(params.paymentCode);
    }
    if (params.cardRefCode) {
      keyParts.push('CARD_REF_CODE = ?');
      values.push(params.cardRefCode);
    }

    if (keyParts.length) {
      conditions.push(`(${keyParts.join(' OR ')})`);
    }
    if (params.terminalNo) {
      conditions.push('(TERMINAL_NO = ? OR COALESCE(CANON_TERMINAL_NO, \'\') = ?)');
      values.push(params.terminalNo, params.terminalNo.replace(/\D/g, ''));
    }

    if (!conditions.length) {
      return null;
    }

    const rows = await this.dbService.queryTx<{ ID: number }>(
      tx,
      `SELECT FIRST 1 ID
         FROM T_SICREDI_SALES
        WHERE ${conditions.join(' AND ')}
        ORDER BY ID DESC`,
      values,
    );
    return rows.length ? Number(rows[0].ID) : null;
  }

  private buildSicrediOperationalCanon(values: {
    saleDatetime: Date;
    grossAmount: number | null;
    mdrAmount: number | null;
    netAmount: number | null;
    sectionKind: string | null;
    product: string | null;
    cardType: string | null;
    cardSchemeCode: string | null;
    cardSchemeDesc: string | null;
    brand: string | null;
    terminalNo: string | null;
    authCode: string | null;
    saleReceipt: string | null;
    paymentCode: string | null;
    cardRefCode: string | null;
  }) {
    return canonizeSicredi({
      SALE_DATETIME: values.saleDatetime,
      GROSS_AMOUNT: values.grossAmount,
      MDR_AMOUNT: values.mdrAmount,
      NET_AMOUNT: values.netAmount,
      SECTION_KIND: values.sectionKind,
      PRODUCT: values.product,
      CARD_TYPE: values.cardType,
      CARD_SCHEME_CODE: values.cardSchemeCode,
      CARD_SCHEME_DESC: values.cardSchemeDesc,
      BRAND: values.brand,
      TERMINAL_NO: values.terminalNo,
      AUTH_CODE: values.authCode,
      SALE_RECEIPT: values.saleReceipt,
      PAYMENT_CODE: values.paymentCode,
      CARD_REF_CODE: values.cardRefCode,
    });
  }

  private buildSicrediOperationalCanonSafe(values: {
    saleDatetime: Date;
    grossAmount: number | null;
    mdrAmount: number | null;
    netAmount: number | null;
    sectionKind: string | null;
    product: string | null;
    cardType: string | null;
    cardSchemeCode: string | null;
    cardSchemeDesc: string | null;
    brand: string | null;
    terminalNo: string | null;
    authCode: string | null;
    saleReceipt: string | null;
    paymentCode: string | null;
    cardRefCode: string | null;
  }) {
    const canon = this.buildSicrediOperationalCanon(values);
    return {
      CANON_SALE_DATE: canon.CANON_SALE_DATE,
      CANON_METHOD: this.trimTo(canon.CANON_METHOD, 16),
      CANON_METHOD_GROUP: this.trimTo(canon.CANON_METHOD_GROUP, 8),
      CANON_BRAND: this.trimTo(canon.CANON_BRAND, 32),
      CANON_TERMINAL_NO: this.trimTo(canon.CANON_TERMINAL_NO, 32),
      CANON_AUTH_CODE: this.trimTo(canon.CANON_AUTH_CODE, 32),
      CANON_NSU: this.trimTo(canon.CANON_NSU, 32),
      CANON_GROSS_AMOUNT: canon.CANON_GROSS_AMOUNT,
      CANON_FEE_AMOUNT: canon.CANON_FEE_AMOUNT,
      CANON_NET_AMOUNT: canon.CANON_NET_AMOUNT,
      CANON_PERC_TAXA: canon.CANON_PERC_TAXA,
      CANON_INSTALLMENT_NO: canon.CANON_INSTALLMENT_NO,
      CANON_INSTALLMENT_TOTAL: canon.CANON_INSTALLMENT_TOTAL,
    };
  }

  private async upsertSicrediOperationalSaleTx(
    tx: any,
    params: {
      saleDatetime: Date;
      ediSourceKind: string | null;
      authCode: string | null;
      establishmentCode: string | null;
      establishmentName: string | null;
      matrixClientCode: string | null;
      saleReceipt: string | null;
      salesSummaryNumber: string | null;
      orderNo: string | null;
      salesChannel: string | null;
      terminalNo: string | null;
      product: string | null;
      cardType: string | null;
      cardTypeCode: string | null;
      brand: string | null;
      cardSchemeCode: string | null;
      cardSchemeDesc: string | null;
      status: string | null;
      transactionTypeCode: string | null;
      transactionTypeDesc: string | null;
      transactionStatusCode: string | null;
      transactionStatusDesc: string | null;
      grossAmount: number | null;
      mdrAmount: number | null;
      netAmount: number | null;
      orderIdDesc: string | null;
      cardNumber: string | null;
      prepaid: string | null;
      expectedPayDate: Date | null;
      payStatus: string | null;
      payDate: Date | null;
      paymentCode: string | null;
      cardRefCode: string | null;
      cardOrigin: string | null;
      paymentAccountReference: string | null;
      idUr: string | null;
      feePercent: number | null;
      interchangePlusAmount: number | null;
      interchangeFeePercent: number | null;
      cashbackAmount: number | null;
      reverseInterchange: number | null;
      entryModeCode: string | null;
      entryModeDesc: string | null;
      technologyTypeCode: string | null;
      technologyTypeDesc: string | null;
      rowHashSeed: Array<string | number | null | undefined>;
    },
  ): Promise<'inserted' | 'updated'> {
    const saleDate = new Date(
      params.saleDatetime.getFullYear(),
      params.saleDatetime.getMonth(),
      params.saleDatetime.getDate(),
    );
    const existingId = await this.findExistingSicrediSaleIdTx(tx, {
      saleDate,
      authCode: params.authCode,
      saleReceipt: params.saleReceipt,
      terminalNo: params.terminalNo,
      grossAmount: params.grossAmount,
      paymentCode: params.paymentCode,
      cardRefCode: params.cardRefCode,
    });

    const canon = this.buildSicrediOperationalCanonSafe({
      saleDatetime: params.saleDatetime,
      grossAmount: params.grossAmount,
      mdrAmount: params.mdrAmount,
      netAmount: params.netAmount,
      sectionKind:
        params.ediSourceKind === 'EDI_PIX'
          ? 'PIX'
          : params.ediSourceKind === 'EDI_DEB'
            ? 'DEBIT'
            : params.ediSourceKind === 'EDI_CRD'
              ? 'CREDIT'
              : null,
      product: params.product,
      cardType: params.cardType,
      cardSchemeCode: params.cardSchemeCode,
      cardSchemeDesc: params.cardSchemeDesc,
      brand: params.brand,
      terminalNo: params.terminalNo,
      authCode: params.authCode,
      saleReceipt: params.saleReceipt,
      paymentCode: params.paymentCode,
      cardRefCode: params.cardRefCode,
    });
    const rowHash = hashRow(['SICREDI_EDI_OP', ...params.rowHashSeed]);

    if (existingId) {
      await this.dbService.executeTx(
        tx,
        `UPDATE T_SICREDI_SALES
            SET EDI_SOURCE_KIND = COALESCE(EDI_SOURCE_KIND, ?),
                AUTH_CODE = COALESCE(AUTH_CODE, ?),
                ESTABLISHMENT_CODE = COALESCE(ESTABLISHMENT_CODE, ?),
                ESTABLISHMENT_NAME = COALESCE(ESTABLISHMENT_NAME, ?),
                MATRIX_CLIENT_CODE = COALESCE(MATRIX_CLIENT_CODE, ?),
                SALE_RECEIPT = COALESCE(SALE_RECEIPT, ?),
                SALES_SUMMARY_NUMBER = COALESCE(SALES_SUMMARY_NUMBER, ?),
                ORDER_NO = COALESCE(ORDER_NO, ?),
                SALES_CHANNEL = COALESCE(SALES_CHANNEL, ?),
                TERMINAL_NO = COALESCE(TERMINAL_NO, ?),
                PRODUCT = COALESCE(PRODUCT, ?),
                CARD_TYPE = COALESCE(CARD_TYPE, ?),
                CARD_TYPE_CODE = COALESCE(CARD_TYPE_CODE, ?),
                BRAND = COALESCE(BRAND, ?),
                CARD_SCHEME_CODE = COALESCE(CARD_SCHEME_CODE, ?),
                CARD_SCHEME_DESC = COALESCE(CARD_SCHEME_DESC, ?),
                STATUS = COALESCE(STATUS, ?),
                TRANSACTION_TYPE_CODE = COALESCE(TRANSACTION_TYPE_CODE, ?),
                TRANSACTION_TYPE_DESC = COALESCE(TRANSACTION_TYPE_DESC, ?),
                TRANSACTION_STATUS_CODE = COALESCE(TRANSACTION_STATUS_CODE, ?),
                TRANSACTION_STATUS_DESC = COALESCE(TRANSACTION_STATUS_DESC, ?),
                GROSS_AMOUNT = COALESCE(GROSS_AMOUNT, ?),
                MDR_AMOUNT = COALESCE(MDR_AMOUNT, ?),
                NET_AMOUNT = COALESCE(NET_AMOUNT, ?),
                ORDER_ID_DESC = COALESCE(ORDER_ID_DESC, ?),
                CARD_NUMBER = COALESCE(CARD_NUMBER, ?),
                PREPAID = COALESCE(PREPAID, ?),
                EXPECTED_PAY_DATE = COALESCE(EXPECTED_PAY_DATE, ?),
                PAY_STATUS = COALESCE(PAY_STATUS, ?),
                PAY_DATE = COALESCE(PAY_DATE, ?),
                PAYMENT_CODE = COALESCE(PAYMENT_CODE, ?),
                CARD_REF_CODE = COALESCE(CARD_REF_CODE, ?),
                CARD_ORIGIN = COALESCE(CARD_ORIGIN, ?),
                PAYMENT_ACCOUNT_REFERENCE = COALESCE(PAYMENT_ACCOUNT_REFERENCE, ?),
                ID_UR = COALESCE(ID_UR, ?),
                FEE_PERCENT = COALESCE(FEE_PERCENT, ?),
                INTERCHANGE_PLUS_AMOUNT = COALESCE(INTERCHANGE_PLUS_AMOUNT, ?),
                INTERCHANGE_FEE_PERCENT = COALESCE(INTERCHANGE_FEE_PERCENT, ?),
                CASHBACK_AMOUNT = COALESCE(CASHBACK_AMOUNT, ?),
                REVERSE_INTERCHANGE = COALESCE(REVERSE_INTERCHANGE, ?),
                ENTRY_MODE_CODE = COALESCE(ENTRY_MODE_CODE, ?),
                ENTRY_MODE_DESC = COALESCE(ENTRY_MODE_DESC, ?),
                TECHNOLOGY_TYPE_CODE = COALESCE(TECHNOLOGY_TYPE_CODE, ?),
                TECHNOLOGY_TYPE_DESC = COALESCE(TECHNOLOGY_TYPE_DESC, ?),
                CANON_SALE_DATE = COALESCE(CANON_SALE_DATE, ?),
                CANON_METHOD = COALESCE(CANON_METHOD, ?),
                CANON_METHOD_GROUP = COALESCE(CANON_METHOD_GROUP, ?),
                CANON_BRAND = COALESCE(CANON_BRAND, ?),
                CANON_TERMINAL_NO = COALESCE(CANON_TERMINAL_NO, ?),
                CANON_AUTH_CODE = COALESCE(CANON_AUTH_CODE, ?),
                CANON_NSU = COALESCE(CANON_NSU, ?),
                CANON_GROSS_AMOUNT = COALESCE(CANON_GROSS_AMOUNT, ?),
                CANON_FEE_AMOUNT = COALESCE(CANON_FEE_AMOUNT, ?),
                CANON_NET_AMOUNT = COALESCE(CANON_NET_AMOUNT, ?),
                CANON_PERC_TAXA = COALESCE(CANON_PERC_TAXA, ?),
                CANON_INSTALLMENT_NO = COALESCE(CANON_INSTALLMENT_NO, ?),
                CANON_INSTALLMENT_TOTAL = COALESCE(CANON_INSTALLMENT_TOTAL, ?)
          WHERE ID = ?`,
        [
          params.ediSourceKind,
          params.authCode,
          params.establishmentCode,
          params.establishmentName,
          params.matrixClientCode,
          params.saleReceipt,
          params.salesSummaryNumber,
          params.orderNo,
          params.salesChannel,
          params.terminalNo,
          params.product,
          params.cardType,
          params.cardTypeCode,
          params.brand,
          params.cardSchemeCode,
          params.cardSchemeDesc,
          params.status,
          params.transactionTypeCode,
          params.transactionTypeDesc,
          params.transactionStatusCode,
          params.transactionStatusDesc,
          params.grossAmount,
          params.mdrAmount,
          params.netAmount,
          params.orderIdDesc,
          params.cardNumber,
          params.prepaid,
          params.expectedPayDate,
          params.payStatus,
          params.payDate,
          params.paymentCode,
          params.cardRefCode,
          params.cardOrigin,
          params.paymentAccountReference,
          params.idUr,
          params.feePercent,
          params.interchangePlusAmount,
          params.interchangeFeePercent,
          params.cashbackAmount,
          params.reverseInterchange,
          params.entryModeCode,
          params.entryModeDesc,
          params.technologyTypeCode,
          params.technologyTypeDesc,
          canon.CANON_SALE_DATE,
          canon.CANON_METHOD,
          canon.CANON_METHOD_GROUP,
          canon.CANON_BRAND,
          canon.CANON_TERMINAL_NO,
          canon.CANON_AUTH_CODE,
          canon.CANON_NSU,
          canon.CANON_GROSS_AMOUNT,
          canon.CANON_FEE_AMOUNT,
          canon.CANON_NET_AMOUNT,
          canon.CANON_PERC_TAXA,
          canon.CANON_INSTALLMENT_NO,
          canon.CANON_INSTALLMENT_TOTAL,
          existingId,
        ],
      );
      return 'updated';
    }

    await this.dbService.executeTx(
      tx,
      `INSERT INTO T_SICREDI_SALES (
        SALE_DATETIME, EDI_SOURCE_KIND, AUTH_CODE, ESTABLISHMENT_CODE, ESTABLISHMENT_NAME, MATRIX_CLIENT_CODE,
        SALE_RECEIPT, SALES_SUMMARY_NUMBER, ORDER_NO, SALES_CHANNEL, TERMINAL_NO, PRODUCT, CARD_TYPE,
        CARD_TYPE_CODE, BRAND, CARD_SCHEME_CODE, CARD_SCHEME_DESC, STATUS, TRANSACTION_TYPE_CODE,
        TRANSACTION_TYPE_DESC, TRANSACTION_STATUS_CODE, TRANSACTION_STATUS_DESC, GROSS_AMOUNT, MDR_AMOUNT,
        NET_AMOUNT, ORDER_ID_DESC, CARD_NUMBER, PREPAID, EXPECTED_PAY_DATE, PAY_STATUS, PAY_DATE,
        PAYMENT_CODE, CARD_REF_CODE, CARD_ORIGIN, PAYMENT_ACCOUNT_REFERENCE, ID_UR, FEE_PERCENT,
        INTERCHANGE_PLUS_AMOUNT, INTERCHANGE_FEE_PERCENT, CASHBACK_AMOUNT, REVERSE_INTERCHANGE,
        ENTRY_MODE_CODE, ENTRY_MODE_DESC, TECHNOLOGY_TYPE_CODE, TECHNOLOGY_TYPE_DESC, ROW_HASH, CREATED_AT,
        CANON_SALE_DATE, CANON_METHOD, CANON_METHOD_GROUP, CANON_BRAND, CANON_TERMINAL_NO, CANON_AUTH_CODE,
        CANON_NSU, CANON_GROSS_AMOUNT, CANON_FEE_AMOUNT, CANON_NET_AMOUNT, CANON_PERC_TAXA,
        CANON_INSTALLMENT_NO, CANON_INSTALLMENT_TOTAL
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        params.saleDatetime,
        params.ediSourceKind,
        params.authCode,
        params.establishmentCode,
        params.establishmentName,
        params.matrixClientCode,
        params.saleReceipt,
        params.salesSummaryNumber,
        params.orderNo,
        params.salesChannel,
        params.terminalNo,
        params.product,
        params.cardType,
        params.cardTypeCode,
        params.brand,
        params.cardSchemeCode,
        params.cardSchemeDesc,
        params.status,
        params.transactionTypeCode,
        params.transactionTypeDesc,
        params.transactionStatusCode,
        params.transactionStatusDesc,
        params.grossAmount,
        params.mdrAmount,
        params.netAmount,
        params.orderIdDesc,
        params.cardNumber,
        params.prepaid,
        params.expectedPayDate,
        params.payStatus,
        params.payDate,
        params.paymentCode,
        params.cardRefCode,
        params.cardOrigin,
        params.paymentAccountReference,
        params.idUr,
        params.feePercent,
        params.interchangePlusAmount,
        params.interchangeFeePercent,
        params.cashbackAmount,
        params.reverseInterchange,
        params.entryModeCode,
        params.entryModeDesc,
        params.technologyTypeCode,
        params.technologyTypeDesc,
        rowHash,
        new Date(),
        canon.CANON_SALE_DATE,
        canon.CANON_METHOD,
        canon.CANON_METHOD_GROUP,
        canon.CANON_BRAND,
        canon.CANON_TERMINAL_NO,
        canon.CANON_AUTH_CODE,
        canon.CANON_NSU,
        canon.CANON_GROSS_AMOUNT,
        canon.CANON_FEE_AMOUNT,
        canon.CANON_NET_AMOUNT,
        canon.CANON_PERC_TAXA,
        canon.CANON_INSTALLMENT_NO,
        canon.CANON_INSTALLMENT_TOTAL,
      ],
    );
    return 'inserted';
  }

  private async enrichSicrediOperationalSaleFromFinanceTx(
    tx: any,
    params: {
      saleDate: Date | null;
      authCode: string | null;
      saleReceipt: string | null;
      paymentCode: string | null;
      cardRefCode: string | null;
      terminalNo?: string | null;
      grossAmount: number | null;
      netAmount: number | null;
      mdrAmount: number | null;
      payDate: Date | null;
      expectedPayDate: Date | null;
      payStatus: string | null;
      paymentInstructionNo: string | null;
      financeSummaryNumber: string | null;
      salesReference: string | null;
      valueDate: Date | null;
      transactionDate: Date | null;
      transactionTime: string | null;
      paymentTypeCode: string | null;
      paymentTypeDesc: string | null;
      paymentTransactionTypeCode: string | null;
      paymentTransactionTypeDesc: string | null;
      receiptTypeCode: string | null;
      receiptTypeDesc: string | null;
      transactionDescriptionCode: string | null;
      transactionDescription: string | null;
      feeModeAmount: number | null;
      feeModeNetAmount: number | null;
      paymentAccountReference: string | null;
      idUr: string | null;
      cardNumber: string | null;
      product: string | null;
      cardType: string | null;
      brand: string | null;
    },
  ): Promise<boolean> {
    const existingId = await this.findExistingSicrediSaleIdTx(tx, {
      saleDate: params.saleDate,
      authCode: params.authCode,
      saleReceipt: params.saleReceipt,
      terminalNo: params.terminalNo,
      grossAmount: params.grossAmount,
      paymentCode: params.paymentCode,
      cardRefCode: params.cardRefCode,
    });
    if (!existingId) {
      return false;
    }

    const effectiveDate = params.saleDate ?? params.payDate ?? new Date();
    const canon = this.buildSicrediOperationalCanonSafe({
      saleDatetime: effectiveDate,
      grossAmount: params.grossAmount,
      mdrAmount: params.mdrAmount,
      netAmount: params.netAmount,
      sectionKind:
        params.product?.toUpperCase().includes('PIX')
          ? 'PIX'
          : params.product?.toUpperCase().includes('DEBIT')
            ? 'DEBIT'
            : params.product?.toUpperCase().includes('CREDIT') ||
                params.product?.toUpperCase().includes('CREDITO')
              ? 'CREDIT'
              : null,
      product: params.product,
      cardType: params.cardType,
      cardSchemeCode: null,
      cardSchemeDesc: params.brand,
      brand: params.brand,
      terminalNo: params.terminalNo ?? null,
      authCode: params.authCode,
      saleReceipt: params.saleReceipt,
      paymentCode: params.paymentCode,
      cardRefCode: params.cardRefCode,
    });

    await this.dbService.executeTx(
      tx,
      `UPDATE T_SICREDI_SALES
          SET PAY_DATE = COALESCE(PAY_DATE, ?),
              EXPECTED_PAY_DATE = COALESCE(EXPECTED_PAY_DATE, ?),
              PAY_STATUS = COALESCE(PAY_STATUS, ?),
              PAYMENT_INSTRUCTION_NO = COALESCE(PAYMENT_INSTRUCTION_NO, ?),
              FINANCE_SUMMARY_NUMBER = COALESCE(FINANCE_SUMMARY_NUMBER, ?),
              SALES_REFERENCE = COALESCE(SALES_REFERENCE, ?),
              VALUE_DATE = COALESCE(VALUE_DATE, ?),
              TRANSACTION_DATE = COALESCE(TRANSACTION_DATE, ?),
              TRANSACTION_TIME = COALESCE(TRANSACTION_TIME, ?),
              PAYMENT_CODE = COALESCE(PAYMENT_CODE, ?),
              CARD_REF_CODE = COALESCE(CARD_REF_CODE, ?),
              PAYMENT_TYPE_CODE = COALESCE(PAYMENT_TYPE_CODE, ?),
              PAYMENT_TYPE_DESC = COALESCE(PAYMENT_TYPE_DESC, ?),
              PAYMENT_TRANSACTION_TYPE_CODE = COALESCE(PAYMENT_TRANSACTION_TYPE_CODE, ?),
              PAYMENT_TRANSACTION_TYPE_DESC = COALESCE(PAYMENT_TRANSACTION_TYPE_DESC, ?),
              RECEIPT_TYPE_CODE = COALESCE(RECEIPT_TYPE_CODE, ?),
              RECEIPT_TYPE_DESC = COALESCE(RECEIPT_TYPE_DESC, ?),
              TRANSACTION_DESCRIPTION_CODE = COALESCE(TRANSACTION_DESCRIPTION_CODE, ?),
              TRANSACTION_DESCRIPTION = COALESCE(TRANSACTION_DESCRIPTION, ?),
              FEE_MODE_AMOUNT = COALESCE(FEE_MODE_AMOUNT, ?),
              FEE_MODE_NET_AMOUNT = COALESCE(FEE_MODE_NET_AMOUNT, ?),
              PAYMENT_ACCOUNT_REFERENCE = COALESCE(PAYMENT_ACCOUNT_REFERENCE, ?),
              ID_UR = COALESCE(ID_UR, ?),
              NET_AMOUNT = COALESCE(NET_AMOUNT, ?),
              MDR_AMOUNT = COALESCE(MDR_AMOUNT, ?),
              CARD_NUMBER = COALESCE(CARD_NUMBER, ?),
              PRODUCT = COALESCE(PRODUCT, ?),
              CARD_TYPE = COALESCE(CARD_TYPE, ?),
              BRAND = COALESCE(BRAND, ?),
              CANON_METHOD = COALESCE(CANON_METHOD, ?),
              CANON_METHOD_GROUP = COALESCE(CANON_METHOD_GROUP, ?),
              CANON_BRAND = COALESCE(CANON_BRAND, ?),
              CANON_AUTH_CODE = COALESCE(CANON_AUTH_CODE, ?),
              CANON_NSU = COALESCE(CANON_NSU, ?),
              CANON_FEE_AMOUNT = COALESCE(CANON_FEE_AMOUNT, ?),
              CANON_NET_AMOUNT = COALESCE(CANON_NET_AMOUNT, ?),
              CANON_PERC_TAXA = COALESCE(CANON_PERC_TAXA, ?)
        WHERE ID = ?`,
      [
        params.payDate,
        params.expectedPayDate,
        params.payStatus,
        params.paymentInstructionNo,
        params.financeSummaryNumber,
        params.salesReference,
        params.valueDate,
        params.transactionDate,
        params.transactionTime,
        params.paymentCode,
        params.cardRefCode,
        params.paymentTypeCode,
        params.paymentTypeDesc,
        params.paymentTransactionTypeCode,
        params.paymentTransactionTypeDesc,
        params.receiptTypeCode,
        params.receiptTypeDesc,
        params.transactionDescriptionCode,
        params.transactionDescription,
        params.feeModeAmount,
        params.feeModeNetAmount,
        params.paymentAccountReference,
        params.idUr,
        params.netAmount,
        params.mdrAmount,
        params.cardNumber,
        params.product,
        params.cardType,
        params.brand,
        canon.CANON_METHOD,
        canon.CANON_METHOD_GROUP,
        canon.CANON_BRAND,
        canon.CANON_AUTH_CODE,
        canon.CANON_NSU,
        canon.CANON_FEE_AMOUNT,
        canon.CANON_NET_AMOUNT,
        canon.CANON_PERC_TAXA,
        existingId,
      ],
    );
    return true;
  }

  private async importSicrediEdiSalesTx(tx: any, fileId: number, payload: any): Promise<ImportResult> {
    const sections = [
      { kind: 'PIX', rows: Array.isArray(payload?.pixTransactions) ? payload.pixTransactions : [] },
      { kind: 'DEBIT', rows: Array.isArray(payload?.debitSalesSummary) ? payload.debitSalesSummary : [] },
      { kind: 'CREDIT', rows: Array.isArray(payload?.creditSalesSummary) ? payload.creditSalesSummary : [] },
    ];

    let inserted = 0;
    let duplicates = 0;
    let invalidRows = 0;

    for (const section of sections) {
      if (section.kind === 'PIX') {
        for (const row of section.rows) {
          const saleDatetime =
            this.parseSicrediEdiDateTime(row?.dateQrCodeConfirmationReceived, row?.hourQrCodeConfirmationReceived) ??
            this.parseSicrediEdiDateTime(row?.dateQRCodeGenerated, row?.hourQRCodeGenerated);
          const amount = this.parseSicrediEdiDecimal(row?.amountTransaction);
          if (!saleDatetime || amount === null) {
            invalidRows += 1;
            continue;
          }
          const rowHash = hashRow([
            'SICREDI_EDI_S_PIX',
            saleDatetime.toISOString(),
            normalizeText(row?.authorizationCode),
            normalizeText(row?.nsuTransaction),
            amount,
            normalizeText(row?.terminalRegister),
          ]);
          let storedRowHash = rowHash;
          if (await this.existsRowHashTx(tx, 'T_SICREDI_EDI_SALES', rowHash)) {
            duplicates += 1;
            storedRowHash = this.buildStoredRowHash(rowHash, duplicates);
          }
          await this.dbService.executeTx(
            tx,
            `INSERT INTO T_SICREDI_EDI_SALES (
              FILE_ID, SECTION_KIND, SUMMARY_RECORD_TYPE, DETAIL_RECORD_TYPE, CLIENT_CODE, SALES_DATE,
              SALE_DATETIME, CREDIT_DATE, SALES_SUMMARY_NUMBER, SALES_RECEIPT_NUMBER, AUTHORIZATION_CODE,
              TERMINAL_NO, NSU_TRANSACTION, SYS_REF_NO, REF_FEPAS_NO,
              TRANSACTION_TYPE_CODE, TX_TYPE_DESC, TRANSACTION_STATUS_CODE, TX_STATUS_DESC,
              CARD_SCHEME_CODE, CARD_SCHEME_DESC, CARD_TYPE_CODE, CARD_TYPE_DESC, GROSS_AMOUNT,
              DISCOUNT_AMOUNT, NET_AMOUNT, FEE_PERCENT, INTERCHANGE_PLUS_AMOUNT, ACQUIRER_REFERENCE,
              PAY_ACCT_REF, MASKED_CARD_NUMBER, ID_UR, QR_CODE_STATUS, ROW_HASH, RAW_JSON, CREATED_AT
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            [
              fileId,
              section.kind,
              null,
              this.trimTo(row?.recordType, 3) ?? '001',
              this.trimTo(row?.merchantId, 20),
              this.parseSicrediEdiDate(row?.dateQRCodeGenerated),
              saleDatetime,
              null,
              null,
              null,
              this.trimTo(row?.authorizationCode, 64),
              this.trimTo(row?.terminalRegister, 32),
              this.trimTo(row?.nsuTransaction, 32),
              this.trimTo(row?.systemRetrievalReferenceNumber, 32),
              this.trimTo(row?.referenceNumberFEPAS, 32),
              null,
              null,
              this.trimTo(row?.qrCodeStatus, 16),
              this.trimTo(row?.qrCodeStatus, 80),
              null,
              'PIX',
              null,
              'PIX',
              amount,
              null,
              amount,
              null,
              null,
              null,
              null,
              null,
              null,
              this.trimTo(row?.qrCodeStatus, 24),
              storedRowHash,
              JSON.stringify(row),
              new Date(),
            ],
          );
          await this.upsertSicrediOperationalSaleTx(tx, {
            saleDatetime,
            ediSourceKind: 'EDI_PIX',
            authCode: this.trimTo(row?.authorizationCode, 64),
            establishmentCode: this.trimTo(row?.merchantId, 20),
            establishmentName: null,
            matrixClientCode: null,
            saleReceipt: this.trimTo(row?.nsuTransaction, 32),
            salesSummaryNumber: null,
            orderNo: null,
            salesChannel: 'EDI_SICREDI',
            terminalNo: this.trimTo(row?.terminalRegister, 32),
            product: 'PIX',
            cardType: 'PIX',
            cardTypeCode: null,
            brand: 'PIX',
            cardSchemeCode: null,
            cardSchemeDesc: 'PIX',
            status: this.normalizeSicrediStatus(this.trimTo(row?.qrCodeStatus, 24)),
            transactionTypeCode: null,
            transactionTypeDesc: null,
            transactionStatusCode: this.trimTo(row?.qrCodeStatus, 16),
            transactionStatusDesc: this.trimTo(row?.qrCodeStatus, 80),
            grossAmount: amount,
            mdrAmount: null,
            netAmount: amount,
            orderIdDesc: null,
            cardNumber: null,
            prepaid: null,
            expectedPayDate: null,
            payStatus: this.normalizeSicrediStatus(this.trimTo(row?.qrCodeStatus, 24)),
            payDate: this.parseSicrediEdiDate(row?.dateQrCodeConfirmationReceived),
            paymentCode: this.trimTo(row?.referenceNumberFEPAS, 32),
            cardRefCode: this.trimTo(row?.systemRetrievalReferenceNumber, 32),
            cardOrigin: 'EDI_SICREDI',
            paymentAccountReference: null,
            idUr: null,
            feePercent: null,
            interchangePlusAmount: null,
            interchangeFeePercent: null,
            cashbackAmount: null,
            reverseInterchange: null,
            entryModeCode: null,
            entryModeDesc: null,
            technologyTypeCode: null,
            technologyTypeDesc: null,
            rowHashSeed: [
              saleDatetime.toISOString(),
              normalizeText(row?.authorizationCode),
              normalizeText(row?.nsuTransaction),
              amount,
              normalizeText(row?.terminalRegister),
            ],
          });
          inserted += 1;
        }
        continue;
      }

      for (const summary of section.rows) {
        const detailsKey = Object.keys(summary ?? {}).find(
          (key) => Array.isArray(summary?.[key]) && key.toLowerCase().endsWith('receipt'),
        );
        const details = detailsKey && Array.isArray(summary?.[detailsKey]) ? summary[detailsKey] : [];
        for (const row of details) {
          const saleDatetime = this.parseSicrediEdiDateTime(row?.salesDate, row?.transactionDateTime);
          const grossAmount = this.parseSicrediEdiDecimal(row?.grossAmount);
          const discountAmount = this.parseSicrediEdiDecimal(row?.discountAmount);
          const netAmount = this.parseSicrediEdiDecimal(row?.netAmount);
          if (!saleDatetime || grossAmount === null) {
            invalidRows += 1;
            continue;
          }
          const rowHash = hashRow([
            'SICREDI_EDI_S',
            section.kind,
            saleDatetime.toISOString(),
            normalizeText(row?.authorizationCode),
            normalizeText(row?.salesReceiptNumber),
            normalizeText(row?.terminalNumber),
            grossAmount,
            normalizeText(row?.idUr),
          ]);
          let storedRowHash = rowHash;
          if (await this.existsRowHashTx(tx, 'T_SICREDI_EDI_SALES', rowHash)) {
            duplicates += 1;
            storedRowHash = this.buildStoredRowHash(rowHash, duplicates);
          }
          await this.dbService.executeTx(
            tx,
            `INSERT INTO T_SICREDI_EDI_SALES (
              FILE_ID, SECTION_KIND, SUMMARY_RECORD_TYPE, DETAIL_RECORD_TYPE, CLIENT_CODE, SALES_DATE,
              SALE_DATETIME, CREDIT_DATE, SALES_SUMMARY_NUMBER, SALES_RECEIPT_NUMBER, AUTHORIZATION_CODE,
              TERMINAL_NO, NSU_TRANSACTION, SYS_REF_NO, REF_FEPAS_NO,
              TRANSACTION_TYPE_CODE, TX_TYPE_DESC, TRANSACTION_STATUS_CODE, TX_STATUS_DESC,
              CARD_SCHEME_CODE, CARD_SCHEME_DESC, CARD_TYPE_CODE, CARD_TYPE_DESC, GROSS_AMOUNT,
              DISCOUNT_AMOUNT, NET_AMOUNT, FEE_PERCENT, INTERCHANGE_PLUS_AMOUNT, ACQUIRER_REFERENCE,
              PAY_ACCT_REF, MASKED_CARD_NUMBER, ID_UR, QR_CODE_STATUS, ROW_HASH, RAW_JSON, CREATED_AT
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            [
              fileId,
              section.kind,
              this.trimTo(summary?.recordType, 3),
              this.trimTo(row?.recordType, 3) ?? '011',
              this.trimTo(row?.clientCode ?? summary?.clientCode, 20),
              this.parseSicrediEdiDate(row?.salesDate ?? summary?.salesDate),
              saleDatetime,
              this.parseSicrediEdiDate(row?.creditDate ?? summary?.creditDate),
              this.trimTo(row?.salesSummaryNumber ?? summary?.salesSummaryNumber, 32),
              this.trimTo(row?.salesReceiptNumber, 32),
              this.trimTo(row?.authorizationCode, 64),
              this.trimTo(row?.terminalNumber, 32),
              null,
              null,
              null,
              this.trimTo(row?.transactionType?.code ?? summary?.transactionType?.code, 8),
              this.trimTo(
                row?.transactionType?.description ??
                  row?.transactionTypeDescription ??
                  summary?.transactionType?.description ??
                  summary?.transactionTypeDescription,
                80,
              ),
              this.trimTo(summary?.transactionStatus?.code, 16),
              this.trimTo(
                summary?.transactionStatus?.description ?? summary?.transactionStatusDescription,
                80,
              ),
              this.trimTo(row?.cardScheme?.code ?? summary?.cardScheme?.code, 8),
              this.trimTo(
                row?.cardScheme?.description ??
                  row?.cardSchemeDescription ??
                  summary?.cardScheme?.description ??
                  summary?.cardSchemeDescription,
                40,
              ),
              this.trimTo(row?.cardType?.code, 8),
              this.trimTo(row?.cardType?.description ?? row?.cardTypeDescription, 80),
              grossAmount,
              discountAmount,
              netAmount,
              this.parseSicrediEdiDecimal(row?.feePercent),
              this.parseSicrediEdiDecimal(row?.interchangePlusAmount),
              this.trimTo(row?.acquirerReference, 64),
              this.trimTo(row?.paymentAccountReference, 64),
              this.trimTo(row?.maskedCardNumber, 32),
              this.trimTo(row?.idUr, 64),
              null,
              storedRowHash,
              JSON.stringify({ summary, detail: row }),
              new Date(),
            ],
          );
          const transactionTypeDescription = this.trimTo(
            row?.transactionType?.description ??
              row?.transactionTypeDescription ??
              summary?.transactionType?.description ??
              summary?.transactionTypeDescription,
            80,
          );
          const cardTypeDescription = this.trimTo(
            row?.cardType?.description ?? row?.cardTypeDescription,
            80,
          );
          const brandDescription = this.trimTo(
            row?.cardScheme?.description ??
              row?.cardSchemeDescription ??
              summary?.cardScheme?.description ??
              summary?.cardSchemeDescription,
            40,
          );
          await this.upsertSicrediOperationalSaleTx(tx, {
            saleDatetime,
            ediSourceKind:
              section.kind === 'DEBIT'
                ? 'EDI_DEB'
                : section.kind === 'CREDIT'
                  ? 'EDI_CRD'
                  : 'EDI_CARD',
            authCode: this.trimTo(row?.authorizationCode, 64),
            establishmentCode: this.trimTo(row?.clientCode ?? summary?.clientCode, 20),
            establishmentName: null,
            matrixClientCode: this.trimTo(summary?.matrixClientCode, 20),
            saleReceipt: this.trimTo(row?.salesReceiptNumber, 32),
            salesSummaryNumber: this.trimTo(row?.salesSummaryNumber ?? summary?.salesSummaryNumber, 32),
            orderNo: this.trimTo(row?.salesSummaryNumber ?? summary?.salesSummaryNumber, 32),
            salesChannel: 'EDI_SICREDI',
            terminalNo: this.trimTo(row?.terminalNumber, 32),
            product: transactionTypeDescription,
            cardType: cardTypeDescription,
            cardTypeCode: this.trimTo(row?.cardType?.code, 8),
            brand: brandDescription,
            cardSchemeCode: this.trimTo(row?.cardScheme?.code ?? summary?.cardScheme?.code, 8),
            cardSchemeDesc: brandDescription,
            status: this.normalizeSicrediStatus(
              this.trimTo(
                summary?.transactionStatus?.description ?? summary?.transactionStatusDescription,
                80,
              ),
            ),
            transactionTypeCode: this.trimTo(row?.transactionType?.code ?? summary?.transactionType?.code, 8),
            transactionTypeDesc: transactionTypeDescription,
            transactionStatusCode: this.trimTo(summary?.transactionStatus?.code, 16),
            transactionStatusDesc: this.trimTo(
              summary?.transactionStatus?.description ?? summary?.transactionStatusDescription,
              80,
            ),
            grossAmount,
            mdrAmount: discountAmount,
            netAmount,
            orderIdDesc: this.trimTo(row?.idUr, 64),
            cardNumber: this.trimTo(row?.maskedCardNumber, 32),
            prepaid: null,
            expectedPayDate: this.parseSicrediEdiDate(row?.creditDate ?? summary?.creditDate),
            payStatus: 'PREVISTO',
            payDate: null,
            paymentCode: this.trimTo(row?.salesSummaryNumber ?? summary?.salesSummaryNumber, 32),
            cardRefCode: this.trimTo(row?.acquirerReference, 64),
            cardOrigin: 'EDI_SICREDI',
            paymentAccountReference: this.trimTo(row?.paymentAccountReference, 64),
            idUr: this.trimTo(row?.idUr, 64),
            feePercent: this.parseSicrediEdiDecimal(row?.feePercent),
            interchangePlusAmount: this.parseSicrediEdiDecimal(row?.interchangePlusAmount),
            interchangeFeePercent: this.parseSicrediEdiDecimal(row?.interchangeFeePercent),
            cashbackAmount: this.parseSicrediEdiDecimal(row?.cashBackAmount),
            reverseInterchange: this.parseSicrediEdiDecimal(row?.reverseInterchange),
            entryModeCode: this.trimTo(row?.entryMode?.code, 8),
            entryModeDesc: this.trimTo(row?.entryMode?.description ?? row?.entryModeDescription, 80),
            technologyTypeCode: this.trimTo(row?.technologyType?.code, 8),
            technologyTypeDesc: this.trimTo(
              row?.technologyType?.description ?? row?.technologyTypeDescription,
              80,
            ),
            rowHashSeed: [
              section.kind,
              saleDatetime.toISOString(),
              normalizeText(row?.authorizationCode),
              normalizeText(row?.salesReceiptNumber),
              normalizeText(row?.terminalNumber),
              grossAmount,
              normalizeText(row?.idUr),
            ],
          });
          inserted += 1;
        }
      }
    }

    return { inserted, duplicates, invalidRows };
  }

  private async importSicrediEdiFinanceTx(tx: any, fileId: number, payload: any): Promise<ImportResult> {
    const sections = [
      { kind: 'DEBIT', rows: Array.isArray(payload?.debitFinanceSummary) ? payload.debitFinanceSummary : [] },
      { kind: 'CREDIT', rows: Array.isArray(payload?.creditFinanceSummary) ? payload.creditFinanceSummary : [] },
      { kind: 'INSTALLMENT', rows: Array.isArray(payload?.installmentFinanceSummary) ? payload.installmentFinanceSummary : [] },
    ];

    let inserted = 0;
    let duplicates = 0;
    let invalidRows = 0;

    for (const section of sections) {
      for (const summary of section.rows) {
        const detailsKey = Object.keys(summary ?? {}).find(
          (key) => Array.isArray(summary?.[key]) && key.toLowerCase().endsWith('receipt'),
        );
        const details = detailsKey && Array.isArray(summary?.[detailsKey]) ? summary[detailsKey] : [];
        for (const row of details) {
          const paymentDate = this.parseSicrediEdiDate(row?.paymentDate ?? summary?.paymentDate);
          const transactionDate = this.parseSicrediEdiDate(row?.transactionDate);
          const grossAmount = this.parseSicrediEdiDecimal(row?.grossAmount);
          if (!paymentDate || grossAmount === null) {
            invalidRows += 1;
            continue;
          }
          const rowHash = hashRow([
            'SICREDI_EDI_P',
            section.kind,
            paymentDate.toISOString(),
            normalizeText(row?.authorizationCode),
            normalizeText(row?.salesReceiptNumber),
            normalizeText(row?.salesSummaryNumber),
            grossAmount,
            normalizeText(row?.idUr),
          ]);
          let storedRowHash = rowHash;
          if (await this.existsRowHashTx(tx, 'T_SICREDI_EDI_FINANCE', rowHash)) {
            duplicates += 1;
            storedRowHash = this.buildStoredRowHash(rowHash, duplicates);
          }
          await this.dbService.executeTx(
            tx,
            `INSERT INTO T_SICREDI_EDI_FINANCE (
              FILE_ID, SECTION_KIND, SUMMARY_RECORD_TYPE, DETAIL_RECORD_TYPE, CLIENT_CODE, PAYMENT_DATE,
              VALUE_DATE, TRANSACTION_DATE, TRANSACTION_TIME, PAY_INSTR_NO, FINANCE_SUMMARY_NUMBER,
              SALES_SUMMARY_NUMBER, SALES_REFERENCE, AUTHORIZATION_CODE, SALES_RECEIPT_NUMBER, PAYMENT_STATUS,
              PAYMENT_TYPE_CODE, PAYMENT_TYPE_DESCRIPTION, PAYMENT_TRANSACTION_TYPE_CODE,
              PAY_TX_TYPE_DESC, TRANSACTION_TYPE_CODE, TX_TYPE_DESC,
              CARD_SCHEME_CODE, CARD_SCHEME_DESC, RECEIPT_TYPE_CODE, RECEIPT_TYPE_DESC,
              GROSS_AMOUNT, DISCOUNT_AMOUNT, NET_AMOUNT, FEE_MODE_AMOUNT, FEE_MODE_NET,
              ACQUIRER_REFERENCE, PAY_ACCT_REF, MASKED_CARD_NUMBER, ID_UR, ROW_HASH, RAW_JSON, CREATED_AT
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            [
              fileId,
              section.kind,
              this.trimTo(summary?.recordType, 3),
              this.trimTo(row?.recordType, 3) ?? '021',
              this.trimTo(row?.clientCode ?? summary?.clientCode, 20),
              paymentDate,
              this.parseSicrediEdiDate(row?.valueDate),
              transactionDate,
              this.trimTo(row?.transactionTime, 8),
              this.trimTo(row?.paymentInstructionNumber ?? summary?.paymentInstructionNumber, 32),
              this.trimTo(row?.financeSummaryNumber ?? summary?.paymentInstructionNumber, 32),
              this.trimTo(row?.salesSummaryNumber, 32),
              this.trimTo(row?.salesReference, 32),
              this.trimTo(row?.authorizationCode, 64),
              this.trimTo(row?.salesReceiptNumber, 32),
              this.trimTo(row?.paymentStatus, 32),
              this.trimTo(row?.paymentType?.code, 8),
              this.trimTo(row?.paymentType?.description ?? row?.paymentTypeDescription, 40),
              this.trimTo(row?.paymentTransactionType?.code, 8),
              this.trimTo(
                row?.paymentTransactionType?.description ?? row?.paymentTransactionTypeDescription,
                80,
              ),
              this.trimTo(row?.transactionType?.code, 8),
              this.trimTo(row?.transactionType?.description ?? row?.transactionTypeDescription, 80),
              this.trimTo(row?.cardScheme?.code ?? summary?.cardScheme?.code, 8),
              this.trimTo(
                row?.cardScheme?.description ??
                  row?.cardSchemeDescription ??
                  summary?.cardScheme?.description ??
                  summary?.cardSchemeDescription,
                40,
              ),
              this.trimTo(row?.receiptType?.code, 8),
              this.trimTo(row?.receiptType?.description ?? row?.receiptTypeDescription, 32),
              grossAmount,
              this.parseSicrediEdiDecimal(row?.discountAmount),
              this.parseSicrediEdiDecimal(row?.netAmount),
              this.parseSicrediEdiDecimal(row?.feeModeAmount),
              this.parseSicrediEdiDecimal(row?.feeModeNetAmount),
              this.trimTo(row?.acquirerReference, 64),
              this.trimTo(row?.paymentAccountReference, 64),
              this.trimTo(row?.maskedCardNumber, 32),
              this.trimTo(row?.idUr, 64),
              storedRowHash,
              JSON.stringify({ summary, detail: row }),
              new Date(),
            ],
          );
          const saleDate = transactionDate ?? paymentDate;
          const grossAmountValue = grossAmount;
          const netAmountValue = this.parseSicrediEdiDecimal(row?.netAmount);
          const mdrAmountValue = this.parseSicrediEdiDecimal(row?.discountAmount);
          const receiptTypeDescription = this.trimTo(
            row?.receiptType?.description ?? row?.receiptTypeDescription,
            32,
          );
          const brandDescription = this.trimTo(
            row?.cardScheme?.description ??
              row?.cardSchemeDescription ??
              summary?.cardScheme?.description ??
              summary?.cardSchemeDescription,
            40,
          );
          const enriched = await this.enrichSicrediOperationalSaleFromFinanceTx(tx, {
            saleDate,
            authCode: this.trimTo(row?.authorizationCode, 64),
            saleReceipt: this.trimTo(row?.salesReceiptNumber, 32),
            paymentCode: this.trimTo(row?.paymentInstructionNumber ?? summary?.paymentInstructionNumber, 32),
            cardRefCode: this.trimTo(row?.acquirerReference, 64),
            terminalNo: null,
            grossAmount: grossAmountValue,
            netAmount: netAmountValue,
            mdrAmount: mdrAmountValue,
            payDate: paymentDate,
            expectedPayDate: this.parseSicrediEdiDate(row?.valueDate),
            payStatus: this.normalizeSicrediStatus(this.trimTo(row?.paymentStatus, 32)),
            paymentInstructionNo: this.trimTo(
              row?.paymentInstructionNumber ?? summary?.paymentInstructionNumber,
              32,
            ),
            financeSummaryNumber: this.trimTo(row?.financeSummaryNumber, 32),
            salesReference: this.trimTo(row?.salesReference, 32),
            valueDate: this.parseSicrediEdiDate(row?.valueDate),
            transactionDate: this.parseSicrediEdiDate(row?.transactionDate),
            transactionTime: this.trimTo(row?.transactionTime, 8),
            paymentTypeCode: this.trimTo(row?.paymentType?.code, 8),
            paymentTypeDesc: this.trimTo(row?.paymentType?.description ?? row?.paymentTypeDescription, 40),
            paymentTransactionTypeCode: this.trimTo(row?.paymentTransactionType?.code, 8),
            paymentTransactionTypeDesc: this.trimTo(
              row?.paymentTransactionType?.description ?? row?.paymentTransactionTypeDescription,
              80,
            ),
            receiptTypeCode: this.trimTo(row?.receiptType?.code, 8),
            receiptTypeDesc: this.trimTo(
              row?.receiptType?.description ?? row?.receiptTypeDescription,
              32,
            ),
            transactionDescriptionCode: this.trimTo(row?.transactionDescriptionCode, 16),
            transactionDescription: this.trimTo(row?.transactionDescription, 80),
            feeModeAmount: this.parseSicrediEdiDecimal(row?.feeModeAmount),
            feeModeNetAmount: this.parseSicrediEdiDecimal(row?.feeModeNetAmount),
            paymentAccountReference: this.trimTo(row?.paymentAccountReference, 64),
            idUr: this.trimTo(row?.idUr, 64),
            cardNumber: this.trimTo(row?.maskedCardNumber, 32),
            product: this.trimTo(
              row?.paymentTransactionType?.description ?? row?.paymentTransactionTypeDescription,
              80,
            ),
            cardType: receiptTypeDescription,
            brand: brandDescription,
          });
          if (!enriched) {
            invalidRows += 1;
          }
          inserted += 1;
        }
      }
    }

    return { inserted, duplicates, invalidRows };
  }

  private async importSicrediEdiReceivablesTx(tx: any, fileId: number, payload: any): Promise<ImportResult> {
    const receivableUnits = Array.isArray(payload?.receivableUnits) ? payload.receivableUnits : [];
    let inserted = 0;
    let duplicates = 0;
    let invalidRows = 0;

    for (const unit of receivableUnits) {
      const paymentDate = this.parseSicrediEdiDate(unit?.paymentDate);
      const idUr = this.trimTo(unit?.idUr, 64);
      if (!idUr || !paymentDate) {
        invalidRows += 1;
        continue;
      }
      const rowHash = hashRow([
        'SICREDI_EDI_R',
        idUr,
        paymentDate.toISOString(),
        normalizeText(unit?.receivableUnitStatus),
        normalizeText(unit?.grossAmount),
        normalizeText(unit?.paidAmount),
      ]);
      let storedRowHash = rowHash;
      if (await this.existsRowHashTx(tx, 'T_SICREDI_EDI_RECEIVABLES', rowHash)) {
        duplicates += 1;
        storedRowHash = this.buildStoredRowHash(rowHash, duplicates);
      }
      const rows = await this.dbService.queryTx<{ ID: number }>(
        tx,
        `INSERT INTO T_SICREDI_EDI_RECEIVABLES (
          FILE_ID, RECORD_TYPE, UPDATE_DATE, ID_UR, RECEIVABLE_UNIT_KEY, RECEIVABLE_UNIT_STATUS,
          ACCREDITATION_DOCUMENT, CLIENT_DOCUMENT, CARD_SCHEME_CODE, CARD_SCHEME_DESCRIPTION,
          PAYMENT_DATE, GROSS_AMOUNT, MDR_AMOUNT, UPDATED_TOTAL_AMOUNT, DISCOUNT_AMOUNT,
          ADVANCE_FEE_AMOUNT, PAID_AMOUNT, FREE_NEGOTIATION_AMOUNT, ALLOCATED_AMOUNT, ROW_HASH, RAW_JSON, CREATED_AT
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        RETURNING ID`,
        [
          fileId,
          this.trimTo(unit?.recordType, 3) ?? '001',
          this.parseSicrediEdiDate(unit?.updateDate),
          idUr,
          this.trimTo(unit?.receivableUnitKey, 64),
          this.trimTo(unit?.receivableUnitStatus, 40),
          this.trimTo(unit?.accreditationDocument, 20),
          this.trimTo(unit?.clientDocument, 20),
          this.trimTo(unit?.cardScheme?.code, 8),
          this.trimTo(unit?.cardScheme?.description ?? unit?.cardSchemeDescription, 40),
          paymentDate,
          this.parseSicrediEdiDecimal(unit?.grossAmount),
          this.parseSicrediEdiDecimal(unit?.mdrAmount),
          this.parseSicrediEdiDecimal(unit?.updatedTotalAmount),
          this.parseSicrediEdiDecimal(unit?.discountAmount),
          this.parseSicrediEdiDecimal(unit?.advanceFeeAmount),
          this.parseSicrediEdiDecimal(unit?.paidAmount),
          this.parseSicrediEdiDecimal(unit?.freeNegotiationAmount),
          this.parseSicrediEdiDecimal(unit?.allocatedAmount),
          storedRowHash,
          JSON.stringify(unit),
          new Date(),
        ],
      );
      const targetReceivableId = Number(rows[0]?.ID);
      inserted += 1;

      const payments = Array.isArray(unit?.paymentRU) ? unit.paymentRU : [];
      for (const payment of payments) {
        const paymentHash = hashRow([
          'SICREDI_EDI_R_PAY',
          idUr,
          normalizeText(payment?.contractPriorityId),
          normalizeText(payment?.paymentAccount),
          normalizeText(payment?.effectiveSettlementDate),
          normalizeText(payment?.effectiveSettlementAmount),
        ]);
        let storedPaymentHash = paymentHash;
        if (await this.existsRowHashTx(tx, 'T_SICREDI_EDI_RECEIVABLE_PAY', paymentHash)) {
          duplicates += 1;
          storedPaymentHash = this.buildStoredRowHash(paymentHash, duplicates);
        }
        await this.dbService.executeTx(
          tx,
          `INSERT INTO T_SICREDI_EDI_RECEIVABLE_PAY (
            FILE_ID, RECEIVABLE_ID, RECORD_TYPE, ID_UR, RECEIVABLE_UNIT_KEY, CONTRACT_PRIORITY_ID,
            ACCOUNT_TYPE_CODE, ACCOUNT_TYPE_DESCRIPTION, ISPB, COMPE_CODE, AGENCY, PAYMENT_ACCOUNT,
            PAY_AMOUNT, BENEFICIARY_DOCUMENT, EFFECTIVE_SETTLEMENT_DATE, EFFECTIVE_SETTLEMENT_AMOUNT,
            CONTRACT_IDENTIFIER, ROW_HASH, RAW_JSON, CREATED_AT
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          [
            fileId,
            targetReceivableId,
            this.trimTo(payment?.recordType, 3) ?? '004',
            idUr,
            this.trimTo(payment?.receivableUnitKey, 64),
            this.trimTo(payment?.contractPriorityId, 8),
            this.trimTo(payment?.accountType?.code, 8),
            this.trimTo(payment?.accountType?.description ?? payment?.accountTypeDescription, 40),
            this.trimTo(payment?.ispb, 20),
            this.trimTo(payment?.compeCode, 8),
            this.trimTo(payment?.agency, 16),
            this.trimTo(payment?.paymentAccount, 32),
            this.parseSicrediEdiDecimal(payment?.payAmount),
            this.trimTo(payment?.cpfCnpjContractBeneficiary, 20),
            this.parseSicrediEdiDate(payment?.effectiveSettlementDate),
            this.parseSicrediEdiDecimal(payment?.effectiveSettlementAmount),
            this.trimTo(payment?.contractIdentifier, 80),
            storedPaymentHash,
            JSON.stringify(payment),
            new Date(),
          ],
        );
        inserted += 1;
      }
    }

    return { inserted, duplicates, invalidRows };
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
    fileName: string,
    hash: string,
    acquirer: 'CIELO' | 'SIPAG' | 'SICREDI' | 'SICREDI_EDI_S' | 'SICREDI_EDI_P' | 'SICREDI_EDI_R',
  ): Promise<void> {
    try {
      await this.dbService.execute(
        'INSERT INTO T_IMPORTED_FILES (FILENAME, HASH, ACQUIRER, IMPORTED_AT) VALUES (?, ?, ?, ?)',
        [fileName, hash, acquirer, new Date()],
      );
    } catch (error) {
      this.logger.warn(
        `Falha ao registrar T_IMPORTED_FILES filename=${fileName}: ${
          error instanceof Error ? error.message : 'erro desconhecido'
        }`,
      );
    }
  }

  async listSicrediSales(options: {
    dateFrom?: string;
    dateTo?: string;
    page?: number;
    limit?: number;
    status?: string;
    brand?: string;
    search?: string;
    paymentType?: string;
    sortBy?: 'datetime' | 'amount';
    sortDir?: 'asc' | 'desc';
    includeReconciled?: boolean;
  }) {
    const page = options.page ?? 1;
    const limit = options.limit;
    const offset = typeof limit === 'number' ? (page - 1) * limit : 0;
    const conditions: string[] = [];
    const params: unknown[] = [];

    if (options.dateFrom) {
      conditions.push('SALE_DATETIME >= ?');
      params.push(this.toStartOfDay(options.dateFrom));
    }
    if (options.dateTo) {
      conditions.push('SALE_DATETIME < ?');
      params.push(this.toNextDay(options.dateTo));
    }
    if (options.status) {
      conditions.push('UPPER(STATUS) LIKE ?');
      params.push(`%${options.status.toUpperCase()}%`);
    }
    if (options.brand) {
      conditions.push('(UPPER(COALESCE(BRAND, \'\')) LIKE ? OR UPPER(COALESCE(CANON_BRAND, \'\')) LIKE ? OR UPPER(COALESCE(CANON_METHOD, \'\')) LIKE ?)');
      const needle = `%${options.brand.toUpperCase()}%`;
      params.push(needle, needle, needle);
    }
    if (options.paymentType) {
      const payment = options.paymentType.toUpperCase();
      const needle = `%${payment}%`;
      conditions.push(
        '(UPPER(COALESCE(CANON_METHOD_GROUP, \'\')) LIKE ? OR UPPER(COALESCE(CANON_METHOD, \'\')) LIKE ? OR UPPER(COALESCE(CARD_TYPE, \'\')) LIKE ? OR UPPER(COALESCE(PRODUCT, \'\')) LIKE ?)',
      );
      params.push(needle, needle, needle, needle);
    }
    if (options.search) {
      const needle = `%${options.search.toUpperCase()}%`;
      conditions.push(
        '(' +
          [
            'UPPER(COALESCE(AUTH_CODE, \'\')) LIKE ?',
            'UPPER(COALESCE(SALE_RECEIPT, \'\')) LIKE ?',
            'UPPER(COALESCE(ORDER_NO, \'\')) LIKE ?',
            'UPPER(COALESCE(TERMINAL_NO, \'\')) LIKE ?',
            'UPPER(COALESCE(ESTABLISHMENT_CODE, \'\')) LIKE ?',
            'UPPER(COALESCE(CARD_NUMBER, \'\')) LIKE ?',
            'UPPER(COALESCE(PAYMENT_CODE, \'\')) LIKE ?',
            'UPPER(COALESCE(CARD_REF_CODE, \'\')) LIKE ?',
          ].join(' OR ') +
          ')',
      );
      params.push(needle, needle, needle, needle, needle, needle, needle, needle);
    }

    if (!options.includeReconciled) {
      conditions.push(
        "NOT EXISTS (SELECT 1 FROM T_RECONCILIATION r WHERE r.ACQUIRER = 'SICREDI' AND r.ACQUIRER_ID = T_SICREDI_SALES.ID AND COALESCE(r.IS_ACTIVE, 1) = 1)",
      );
    }
    const where = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';
    const orderBy = this.buildOrderBy(options.sortBy, options.sortDir, {
      datetime: 'SALE_DATETIME',
      amount: 'GROSS_AMOUNT',
    });
    const paginationClause =
      typeof limit === 'number' ? `FIRST ${limit} SKIP ${offset} ` : '';
    const sql = `SELECT ${paginationClause}* FROM T_SICREDI_SALES ${where} ORDER BY ${orderBy}`;
    return this.dbService.query(sql, params);
  }

  async listUnifiedSales(options: {
    acquirers: Array<'cielo' | 'sipag' | 'sicredi'>;
    dateFrom?: string;
    dateTo?: string;
    page?: number;
    limit?: number;
    search?: string;
    sortBy?: 'datetime' | 'amount';
    sortDir?: 'asc' | 'desc';
    includeReconciled?: boolean;
    fetchAll?: boolean;
  }) {
    const page = options.page ?? 1;
    const limit = options.limit ?? 100;
    const offset = (page - 1) * limit;
    const take = page * limit;
    const sortBy = options.sortBy ?? 'datetime';
    const sortDir = options.sortDir ?? 'desc';
    const normalized: UnifiedAcquirerSale[] = [];

    if (options.acquirers.includes('cielo')) {
      const rows = (await this.listCieloSales({
        dateFrom: options.dateFrom,
        dateTo: options.dateTo,
        page: options.fetchAll ? undefined : 1,
        limit: options.fetchAll ? undefined : take,
        search: options.search,
        sortBy: options.sortBy,
        sortDir: options.sortDir,
        includeReconciled: options.includeReconciled,
      })) as any[];
      rows.forEach((row) => {
        normalized.push({
          acquirer: 'CIELO',
          id: row.ID,
          saleDatetime: row.SALE_DATETIME ?? null,
          grossAmount: row.GROSS_AMOUNT ?? 0,
          mdrAmount: row.MDR_AMOUNT ?? row.FEE_AMOUNT ?? null,
          netAmount: row.NET_AMOUNT ?? null,
          authCode: row.AUTH_CODE ?? null,
          nsu: row.NSU_DOC ?? null,
          terminal: row.MACHINE_NUMBER ?? null,
          pdv: row.ESTABLISHMENT_NO ?? null,
          brand: row.BRAND ?? null,
          status: row.STATUS ?? null,
          raw: row,
        });
      });
    }

    if (options.acquirers.includes('sipag')) {
      const rows = (await this.listSipagSales({
        dateFrom: options.dateFrom,
        dateTo: options.dateTo,
        page: options.fetchAll ? undefined : 1,
        limit: options.fetchAll ? undefined : take,
        search: options.search,
        sortBy: options.sortBy,
        sortDir: options.sortDir,
        includeReconciled: options.includeReconciled,
      })) as any[];
      rows.forEach((row) => {
        normalized.push({
          acquirer: 'SIPAG',
          id: row.ID,
          saleDatetime: row.SALE_DATETIME ?? null,
          grossAmount: row.GROSS_AMOUNT ?? 0,
          mdrAmount: row.FEE_AMOUNT ?? null,
          netAmount: row.NET_AMOUNT ?? null,
          authCode: row.AUTH_NO ?? null,
          nsu: row.TRANSACTION_NO ?? null,
          terminal: row.TERMINAL_NO ?? null,
          pdv: row.ESTABLISHMENT_NO ?? null,
          brand: row.BRAND ?? null,
          status: row.STATUS ?? null,
          raw: row,
        });
      });
    }

    if (options.acquirers.includes('sicredi')) {
      const rows = (await this.listSicrediSales({
        dateFrom: options.dateFrom,
        dateTo: options.dateTo,
        page: options.fetchAll ? undefined : 1,
        limit: options.fetchAll ? undefined : take,
        search: options.search,
        sortBy: options.sortBy,
        sortDir: options.sortDir,
        includeReconciled: options.includeReconciled,
      })) as any[];
      rows.forEach((row) => {
        normalized.push({
          acquirer: 'SICREDI',
          id: row.ID,
          saleDatetime: row.SALE_DATETIME ?? null,
          grossAmount: row.GROSS_AMOUNT ?? 0,
          mdrAmount: row.MDR_AMOUNT ?? null,
          netAmount: row.NET_AMOUNT ?? null,
          authCode: row.AUTH_CODE ?? null,
          nsu: row.SALE_RECEIPT ?? row.PAYMENT_CODE ?? null,
          terminal: row.TERMINAL_NO ?? null,
          pdv: row.ESTABLISHMENT_CODE ?? null,
          brand: row.BRAND ?? null,
          status: row.STATUS ?? null,
          raw: row,
        });
      });
    }

    const getTime = (value: Date | string | null) => {
      if (!value) {
        return 0;
      }
      const date = value instanceof Date ? value : new Date(String(value));
      return Number.isNaN(date.getTime()) ? 0 : date.getTime();
    };

    normalized.sort((a, b) => {
      if (sortBy === 'amount') {
        if (a.grossAmount !== b.grossAmount) {
          return sortDir === 'asc' ? a.grossAmount - b.grossAmount : b.grossAmount - a.grossAmount;
        }
      } else {
        const left = getTime(a.saleDatetime);
        const right = getTime(b.saleDatetime);
        if (left !== right) {
          return sortDir === 'asc' ? left - right : right - left;
        }
      }
      return b.id - a.id;
    });

    if (options.fetchAll) {
      return normalized;
    }
    return normalized.slice(offset, offset + limit);
  }

  async listCieloSales(options: {
    dateFrom?: string;
    dateTo?: string;
    page?: number;
    limit?: number;
    status?: string;
    brand?: string;
    search?: string;
    paymentType?: string;
    sortBy?: 'datetime' | 'amount';
    sortDir?: 'asc' | 'desc';
    includeReconciled?: boolean;
  }) {
    const page = options.page ?? 1;
    const limit = options.limit;
    const offset = typeof limit === 'number' ? (page - 1) * limit : 0;
    const conditions: string[] = [];
    const params: unknown[] = [];

    if (options.dateFrom) {
      conditions.push('SALE_DATETIME >= ?');
      params.push(this.toStartOfDay(options.dateFrom));
    }
    if (options.dateTo) {
      conditions.push('SALE_DATETIME < ?');
      params.push(this.toNextDay(options.dateTo));
    }
    if (options.status) {
      conditions.push('UPPER(STATUS) LIKE ?');
      params.push(`%${options.status.toUpperCase()}%`);
    }
    if (options.brand) {
      conditions.push('(UPPER(COALESCE(BRAND, \'\')) LIKE ? OR UPPER(COALESCE(CANON_BRAND, \'\')) LIKE ? OR UPPER(COALESCE(CANON_METHOD, \'\')) LIKE ?)');
      const needle = `%${options.brand.toUpperCase()}%`;
      params.push(needle, needle, needle);
    }
    if (options.paymentType) {
      const payment = options.paymentType.toUpperCase();
      const needle = `%${payment}%`;
      conditions.push(
        '(UPPER(COALESCE(CANON_METHOD_GROUP, \'\')) LIKE ? OR UPPER(COALESCE(CANON_METHOD, \'\')) LIKE ? OR UPPER(COALESCE(PAYMENT_METHOD, \'\')) LIKE ? OR UPPER(COALESCE(ENTRY_TYPE, \'\')) LIKE ? OR UPPER(COALESCE(MODALITY, \'\')) LIKE ?)',
      );
      params.push(needle, needle, needle, needle, needle);
    }
    if (options.search) {
      const needle = `%${options.search.toUpperCase()}%`;
      conditions.push(
        '(' +
          [
            'UPPER(COALESCE(NSU_DOC, \'\')) LIKE ?',
            'UPPER(COALESCE(SALE_CODE, \'\')) LIKE ?',
            'UPPER(COALESCE(TID, \'\')) LIKE ?',
            'UPPER(COALESCE(AUTH_CODE, \'\')) LIKE ?',
            'UPPER(COALESCE(CARD_NUMBER, \'\')) LIKE ?',
            'UPPER(COALESCE(ORDER_NUMBER, \'\')) LIKE ?',
            'UPPER(COALESCE(INVOICE_NUMBER, \'\')) LIKE ?',
            'UPPER(COALESCE(BATCH_NUMBER, \'\')) LIKE ?',
            'UPPER(COALESCE(PIX_ID, \'\')) LIKE ?',
            'UPPER(COALESCE(TX_ID, \'\')) LIKE ?',
            'UPPER(COALESCE(PIX_PAYMENT_ID, \'\')) LIKE ?',
            'UPPER(COALESCE(ESTABLISHMENT_NO, \'\')) LIKE ?',
            'UPPER(COALESCE(MACHINE_NUMBER, \'\')) LIKE ?',
            'UPPER(COALESCE(STATUS, \'\')) LIKE ?',
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
        needle,
        needle,
        needle,
        needle,
      );
    }

    if (!options.includeReconciled) {
      conditions.push(
        "NOT EXISTS (SELECT 1 FROM T_RECONCILIATION r WHERE r.ACQUIRER = 'CIELO' AND r.ACQUIRER_ID = T_CIELO_SALES.ID AND COALESCE(r.IS_ACTIVE, 1) = 1)",
      );
    }
    const where = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';
    const orderBy = this.buildOrderBy(options.sortBy, options.sortDir, {
      datetime: 'SALE_DATETIME',
      amount: 'GROSS_AMOUNT',
    });
    const paginationClause =
      typeof limit === 'number' ? `FIRST ${limit} SKIP ${offset} ` : '';
    const sql = `SELECT ${paginationClause}* FROM T_CIELO_SALES ${where} ORDER BY ${orderBy}`;
    return this.dbService.query(sql, params);
  }

  async listSipagSales(options: {
    dateFrom?: string;
    dateTo?: string;
    page?: number;
    limit?: number;
    status?: string;
    brand?: string;
    search?: string;
    paymentType?: string;
    sortBy?: 'datetime' | 'amount';
    sortDir?: 'asc' | 'desc';
    includeReconciled?: boolean;
  }) {
    const page = options.page ?? 1;
    const limit = options.limit;
    const offset = typeof limit === 'number' ? (page - 1) * limit : 0;
    const conditions: string[] = [];
    const params: unknown[] = [];

    if (options.dateFrom) {
      conditions.push('SALE_DATETIME >= ?');
      params.push(this.toStartOfDay(options.dateFrom));
    }
    if (options.dateTo) {
      conditions.push('SALE_DATETIME < ?');
      params.push(this.toNextDay(options.dateTo));
    }
    if (options.status) {
      conditions.push('UPPER(STATUS) LIKE ?');
      params.push(`%${options.status.toUpperCase()}%`);
    }
    if (options.brand) {
      conditions.push('(UPPER(COALESCE(BRAND, \'\')) LIKE ? OR UPPER(COALESCE(CANON_BRAND, \'\')) LIKE ? OR UPPER(COALESCE(CANON_METHOD, \'\')) LIKE ?)');
      const needle = `%${options.brand.toUpperCase()}%`;
      params.push(needle, needle, needle);
    }
    if (options.paymentType) {
      const payment = options.paymentType.toUpperCase();
      const needle = `%${payment}%`;
      conditions.push(
        '(UPPER(COALESCE(CANON_METHOD_GROUP, \'\')) LIKE ? OR UPPER(COALESCE(CANON_METHOD, \'\')) LIKE ? OR UPPER(COALESCE(PAYMENT_METHOD, \'\')) LIKE ? OR UPPER(COALESCE(CREDIT_DEBIT_IND, \'\')) LIKE ? OR UPPER(COALESCE(CARD_TYPE, \'\')) LIKE ? OR UPPER(COALESCE(PLAN_DESC, \'\')) LIKE ?)',
      );
      params.push(needle, needle, needle, needle, needle, needle);
    }
    if (options.search) {
      const needle = `%${options.search.toUpperCase()}%`;
      conditions.push(
        '(' +
          [
            'UPPER(COALESCE(TRANSACTION_NO, \'\')) LIKE ?',
            'UPPER(COALESCE(SALE_ID, \'\')) LIKE ?',
            'UPPER(COALESCE(AUTH_NO, \'\')) LIKE ?',
            'UPPER(COALESCE(CARD_NUMBER, \'\')) LIKE ?',
            'UPPER(COALESCE(TERMINAL_NO, \'\')) LIKE ?',
            'UPPER(COALESCE(SUMMARY_NO, \'\')) LIKE ?',
            'UPPER(COALESCE(YOUR_NUMBER, \'\')) LIKE ?',
            'UPPER(COALESCE(PAYMENT_ORDER_NO, \'\')) LIKE ?',
            'UPPER(COALESCE(STATUS, \'\')) LIKE ?',
            'UPPER(COALESCE(ESTABLISHMENT_NO, \'\')) LIKE ?',
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

    if (!options.includeReconciled) {
      conditions.push(
        "NOT EXISTS (SELECT 1 FROM T_RECONCILIATION r WHERE r.ACQUIRER = 'SIPAG' AND r.ACQUIRER_ID = T_SIPAG_SALES.ID AND COALESCE(r.IS_ACTIVE, 1) = 1)",
      );
    }
    const where = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';
    const orderBy = this.buildOrderBy(options.sortBy, options.sortDir, {
      datetime: 'SALE_DATETIME',
      amount: 'GROSS_AMOUNT',
    });
    const paginationClause =
      typeof limit === 'number' ? `FIRST ${limit} SKIP ${offset} ` : '';
    const sql = `SELECT ${paginationClause}* FROM T_SIPAG_SALES ${where} ORDER BY ${orderBy}`;
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

  private toStartOfDay(dateText: string): Date {
    const [year, month, day] = dateText.split('-').map(Number);
    return new Date(year, month - 1, day, 0, 0, 0, 0);
  }

  private toNextDay(dateText: string): Date {
    const start = this.toStartOfDay(dateText);
    start.setDate(start.getDate() + 1);
    return start;
  }
}
