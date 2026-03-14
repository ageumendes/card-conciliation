import { HttpService } from '@nestjs/axios';
import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { createHash } from 'crypto';
import { lastValueFrom } from 'rxjs';
import { DbService } from '../../db/db.service';
import { mapSipagToNormalized } from './sipag.mapper';
import { NormalizedTransaction, SipagExtractResponse, SipagTransactionRaw } from './sipag.types';

@Injectable()
export class SipagService {
  private readonly logger = new Logger(SipagService.name);

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
    private readonly dbService: DbService,
  ) {}

  async fetchDminus1(refDate: string): Promise<NormalizedTransaction[]> {
    const baseUrl = this.configService.get<string>('SIPAG_BASE_URL');
    const endpoint = this.configService.get<string>('SIPAG_ENDPOINT_EXTRATO');
    const token = this.configService.get<string>('SIPAG_TOKEN');

    if (!baseUrl || !endpoint || !token) {
      throw new Error('Config Sipag incompleta');
    }

    const normalized: NormalizedTransaction[] = [];
    let page = 1;
    const pageSize = 200;

    while (true) {
      const response = await lastValueFrom(
        this.httpService.get<SipagExtractResponse>(endpoint, {
          baseURL: baseUrl,
          headers: {
            Authorization: `Bearer ${token}`,
          },
          params: {
            refDate,
            page,
            pageSize,
          },
        }),
      );

      const payload = response.data;
      await this.saveRawPayload(refDate, payload);

      const items = this.extractTransactions(payload);
      const batch = items.map((item) => mapSipagToNormalized(item, refDate));
      await this.saveNormalized(refDate, batch);
      normalized.push(...batch);

      if (!this.hasNextPage(payload, page, pageSize, items.length)) {
        break;
      }

      page += 1;
    }

    this.logger.log(`Sipag D-1 concluido: ${normalized.length} transacoes`);
    return normalized;
  }

  async importDminus1(
    refDate: string,
  ): Promise<{ imported: number; rawId: number | null }> {
    const normalized = await this.fetchDminus1(refDate);
    const rows = await this.dbService.query<{ ID: number }>(
      'SELECT FIRST 1 ID FROM ACQ_RAW WHERE PROVIDER = ? AND REF_DATE = ? ORDER BY ID DESC',
      ['SIPAG', refDate],
    );
    const rawId = rows[0]?.ID ?? null;
    return { imported: normalized.length, rawId };
  }

  private extractTransactions(payload: SipagExtractResponse): SipagTransactionRaw[] {
    if (Array.isArray(payload.data)) {
      return payload.data;
    }

    if (Array.isArray(payload.transactions)) {
      return payload.transactions;
    }

    if (Array.isArray((payload as any).items)) {
      return (payload as any).items as SipagTransactionRaw[];
    }

    return [];
  }

  private hasNextPage(
    payload: SipagExtractResponse,
    currentPage: number,
    pageSize: number,
    itemsCount: number,
  ): boolean {
    if (typeof payload.nextPage === 'number') {
      return payload.nextPage > currentPage;
    }

    if (typeof payload.totalPages === 'number' && typeof payload.page === 'number') {
      return payload.page < payload.totalPages;
    }

    return itemsCount === pageSize;
  }

  private async saveRawPayload(refDate: string, payload: SipagExtractResponse): Promise<void> {
    const rawJson = JSON.stringify(payload);
    const hash = createHash('sha256').update(rawJson).digest('hex');

    const existing = await this.dbService.query<{ ID: number }>(
      'SELECT FIRST 1 ID FROM ACQ_RAW WHERE PROVIDER = ? AND PAYLOAD_HASH = ?',
      ['SIPAG', hash],
    );
    if (existing.length) {
      return;
    }

    const sql =
      'INSERT INTO ACQ_RAW (PROVIDER, REF_DATE, PAYLOAD_HASH, PAYLOAD_JSON, CREATED_AT) VALUES (?, ?, ?, ?, ?)';

    await this.dbService.execute(sql, ['SIPAG', refDate, hash, rawJson, new Date()]);
  }

  private async saveNormalized(refDate: string, items: NormalizedTransaction[]): Promise<void> {
    if (items.length === 0) {
      return;
    }

    const sql =
      'INSERT INTO ACQ_TX (PROVIDER, REF_DATE, EXT_ID, NSU, AUTH_CODE, TID, GROSS_AMOUNT, NET_AMOUNT, FEE_AMOUNT, BRAND, INSTALLMENTS, STATUS_ACQ, CAPTURED_AT, SETTLEMENT_DATE, CREATED_AT) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)';

    for (const item of items) {
      const exists = await this.isNormalizedDuplicate(refDate, item);
      if (exists) {
        continue;
      }
      await this.dbService.execute(sql, [
        'SIPAG',
        refDate,
        item.extId ?? null,
        item.nsu ?? null,
        item.authCode ?? null,
        item.tid ?? null,
        item.grossAmount ?? null,
        item.netAmount ?? null,
        item.feeAmount ?? null,
        item.brand ?? null,
        item.installments ?? null,
        item.statusAcq ?? null,
        item.capturedAt ?? null,
        item.settlementDate ?? null,
        new Date(),
      ]);
    }
  }

  private async isNormalizedDuplicate(
    refDate: string,
    item: NormalizedTransaction,
  ): Promise<boolean> {
    if (item.extId) {
      const rows = await this.dbService.query<{ ID: number }>(
        'SELECT FIRST 1 ID FROM ACQ_TX WHERE PROVIDER = ? AND EXT_ID = ?',
        ['SIPAG', item.extId],
      );
      return rows.length > 0;
    }

    const rows = await this.dbService.query<{ ID: number }>(
      'SELECT FIRST 1 ID FROM ACQ_TX WHERE PROVIDER = ? AND REF_DATE = ? AND NSU = ? AND AUTH_CODE = ? AND TID = ? AND GROSS_AMOUNT = ?',
      [
        'SIPAG',
        refDate,
        item.nsu ?? null,
        item.authCode ?? null,
        item.tid ?? null,
        item.grossAmount ?? null,
      ],
    );
    return rows.length > 0;
  }
}
